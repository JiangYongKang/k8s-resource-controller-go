package storage

import (
	"errors"
	"fmt"
	"testing"

	"github.com/highcumontoa/k8s-resource-controller-go/pkg/apis/v1"
)

// batchFixture builds four objects exercising every precheck verdict:
// "ok" (deletable), "stale" (will be modified after the read), "term"
// (already terminating) — plus "ghost" which never exists.
func batchFixture(t *testing.T) (*Store, []DeleteIntent) {
	t.Helper()
	s := NewStore()

	ok, _ := s.Create(mkWidget("ok", "v0"))
	stale, _ := s.Create(mkWidget("stale", "v0"))
	term, _ := s.Create(mkWidget("term", "v0"))
	plain, _ := s.Create(mkWidget("plain", "v0"))

	// "term" gets a finalizer and enters the deletion flow.
	term, err := s.Update("term", term.Metadata.ResourceVersion, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Metadata.Finalizers = []string{v1.FinalizerAttached}
		return cur, nil
	})
	if err != nil {
		t.Fatalf("add finalizer: %v", err)
	}
	if _, err := s.Delete("term"); err != nil {
		t.Fatalf("pre-delete term: %v", err)
	}

	// "stale" is modified after the caller's read.
	staleRV := stale.Metadata.ResourceVersion
	if _, err := s.Update("stale", staleRV, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Spec.Message = "changed-after-read"
		return cur, nil
	}); err != nil {
		t.Fatalf("concurrent-style update: %v", err)
	}

	intents := []DeleteIntent{
		{Name: "term", ResourceVersion: term.Metadata.ResourceVersion},
		{Name: "ghost", ResourceVersion: 1},
		{Name: "ok", ResourceVersion: ok.Metadata.ResourceVersion},
		{Name: "stale", ResourceVersion: staleRV},
		{Name: "plain", ResourceVersion: plain.Metadata.ResourceVersion},
	}
	t.Logf("event: fixture built intents=%v", intents)
	return s, intents
}

func outcomesByName(results []DeleteResult) map[string]DeleteOutcome {
	m := make(map[string]DeleteOutcome, len(results))
	for _, r := range results {
		m[r.Name] = r.Outcome
	}
	return m
}

// TestCheckDeleteBatchIsReadOnlyAndSorted verifies the precheck reports each
// object's individual verdict, sorted by name, and consumes nothing.
func TestCheckDeleteBatchIsReadOnlyAndSorted(t *testing.T) {
	s, intents := batchFixture(t)

	before := make(map[string]int64)
	for _, w := range s.List() {
		before[w.Metadata.Name] = w.Metadata.ResourceVersion
	}

	results := s.CheckDeleteBatch(intents)
	for _, r := range results {
		t.Logf("precheck: name=%s outcome=%s reason=%v observed-rv=%d", r.Name, r.Outcome, r.Reason, r.ResourceVersion)
	}

	// Sorted by name.
	wantOrder := []string{"ghost", "ok", "plain", "stale", "term"}
	if len(results) != len(wantOrder) {
		t.Fatalf("result count: want %d, got %d", len(wantOrder), len(results))
	}
	for i, name := range wantOrder {
		if results[i].Name != name {
			t.Fatalf("results not sorted by name at %d: want %s, got %s", i, name, results[i].Name)
		}
	}

	got := outcomesByName(results)
	want := map[string]DeleteOutcome{
		"ok":    DeleteOutcomeReady,
		"plain": DeleteOutcomeReady,
		"stale": DeleteOutcomeVersionConflict,
		"term":  DeleteOutcomeAlreadyDeleting,
		"ghost": DeleteOutcomeNotFound,
	}
	for name, oc := range want {
		if got[name] != oc {
			t.Fatalf("precheck %s: want %s, got %s", name, oc, got[name])
		}
	}
	// Rejection reasons stay distinguishable through the batch API.
	reasonOf := func(name string) error {
		for _, r := range results {
			if r.Name == name {
				return r.Reason
			}
		}
		return nil
	}
	if !errors.Is(reasonOf("stale"), ErrVersionConflict) ||
		!errors.Is(reasonOf("term"), ErrDeleting) ||
		!errors.Is(reasonOf("ghost"), ErrNotFound) {
		t.Fatalf("precheck reasons not distinguishable: %v", results)
	}

	// Read-only: no version moved, nothing marked.
	for _, w := range s.List() {
		if before[w.Metadata.Name] != w.Metadata.ResourceVersion {
			t.Fatalf("precheck consumed a version for %s", w.Metadata.Name)
		}
		if w.Metadata.Name != "term" && w.Metadata.DeletionTimestamp != "" {
			t.Fatalf("precheck marked %s for deletion", w.Metadata.Name)
		}
	}
	t.Logf("judgement: precheck is read-only, versions unchanged: %v", before)
}

// TestDeleteBatchMatchesPrecheck executes the batch right after the precheck
// and requires identical per-object conclusions; one object's conflict must
// not affect the others.
func TestDeleteBatchMatchesPrecheck(t *testing.T) {
	s, intents := batchFixture(t)

	pre := outcomesByName(s.CheckDeleteBatch(intents))
	results := s.DeleteBatch(intents)
	for _, r := range results {
		t.Logf("execute: name=%s outcome=%s reason=%v rv=%d", r.Name, r.Outcome, r.Reason, r.ResourceVersion)
	}
	got := outcomesByName(results)

	// Ready in precheck -> accepted at execution (Terminating for the
	// finalizer-less "ok"? no: ok/plain have no finalizers -> Purged).
	want := map[string]DeleteOutcome{
		"ok":    DeleteOutcomePurged,
		"plain": DeleteOutcomePurged,
		"stale": DeleteOutcomeVersionConflict,
		"term":  DeleteOutcomeAlreadyDeleting,
		"ghost": DeleteOutcomeNotFound,
	}
	for name, oc := range want {
		if got[name] != oc {
			t.Fatalf("execute %s: want %s, got %s", name, oc, got[name])
		}
		if pre[name] == DeleteOutcomeReady && got[name] != DeleteOutcomePurged && got[name] != DeleteOutcomeTerminating {
			t.Fatalf("precheck/execute mismatch for %s: pre=%s exec=%s", name, pre[name], got[name])
		}
		if pre[name] != DeleteOutcomeReady && pre[name] != got[name] {
			t.Fatalf("precheck/execute mismatch for %s: pre=%s exec=%s", name, pre[name], got[name])
		}
	}

	// The conflicted object is untouched; the accepted ones are gone.
	stale, err := s.Get("stale")
	if err != nil {
		t.Fatalf("stale object must survive: %v", err)
	}
	if stale.Spec.Message != "changed-after-read" || stale.Metadata.DeletionTimestamp != "" {
		t.Fatalf("rejected object mutated: %+v", stale.Metadata)
	}
	t.Logf("judgement: conflicted object untouched rv=%d message=%q; accepted objects purged independently",
		stale.Metadata.ResourceVersion, stale.Spec.Message)
	if _, err := s.Get("ok"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ok should be purged: %v", err)
	}
	if _, err := s.Get("plain"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("plain should be purged: %v", err)
	}
}

// TestDeleteBatchWithFinalizerObject checks the Terminating outcome and that
// the object stays readable afterwards.
func TestDeleteBatchWithFinalizerObject(t *testing.T) {
	s := NewStore()
	w, _ := s.Create(mkWidget("guarded", "v0"))
	w, err := s.Update("guarded", w.Metadata.ResourceVersion, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Metadata.Finalizers = []string{v1.FinalizerAttached}
		return cur, nil
	})
	if err != nil {
		t.Fatalf("add finalizer: %v", err)
	}
	rv := w.Metadata.ResourceVersion

	results := s.DeleteBatch([]DeleteIntent{{Name: "guarded", ResourceVersion: rv}})
	if len(results) != 1 || results[0].Outcome != DeleteOutcomeTerminating {
		t.Fatalf("want one Terminating result, got %+v", results)
	}
	t.Logf("execute: name=guarded outcome=%s oldrv=%d newrv=%d", results[0].Outcome, rv, results[0].ResourceVersion)
	if results[0].ResourceVersion != rv+1 {
		t.Fatalf("accepted delete must advance version by one: %d -> %d", rv, results[0].ResourceVersion)
	}
	got, err := s.Get("guarded")
	if err != nil || got.AggregatedPhase() != v1.WidgetStatusTerminating {
		t.Fatalf("terminating object must stay readable as Terminating: %v %v", got, err)
	}
	t.Logf("judgement: object readable, phase=%s", got.AggregatedPhase())
}

// TestDeleteBatchRepeatSubmissionIsIdempotent re-submits the same intents:
// conclusions stay consistent, no version advances twice, no object is
// deleted twice.
func TestDeleteBatchRepeatSubmissionIsIdempotent(t *testing.T) {
	s := NewStore()
	g, _ := s.Create(mkWidget("guarded", "v0"))
	g, err := s.Update("guarded", g.Metadata.ResourceVersion, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Metadata.Finalizers = []string{v1.FinalizerAttached}
		return cur, nil
	})
	if err != nil {
		t.Fatalf("add finalizer: %v", err)
	}
	p, _ := s.Create(mkWidget("plain", "v0"))

	intents := []DeleteIntent{
		{Name: "guarded", ResourceVersion: g.Metadata.ResourceVersion},
		{Name: "plain", ResourceVersion: p.Metadata.ResourceVersion},
	}

	first := s.DeleteBatch(intents)
	second := s.DeleteBatch(intents)
	third := s.DeleteBatch(intents)
	for i, round := range [][]DeleteResult{first, second, third} {
		for _, r := range round {
			t.Logf("round=%d name=%s outcome=%s reason=%v rv=%d", i+1, r.Name, r.Outcome, r.Reason, r.ResourceVersion)
		}
	}

	if first[0].Outcome != DeleteOutcomeTerminating || first[1].Outcome != DeleteOutcomePurged {
		t.Fatalf("first round must accept both: %+v", first)
	}
	// Rounds 2 and 3 see the consequences of round 1 and must agree with
	// each other: guarded is AlreadyDeleting, plain is NotFound.
	for _, round := range [][]DeleteResult{second, third} {
		if round[0].Name != "guarded" || round[0].Outcome != DeleteOutcomeAlreadyDeleting {
			t.Fatalf("repeat round guarded: %+v", round[0])
		}
		if round[1].Name != "plain" || round[1].Outcome != DeleteOutcomeNotFound {
			t.Fatalf("repeat round plain: %+v", round[1])
		}
	}
	// The guarded object's version advanced exactly once, at the first round.
	got, _ := s.Get("guarded")
	t.Logf("judgement: guarded rv=%d after 3 identical submissions (first delete rv=%d, exactly one step)",
		got.Metadata.ResourceVersion, first[0].ResourceVersion)
	if got.Metadata.ResourceVersion != first[0].ResourceVersion {
		t.Fatalf("repeated submissions consumed extra versions: rv=%d first-delete-rv=%d",
			got.Metadata.ResourceVersion, first[0].ResourceVersion)
	}
}

// TestDeleteBatchConcurrentPerObjectIndependence runs two batches
// concurrently where both target a shared object with the same read version:
// exactly one batch wins the shared object, and the unshared objects in both
// batches are unaffected by the contention.
func TestDeleteBatchConcurrentPerObjectIndependence(t *testing.T) {
	s := NewStore()
	shared, _ := s.Create(mkWidget("shared", "v0"))
	a1, _ := s.Create(mkWidget("a-only", "v0"))
	b1, _ := s.Create(mkWidget("b-only", "v0"))
	rv := shared.Metadata.ResourceVersion

	batchA := []DeleteIntent{
		{Name: "shared", ResourceVersion: rv},
		{Name: "a-only", ResourceVersion: a1.Metadata.ResourceVersion},
	}
	batchB := []DeleteIntent{
		{Name: "shared", ResourceVersion: rv},
		{Name: "b-only", ResourceVersion: b1.Metadata.ResourceVersion},
	}

	type outcome struct{ res []DeleteResult }
	resCh := make(chan outcome, 2)
	go func() { resCh <- outcome{s.DeleteBatch(batchA)} }()
	go func() { resCh <- outcome{s.DeleteBatch(batchB)} }()
	ra, rb := <-resCh, <-resCh

	sharedWins := 0
	for _, r := range append(append([]DeleteResult{}, ra.res...), rb.res...) {
		t.Logf("concurrent batch: name=%s outcome=%s reason=%v", r.Name, r.Outcome, r.Reason)
		if r.Name == "shared" && r.Outcome == DeleteOutcomePurged {
			sharedWins++
		}
	}
	if sharedWins != 1 {
		t.Fatalf("exactly one batch may win the shared object, got %d", sharedWins)
	}
	// Unshared objects are always purged regardless of the shared contention.
	for _, r := range append(append([]DeleteResult{}, ra.res...), rb.res...) {
		if (r.Name == "a-only" || r.Name == "b-only") && r.Outcome != DeleteOutcomePurged {
			t.Fatalf("uncontended object %s affected by contention: %s", r.Name, r.Outcome)
		}
	}
	t.Logf("judgement: shared object won exactly once; per-object results independent")
}

// TestDeleteBatchDeterministicConclusions submits the same intents twice
// against unchanged (all-rejected) state and requires identical output.
func TestDeleteBatchDeterministicConclusions(t *testing.T) {
	s := NewStore()
	w, _ := s.Create(mkWidget("w1", "v0"))

	intents := []DeleteIntent{
		{Name: "w1", ResourceVersion: w.Metadata.ResourceVersion + 5},
		{Name: "ghost", ResourceVersion: 1},
	}
	r1 := s.DeleteBatch(intents)
	r2 := s.DeleteBatch(intents)
	if fmt.Sprintf("%v|%v", r1[0].Outcome, r1[1].Outcome) != fmt.Sprintf("%v|%v", r2[0].Outcome, r2[1].Outcome) {
		t.Fatalf("non-deterministic conclusions: %+v vs %+v", r1, r2)
	}
	if r1[0].Outcome != DeleteOutcomeNotFound || r1[1].Outcome != DeleteOutcomeVersionConflict {
		t.Fatalf("unexpected outcomes: %+v", r1)
	}
	got, _ := s.Get("w1")
	t.Logf("judgement: repeated all-rejected batch deterministic, w1 rv=%d unchanged", got.Metadata.ResourceVersion)
	if got.Metadata.ResourceVersion != w.Metadata.ResourceVersion {
		t.Fatal("rejected batch consumed versions")
	}
}
