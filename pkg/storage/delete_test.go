package storage

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/highcumontoa/k8s-resource-controller-go/pkg/apis/v1"
)

// withFinalizer attaches the managed finalizer through an ordinary update and
// returns the new version.
func withFinalizer(t *testing.T, s *Store, name string, rv int64) int64 {
	t.Helper()
	w, err := s.Update(name, rv, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Metadata.Finalizers = []string{v1.FinalizerAttached}
		return cur, nil
	})
	if err != nil {
		t.Fatalf("attach finalizer to %s: %v", name, err)
	}
	return w.Metadata.ResourceVersion
}

// TestDeleteWithVersionRejectsStaleWithoutTrace: a delete carrying a version
// that a concurrent write has already moved past must be rejected with
// ErrVersionConflict — distinguishable from not-found and already-deleting —
// and must leave zero trace: no deletion timestamp, no version allocation.
func TestDeleteWithVersionRejectsStaleWithoutTrace(t *testing.T) {
	s := NewStore()
	w, _ := s.Create(mkWidget("w1", "v0"))
	rv := w.Metadata.ResourceVersion
	t.Logf("created name=w1 version=%d", rv)

	// A concurrent writer moves the version on.
	w, err := s.Update("w1", rv, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Spec.Message = "v1"
		return cur, nil
	})
	if err != nil {
		t.Fatalf("concurrent update: %v", err)
	}
	t.Logf("event: concurrent write advanced version %d -> %d", rv, w.Metadata.ResourceVersion)

	// The caller deletes with its now-stale read version.
	_, delErr := s.DeleteWithVersion("w1", rv)
	if !errors.Is(delErr, ErrVersionConflict) {
		t.Fatalf("stale delete: want ErrVersionConflict, got %v", delErr)
	}
	if errors.Is(delErr, ErrNotFound) || errors.Is(delErr, ErrDeleting) {
		t.Fatalf("stale-delete reason must be distinguishable from not-found/deleting: %v", delErr)
	}
	t.Logf("judgement: delete with stale version=%d rejected reason=%q (distinct from not-found/deleting)", rv, delErr)

	// No trace: object untouched, version not advanced, not terminating.
	got, _ := s.Get("w1")
	t.Logf("judgement: after rejected delete version=%d (must still be %d) message=%q deletionTimestamp=%q",
		got.Metadata.ResourceVersion, w.Metadata.ResourceVersion, got.Spec.Message, got.Metadata.DeletionTimestamp)
	if got.Metadata.ResourceVersion != w.Metadata.ResourceVersion {
		t.Fatal("rejected delete allocated a new resource version")
	}
	if got.Metadata.DeletionTimestamp != "" {
		t.Fatal("rejected delete set a deletion timestamp")
	}
	if got.Spec.Message != "v1" {
		t.Fatalf("rejected delete mutated state: %q", got.Spec.Message)
	}

	// The other two rejection reasons are observable and pairwise distinct.
	_, nfErr := s.DeleteWithVersion("ghost", 1)
	if !errors.Is(nfErr, ErrNotFound) {
		t.Fatalf("missing object: want ErrNotFound, got %v", nfErr)
	}
	rv2 := withFinalizer(t, s, "w1", w.Metadata.ResourceVersion)
	term, err := s.DeleteWithVersion("w1", rv2)
	if err != nil || term == nil {
		t.Fatalf("accepted delete with current version=%d: term=%v err=%v", rv2, term, err)
	}
	_, dupErr := s.DeleteWithVersion("w1", term.Metadata.ResourceVersion)
	if !errors.Is(dupErr, ErrDeleting) {
		t.Fatalf("delete of terminating object: want ErrDeleting, got %v", dupErr)
	}
	t.Logf("judgement: three rejection reasons pairwise distinct: stale=%q missing=%q terminating=%q",
		delErr, nfErr, dupErr)
	for a, b := range [][2]error{{delErr, nfErr}, {delErr, dupErr}, {nfErr, dupErr}} {
		if b[0].Error() == b[1].Error() {
			t.Fatalf("indistinguishable rejection reasons %q vs %q (pair %d)", b[0], b[1], a)
		}
	}
}

// TestDeleteWithVersionAcceptedSemantics: an accepted version-conditional
// delete keeps the established semantics — finalizer-bearing objects enter
// the readable terminating state, finalizer-less objects purge at once — and
// the version advances by exactly one.
func TestDeleteWithVersionAcceptedSemantics(t *testing.T) {
	s := NewStore()

	// With finalizer: terminating, still readable, version +1.
	w1, _ := s.Create(mkWidget("fin", "x"))
	rvFin := withFinalizer(t, s, "fin", w1.Metadata.ResourceVersion)
	term, err := s.DeleteWithVersion("fin", rvFin)
	if err != nil {
		t.Fatalf("delete with finalizer: %v", err)
	}
	t.Logf("event: delete accepted name=fin oldrv=%d newrv=%d deletionTimestamp=%q finalizers=%d",
		rvFin, term.Metadata.ResourceVersion, term.Metadata.DeletionTimestamp, len(term.Metadata.Finalizers))
	if term == nil || term.Metadata.DeletionTimestamp == "" {
		t.Fatal("finalizer-bearing object must enter terminating state")
	}
	if term.Metadata.ResourceVersion != rvFin+1 {
		t.Fatalf("version must advance by one: old=%d new=%d", rvFin, term.Metadata.ResourceVersion)
	}
	got, _ := s.Get("fin")
	if got.AggregatedPhase() != v1.WidgetStatusTerminating {
		t.Fatalf("terminating object must stay readable as Terminating, got %s", got.AggregatedPhase())
	}
	t.Logf("judgement: name=fin readable terminating phase=%s", got.AggregatedPhase())

	// Without finalizer: immediate purge.
	w2, _ := s.Create(mkWidget("plain", "x"))
	out, err := s.DeleteWithVersion("plain", w2.Metadata.ResourceVersion)
	if err != nil {
		t.Fatalf("delete without finalizer: %v", err)
	}
	if out != nil {
		t.Fatalf("finalizer-less object must purge at once, got %+v", out)
	}
	if _, err := s.Get("plain"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("purged object: want ErrNotFound, got %v", err)
	}
	t.Logf("judgement: name=plain purged immediately, get reason=%v", err)
}

// TestConcurrentDeleteWithVersionOnlyOneWins: many callers hold the same read
// result and delete the same object at once. At most one may succeed; every
// loser gets the same distinguishable conflict reason; the version advances
// exactly once.
func TestConcurrentDeleteWithVersionOnlyOneWins(t *testing.T) {
	s := NewStore()
	w, _ := s.Create(mkWidget("hot", "x"))
	rv := withFinalizer(t, s, "hot", w.Metadata.ResourceVersion)
	t.Logf("event: %d callers will delete name=hot with the same read version=%d", 32, rv)

	const n = 32
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins, conflicts, others := 0, 0, 0
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := s.DeleteWithVersion("hot", rv)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				wins++
			case errors.Is(err, ErrVersionConflict):
				conflicts++
			default:
				others++
				t.Errorf("unexpected delete result: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	t.Logf("judgement: wins=%d conflicts=%d others=%d (wins must be exactly 1, losers all ErrVersionConflict)",
		wins, conflicts, others)
	if wins != 1 || conflicts != n-1 || others != 0 {
		t.Fatalf("unexpected outcome wins=%d conflicts=%d others=%d", wins, conflicts, others)
	}
	got, err := s.Get("hot")
	if err != nil {
		t.Fatalf("terminating object must stay readable: %v", err)
	}
	t.Logf("judgement: version=%d advanced exactly once from %d; phase=%s deletionTimestamp=%q",
		got.Metadata.ResourceVersion, rv, got.AggregatedPhase(), got.Metadata.DeletionTimestamp)
	if got.Metadata.ResourceVersion != rv+1 {
		t.Fatalf("version advanced %d times, want exactly 1", got.Metadata.ResourceVersion-rv)
	}
	if got.Metadata.DeletionTimestamp == "" {
		t.Fatal("winning delete did not mark the object terminating")
	}
}

// TestPrecheckDeleteReadOnly: the precheck reports a per-object verdict with
// its reason, sorted by name, and mutates nothing.
func TestPrecheckDeleteReadOnly(t *testing.T) {
	s := NewStore()
	okW, _ := s.Create(mkWidget("b-ok", "x"))
	staleW, _ := s.Create(mkWidget("a-stale", "x"))
	termW, _ := s.Create(mkWidget("c-terminating", "x"))

	// a-stale: move its version past the caller's read.
	if _, err := s.Update("a-stale", staleW.Metadata.ResourceVersion, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Spec.Message = "moved"
		return cur, nil
	}); err != nil {
		t.Fatal(err)
	}
	// c-terminating: already in the deletion flow.
	rvTerm := withFinalizer(t, s, "c-terminating", termW.Metadata.ResourceVersion)
	if _, err := s.DeleteWithVersion("c-terminating", rvTerm); err != nil {
		t.Fatal(err)
	}

	intents := []DeleteIntent{
		{Name: "c-terminating", ExpectedRV: rvTerm + 1},
		{Name: "d-missing", ExpectedRV: 1},
		{Name: "b-ok", ExpectedRV: okW.Metadata.ResourceVersion},
		{Name: "a-stale", ExpectedRV: staleW.Metadata.ResourceVersion},
	}
	before := s.List()
	checks := s.PrecheckDelete(intents)

	if len(checks) != len(intents) {
		t.Fatalf("precheck returned %d verdicts for %d intents", len(checks), len(intents))
	}
	wantNames := []string{"a-stale", "b-ok", "c-terminating", "d-missing"}
	wantReasons := []error{ErrVersionConflict, nil, ErrDeleting, ErrNotFound}
	for i, c := range checks {
		t.Logf("precheck[%d] name=%s allowed=%v reason=%v", i, c.Name, c.Allowed, c.Reason)
		if c.Name != wantNames[i] {
			t.Fatalf("verdicts not sorted by name: [%d]=%s want %s", i, c.Name, wantNames[i])
		}
		if wantReasons[i] == nil && !c.Allowed {
			t.Fatalf("%s: want allowed, got reason=%v", c.Name, c.Reason)
		}
		if wantReasons[i] != nil && (c.Allowed || !errors.Is(c.Reason, wantReasons[i])) {
			t.Fatalf("%s: want reason=%v, got allowed=%v reason=%v", c.Name, wantReasons[i], c.Allowed, c.Reason)
		}
	}

	// Read-only: nothing changed, no version allocated.
	after := s.List()
	for i := range before {
		if before[i].Metadata.ResourceVersion != after[i].Metadata.ResourceVersion ||
			before[i].Spec.Message != after[i].Spec.Message {
			t.Fatalf("precheck mutated %s: rv %d->%d", before[i].Metadata.Name,
				before[i].Metadata.ResourceVersion, after[i].Metadata.ResourceVersion)
		}
	}
	t.Logf("judgement: precheck is read-only; %d objects unchanged, verdicts sorted by name", len(before))
}

// TestBatchDeletePerObjectResults: a mixed batch yields one explicit,
// name-sorted result per object; a stale version on one object never affects
// the verdicts of the others.
func TestBatchDeletePerObjectResults(t *testing.T) {
	s := NewStore()
	okW, _ := s.Create(mkWidget("b-ok", "x"))
	rvOK := withFinalizer(t, s, "b-ok", okW.Metadata.ResourceVersion)
	staleW, _ := s.Create(mkWidget("a-stale", "x"))
	if _, err := s.Update("a-stale", staleW.Metadata.ResourceVersion, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Spec.Message = "moved"
		return cur, nil
	}); err != nil {
		t.Fatal(err)
	}
	plainW, _ := s.Create(mkWidget("c-plain", "x"))

	// Precheck first, then execute the same intents.
	intents := []DeleteIntent{
		{Name: "b-ok", ExpectedRV: rvOK},
		{Name: "a-stale", ExpectedRV: staleW.Metadata.ResourceVersion},
		{Name: "d-missing", ExpectedRV: 9},
		{Name: "c-plain", ExpectedRV: plainW.Metadata.ResourceVersion},
	}
	checks := s.PrecheckDelete(intents)
	results := s.BatchDelete(intents)

	if len(results) != len(intents) {
		t.Fatalf("batch returned %d results for %d intents", len(results), len(intents))
	}
	// Execution must agree with the precheck (no concurrent change in between).
	for i := range checks {
		if checks[i].Name != results[i].Name {
			t.Fatalf("precheck/batch order mismatch at %d: %s vs %s", i, checks[i].Name, results[i].Name)
		}
		if checks[i].Allowed != results[i].Accepted {
			t.Fatalf("%s: precheck allowed=%v but execution accepted=%v (reason=%v)",
				checks[i].Name, checks[i].Allowed, results[i].Accepted, results[i].Reason)
		}
		t.Logf("batch[%d] name=%s accepted=%v purged=%v reason=%v (precheck agreed: %v)",
			i, results[i].Name, results[i].Accepted, results[i].Purged, results[i].Reason, checks[i].Allowed)
	}

	byName := map[string]DeleteResult{}
	for _, r := range results {
		byName[r.Name] = r
	}
	if r := byName["b-ok"]; !r.Accepted || r.Purged {
		t.Fatalf("b-ok: want accepted terminating delete, got %+v", r)
	}
	if r := byName["c-plain"]; !r.Accepted || !r.Purged {
		t.Fatalf("c-plain: want accepted immediate purge, got %+v", r)
	}
	if r := byName["a-stale"]; r.Accepted || !errors.Is(r.Reason, ErrVersionConflict) {
		t.Fatalf("a-stale: want ErrVersionConflict, got %+v", r)
	}
	if r := byName["d-missing"]; r.Accepted || !errors.Is(r.Reason, ErrNotFound) {
		t.Fatalf("d-missing: want ErrNotFound, got %+v", r)
	}

	// The stale object was untouched; the accepted ones moved on.
	got, _ := s.Get("a-stale")
	if got.Spec.Message != "moved" || got.Metadata.DeletionTimestamp != "" {
		t.Fatalf("stale object affected by sibling deletions: %+v", got.Metadata)
	}
	term, _ := s.Get("b-ok")
	if term.AggregatedPhase() != v1.WidgetStatusTerminating {
		t.Fatalf("b-ok must be terminating, got %s", term.AggregatedPhase())
	}
	if _, err := s.Get("c-plain"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("c-plain must be purged, got %v", err)
	}
	t.Logf("judgement: per-object results explicit and independent; a-stale untouched at version=%d",
		got.Metadata.ResourceVersion)
}

// TestBatchDeleteReplayConsistent: re-submitting the same batch must not
// produce duplicate side effects or advance a version twice; every replayed
// intent still gets an explicit, consistent verdict.
func TestBatchDeleteReplayConsistent(t *testing.T) {
	s := NewStore()
	w1, _ := s.Create(mkWidget("x-term", "x"))
	rv1 := withFinalizer(t, s, "x-term", w1.Metadata.ResourceVersion)
	w2, _ := s.Create(mkWidget("y-purge", "x"))

	intents := []DeleteIntent{
		{Name: "x-term", ExpectedRV: rv1},
		{Name: "y-purge", ExpectedRV: w2.Metadata.ResourceVersion},
	}
	first := s.BatchDelete(intents)
	for _, r := range first {
		t.Logf("first run: name=%s accepted=%v purged=%v reason=%v", r.Name, r.Accepted, r.Purged, r.Reason)
		if !r.Accepted {
			t.Fatalf("first run must accept %s, got %+v", r.Name, r)
		}
	}
	termAfterFirst, _ := s.Get("x-term")
	rvAfterFirst := termAfterFirst.Metadata.ResourceVersion

	second := s.BatchDelete(intents)
	for _, r := range second {
		t.Logf("replay: name=%s accepted=%v purged=%v reason=%v", r.Name, r.Accepted, r.Purged, r.Reason)
		if r.Accepted {
			t.Fatalf("replay must not re-accept %s (would double side effects)", r.Name)
		}
	}
	// x-term's version moved on when the first run marked it terminating, so
	// the replayed stale intent sees ErrVersionConflict; y-purge is gone, so
	// its replay sees ErrNotFound. Both explicit and distinguishable.
	if !errors.Is(second[0].Reason, ErrVersionConflict) {
		t.Fatalf("replay of x-term: want ErrVersionConflict, got %+v", second[0])
	}
	if !errors.Is(second[1].Reason, ErrNotFound) {
		t.Fatalf("replay of y-purge: want ErrNotFound, got %+v", second[1])
	}

	termAfterReplay, _ := s.Get("x-term")
	t.Logf("judgement: x-term version=%d after first run, %d after replay (must be equal: no double version bump)",
		rvAfterFirst, termAfterReplay.Metadata.ResourceVersion)
	if termAfterReplay.Metadata.ResourceVersion != rvAfterFirst {
		t.Fatal("replayed batch advanced the version a second time")
	}
	if fmt.Sprint(second[0].Reason) == fmt.Sprint(second[1].Reason) {
		t.Fatal("replay reasons must stay distinguishable")
	}
}
