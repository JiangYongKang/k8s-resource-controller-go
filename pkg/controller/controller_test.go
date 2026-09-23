package controller

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/k8s-resource-controller-go/pkg/apis/v1"
	"github.com/highcumontoa/k8s-resource-controller-go/pkg/storage"
)

// newTestController builds a controller whose logs are tee'd into the test
// output (versions/events/decisions stay visible in -v runs) and a buffer
// that tests can assert on.
func newTestController(t *testing.T, base, max time.Duration, hook ReconcileHook) (*Controller, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	c := New(Config{
		Store:       storage.NewStore(),
		Workers:     2,
		BaseDelay:   base,
		MaxDelay:    max,
		MaxAttempts: 3,
		FailHook:    hook,
		LogWriter:   io.MultiWriter(&buf, testWriter{t}),
	})
	return c, &buf
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", bytes.TrimRight(p, "\n"))
	return len(p), nil
}

func createWidget(t *testing.T, s *storage.Store, name, msg string) *v1.Widget {
	t.Helper()
	w, err := s.Create(&v1.Widget{Metadata: v1.ObjectMeta{Name: name}, Spec: v1.WidgetSpec{Message: msg}})
	if err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	return w
}

// waitFor polls cond until true or times out.
func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(2 * time.Millisecond)
	}
	return cond()
}

func runCancelAfter(t *testing.T, c *Controller, wait time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, 5*time.Second) }()
	time.Sleep(wait)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("controller did not stop after cancel")
	}
}

func TestReconcileHappyPath(t *testing.T) {
	c, _ := newTestController(t, 5*time.Millisecond, 50*time.Millisecond, nil)
	w := createWidget(t, c.store, "w1", "hello")
	t.Logf("event: created name=w1 version=%d", w.Metadata.ResourceVersion)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Reconcile(ctx, "w1"); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	got, _ := c.store.Get("w1")
	t.Logf("judgement: version=%d finalizers=%v phase=%s external-attached=%v external-ops=%d",
		got.Metadata.ResourceVersion, got.Metadata.Finalizers, got.AggregatedPhase(),
		c.external.Attached("w1"), c.external.OpCount("w1"))
	if !c.external.Attached("w1") {
		t.Fatal("external attachment missing after reconcile")
	}
	if len(got.Metadata.Finalizers) != 1 || got.Metadata.Finalizers[0] != v1.FinalizerAttached {
		t.Fatalf("managed finalizer missing: %v", got.Metadata.Finalizers)
	}
	if got.AggregatedPhase() != v1.WidgetStatusReady {
		t.Fatalf("aggregated phase: want Ready, got %s", got.AggregatedPhase())
	}
	if c.external.OpCount("w1") != 1 {
		t.Fatalf("expected exactly 1 attach op, got %d", c.external.OpCount("w1"))
	}
}

func TestDuplicateEventsNoDuplicateSideEffects(t *testing.T) {
	c, _ := newTestController(t, 5*time.Millisecond, 50*time.Millisecond, nil)
	createWidget(t, c.store, "w1", "hello")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for i := 0; i < 5; i++ {
		if err := c.Reconcile(ctx, "w1"); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}
	got, _ := c.store.Get("w1")
	t.Logf("event: 5 reconciliations of name=w1; judgement: external-ops=%d final-version=%d initial+finalizer+status => expected rv=3",
		c.external.OpCount("w1"), got.Metadata.ResourceVersion)
	if c.external.OpCount("w1") != 1 {
		t.Fatalf("duplicate reconciles caused %d external ops, want exactly 1", c.external.OpCount("w1"))
	}
	// rv: 1 create, 2 finalizer add, 3 status ready; repeats write nothing.
	if got.Metadata.ResourceVersion != 3 {
		t.Fatalf("expected stable rv=3 after convergence, got %d", got.Metadata.ResourceVersion)
	}
}

func TestFailureRetryWithObservableBackoff(t *testing.T) {
	// First 3 attempts fail inside the external system; then it recovers.
	var hookMu sync.Mutex
	failed := 0
	hook := func(name string, attempt int) error {
		hookMu.Lock()
		defer hookMu.Unlock()
		if name != "w1" {
			return nil
		}
		if failed < 3 {
			failed++
			return errInjected(failed)
		}
		return nil
	}
	c, buf := newTestController(t, 20*time.Millisecond, 400*time.Millisecond, hook)
	createWidget(t, c.store, "w1", "hello")

	start := time.Now()
	ok := c.Submit(Event{Kind: EventUpsert, Name: "w1"})
	if !ok {
		t.Fatal("Submit rejected before shutdown")
	}
	runCancelAfter(t, c, 2*time.Second)
	elapsed := time.Since(start)

	got, _ := c.store.Get("w1")
	retryLines := strings.Count(buf.String(), "RETRY")
	t.Logf("event: upsert w1 with 3 injected failures; judgement: attempts=%d phase=%s elapsed=%s retry-lines=%d",
		c.Attempts("w1"), got.AggregatedPhase(), elapsed, retryLines)
	if c.Attempts("w1") < 4 {
		t.Fatalf("expected >=4 attempts (3 failures + success), got %d", c.Attempts("w1"))
	}
	if got.AggregatedPhase() != v1.WidgetStatusReady {
		t.Fatalf("expected eventual Ready, got %s", got.AggregatedPhase())
	}
	if c.external.OpCount("w1") != 1 {
		t.Fatalf("expected exactly 1 real attach op despite retries, got %d", c.external.OpCount("w1"))
	}
	if retryLines < 3 {
		t.Fatalf("expected >=3 observable RETRY log lines, got %d", retryLines)
	}
	// Backoff observability: first retry delay logged must be BaseDelay.
	if !strings.Contains(buf.String(), "RETRY after=20ms") {
		t.Fatalf("expected exponential backoff start at 20ms in logs:\n%s", buf.String())
	}
	// 20ms + 40ms lower bound; timing assertions stay loose for reproducibility.
	if elapsed < 55*time.Millisecond {
		t.Fatalf("retries ran without waiting for backoff: %s", elapsed)
	}
	t.Logf("judgement: retries honored exponential backoff; convergence after %s", elapsed)
}

type injectedError int

func (e injectedError) Error() string { return "injected reconcile failure" }
func errInjected(n int) error         { return injectedError(n) }

// converge brings an object to Ready via the controller loop.
func converge(t *testing.T, c *Controller, name string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Reconcile(ctx, name); err != nil {
		t.Fatalf("converge %s: %v", name, err)
	}
	got, err := c.store.Get(name)
	if err != nil {
		t.Fatalf("get after converge: %v", err)
	}
	if got.AggregatedPhase() != v1.WidgetStatusReady {
		t.Fatalf("converge: phase=%s want Ready", got.AggregatedPhase())
	}
}

func TestDeletionFlowWithFinalizer(t *testing.T) {
	c, _ := newTestController(t, 5*time.Millisecond, 50*time.Millisecond, nil)
	createWidget(t, c.store, "w1", "hello")
	converge(t, c, "w1")
	ready, _ := c.store.Get("w1")
	t.Logf("event: converged name=w1 version=%d finalizers=%v attached=%v",
		ready.Metadata.ResourceVersion, ready.Metadata.Finalizers, c.external.Attached("w1"))

	// User deletion request: object enters terminating state, still readable.
	term, err := c.store.Delete("w1")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	got, _ := c.store.Get("w1")
	t.Logf("event: delete requested; judgement: still readable=true version=%d aggregated-phase=%s finalizers=%d",
		got.Metadata.ResourceVersion, got.AggregatedPhase(), len(got.Metadata.Finalizers))
	if got.AggregatedPhase() != v1.WidgetStatusTerminating || term == nil {
		t.Fatal("object did not enter Terminating state")
	}

	// A normal update must not restore it to a normal phase; reason must be
	// distinguishable from a stale version / not-found.
	_, errUpd := c.store.Update("w1", term.Metadata.ResourceVersion, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Status.Phase = v1.WidgetStatusReady
		cur.Spec.Message = "restored?"
		return cur, nil
	})
	if !isReject(errUpd, storage.ErrDeleting) {
		t.Fatalf("update during deletion: want ErrDeleting, got %v", errUpd)
	}
	t.Logf("judgement: ordinary update while terminating rejected reason=%v (distinct from conflict/not-found)", errUpd)
	gotAfter, _ := c.store.Get("w1")
	if gotAfter.Spec.Message == "restored?" {
		t.Fatal("rejected update mutated deleting object")
	}

	// Deletion reconcile: detach exactly once and remove finalizer -> purge.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Reconcile(ctx, "w1"); err != nil {
		t.Fatalf("reconcile deletion: %v", err)
	}
	if c.external.Attached("w1") {
		t.Fatal("external attachment still present after deletion reconcile")
	}
	// 1 attach during converge + 1 detach during deletion = 2 total, with
	// exactly one mutation of each kind (no duplicated side effects).
	if c.external.OpCount("w1") != 2 {
		t.Fatalf("want 2 external ops (1 attach + 1 detach), got %d", c.external.OpCount("w1"))
	}
	if _, err := c.store.Get("w1"); !isReject(err, storage.ErrNotFound) {
		t.Fatalf("purged object: want ErrNotFound, got %v", err)
	}
	t.Logf("judgement: detach performed exactly once (external-ops=%d), finalizer removed, object purged reason=%v",
		c.external.OpCount("w1"), err)

	// Repeated deletion reconciliation is a no-op: no second detach side effect.
	if err := c.Reconcile(ctx, "w1"); err != nil {
		t.Fatalf("repeated deletion reconcile: %v", err)
	}
	if c.external.OpCount("w1") != 2 {
		t.Fatalf("repeated finalization produced extra side effects: ops=%d", c.external.OpCount("w1"))
	}
	t.Logf("judgement: repeated finalization no-op, external-ops stayed=%d", c.external.OpCount("w1"))
}

func TestDeletionReasonsAreDistinguishable(t *testing.T) {
	c, _ := newTestController(t, 5*time.Millisecond, 50*time.Millisecond, nil)
	createWidget(t, c.store, "w1", "hello")
	converge(t, c, "w1")
	w, _ := c.store.Get("w1")
	rv := w.Metadata.ResourceVersion

	// 1) stale version against a live object -> conflict.
	_, conflictErr := c.store.Update("w1", rv-1, func(cur *v1.Widget) (*v1.Widget, error) { return cur, nil })
	// 2) delete the object-with-finalizer -> terminating.
	term, derr := c.store.Delete("w1")
	if derr != nil {
		t.Fatal(derr)
	}
	_, deletingErr := c.store.Update("w1", term.Metadata.ResourceVersion, func(cur *v1.Widget) (*v1.Widget, error) { return cur, nil })
	// 3) remove finalizer -> purged; then a write -> not-found.
	if _, err := c.store.RemoveFinalizer("w1", term.Metadata.ResourceVersion, v1.FinalizerAttached); err != nil {
		t.Fatalf("remove finalizer: %v", err)
	}
	_, notFoundErr := c.store.Update("w1", term.Metadata.ResourceVersion, func(cur *v1.Widget) (*v1.Widget, error) { return cur, nil })

	reasons := []error{conflictErr, deletingErr, notFoundErr}
	want := []error{storage.ErrVersionConflict, storage.ErrDeleting, storage.ErrNotFound}
	t.Logf("event: three rejected writes; judgement must be pairwise distinct:")
	for i := range reasons {
		if !isReject(reasons[i], want[i]) {
			t.Fatalf("rejection %d: want %v, got %v", i, want[i], reasons[i])
		}
		t.Logf("  rejection[%d] reason=%v", i, reasons[i])
	}
	for i := range reasons {
		for j := i + 1; j < len(reasons); j++ {
			if reasons[i].Error() == reasons[j].Error() {
				t.Fatalf("rejection reasons %d and %d are indistinguishable", i, j)
			}
		}
	}
}

func isReject(err, target error) bool { return errors.Is(err, target) }

func TestSingleInFlightReconcilePerObject(t *testing.T) {
	var inFlight, maxInFlight int64
	gate := make(chan struct{})
	var gateOnce sync.Once
	hook := func(name string, attempt int) error {
		if name != "hot" {
			return nil
		}
		cur := atomic.AddInt64(&inFlight, 1)
		for {
			old := atomic.LoadInt64(&maxInFlight)
			if cur <= old || atomic.CompareAndSwapInt64(&maxInFlight, old, cur) {
				break
			}
		}
		gateOnce.Do(func() {
			<-gate // hold the first "hot" reconcile while duplicates arrive
		})
		atomic.AddInt64(&inFlight, -1)
		return nil
	}
	var buf bytes.Buffer
	c := New(Config{
		Store: storage.NewStore(), Workers: 4,
		BaseDelay: 5 * time.Millisecond, MaxDelay: 50 * time.Millisecond,
		FailHook: hook, LogWriter: io.MultiWriter(&buf, testWriter{t}),
	})

	for i := 0; i < 8; i++ {
		createWidget(t, c.store, "other-"+string(rune('a'+i)), "x")
	}
	createWidget(t, c.store, "hot", "x")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, 5*time.Second) }()

	// 100 duplicate events while the single in-flight reconcile is gated.
	for i := 0; i < 100; i++ {
		if !c.Submit(Event{Kind: EventUpsert, Name: "hot"}) {
			t.Fatal("Submit rejected while running")
		}
	}
	time.Sleep(50 * time.Millisecond) // let other objects drain and "hot" enter gate
	close(gate)

	if !waitFor(2*time.Second, func() bool {
		w, err := c.store.Get("hot")
		return err == nil && w.AggregatedPhase() == v1.WidgetStatusReady
	}) {
		t.Fatal("hot never converged")
	}
	cancel()
	<-done

	t.Logf("event: 100 duplicate upserts for name=hot under 4 workers; judgement: maxConcurrentReconciles=%d attempts=%d (must be 1, duplicates coalesced)",
		atomic.LoadInt64(&maxInFlight), c.Attempts("hot"))
	if atomic.LoadInt64(&maxInFlight) != 1 {
		t.Fatalf("overlapping reconciliations for one object: max=%d", maxInFlight)
	}
	if c.external.OpCount("hot") != 1 {
		t.Fatalf("duplicate events caused %d attach ops, want 1", c.external.OpCount("hot"))
	}
}

func TestShutdownDrainsAcceptedEvents(t *testing.T) {
	c, _ := newTestController(t, 5*time.Millisecond, 50*time.Millisecond, nil)
	const n = 150
	for i := 0; i < n; i++ {
		createWidget(t, c.store, "w"+padIndex(i), "x")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, 10*time.Second) }()

	accepted := 0
	for i := 0; i < n; i++ {
		if c.Submit(Event{Kind: EventUpsert, Name: "w" + padIndex(i)}) {
			accepted++
		}
	}
	t.Logf("event: accepted=%d upserts, then immediate shutdown signal", accepted)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run during drain: %v", err)
	}

	converged := 0
	for i := 0; i < n; i++ {
		w, err := c.store.Get("w" + padIndex(i))
		if err == nil && w.AggregatedPhase() == v1.WidgetStatusReady {
			converged++
		}
	}
	t.Logf("judgement: converged=%d/%d after graceful drain (accepted events must not be lost)", converged, accepted)
	if converged != accepted {
		t.Fatalf("lost accepted events during shutdown: converged=%d accepted=%d", converged, accepted)
	}
	if c.Submit(Event{Kind: EventUpsert, Name: "late"}) {
		t.Fatal("Submit after shutdown must be rejected (observable non-acceptance)")
	}
	t.Logf("judgement: post-shutdown Submit rejected; event log contains exactly the accepted events")
	if len(c.EventLog()) != accepted {
		t.Fatalf("event log size=%d want %d", len(c.EventLog()), accepted)
	}
}

func TestShutdownDrainBoundedRetriesWithFastForward(t *testing.T) {
	// Permanently failing object: during normal running it keeps backing off;
	// after the shutdown signal pending delays fast-forward and retries are
	// bounded by MaxAttempts so exit cannot wedge.
	c, buf := newTestController(t, time.Hour, time.Hour, func(name string, attempt int) error {
		if name == "bad" {
			return errInjected(1)
		}
		return nil
	})
	createWidget(t, c.store, "bad", "x")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, 3*time.Second) }()

	c.Submit(Event{Kind: EventUpsert, Name: "bad"})
	if !waitFor(time.Second, func() bool { return c.Attempts("bad") >= 1 }) {
		t.Fatal("first attempt never happened")
	}
	cancel() // while the next retry is delayed for ~1h
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("drain wedged on the hour-long backoff delay")
	}
	t.Logf("event: shutdown with a 1h pending backoff; judgement: attempts=%d give-up-lines=%d (delay fast-forwarded, retries bounded)",
		c.Attempts("bad"), strings.Count(buf.String(), "GIVE-UP"))
	if c.Attempts("bad") != c.cfg.MaxAttempts {
		t.Fatalf("expected exactly MaxAttempts=%d attempts, got %d", c.cfg.MaxAttempts, c.Attempts("bad"))
	}
	if !strings.Contains(buf.String(), "GIVE-UP") {
		t.Fatalf("expected observable GIVE-UP decision in logs:\n%s", buf.String())
	}
}

func padIndex(i int) string {
	if i < 10 {
		return "00" + string(rune('0'+i))
	}
	if i < 100 {
		return "0" + string(rune('0'+i/10)) + string(rune('0'+i%10))
	}
	return string(rune('0'+(i/100)%10)) + string(rune('0'+(i/10)%10)) + string(rune('0'+i%10))
}
