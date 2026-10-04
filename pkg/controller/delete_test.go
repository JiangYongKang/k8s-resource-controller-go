package controller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/k8s-resource-controller-go/pkg/apis/v1"
	"github.com/highcumontoa/k8s-resource-controller-go/pkg/storage"
)

// reconcileToReady creates the object and runs one reconcile pass so it is
// Ready with the managed finalizer attached; returns the current version.
func reconcileToReady(t *testing.T, c *Controller, name, msg string) int64 {
	t.Helper()
	createWidget(t, c.store, name, msg)
	if err := c.Reconcile(context.Background(), name); err != nil {
		t.Fatalf("reconcile %s: %v", name, err)
	}
	w, err := c.store.Get(name)
	if err != nil {
		t.Fatalf("get %s: %v", name, err)
	}
	if w.AggregatedPhase() != v1.WidgetStatusReady {
		t.Fatalf("%s not Ready: %s", name, w.AggregatedPhase())
	}
	return w.Metadata.ResourceVersion
}

// drainUntil runs the controller until cond holds or the timeout elapses,
// then cancels and waits for the drain.
func drainUntil(t *testing.T, c *Controller, timeout time.Duration, cond func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, 5*time.Second) }()
	if !waitFor(timeout, cond) {
		cancel()
		<-done
		t.Fatal("condition not reached before timeout")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run returned error: %v", err)
	}
}

// TestVersionedDeleteConvergesToPurge deletes a Ready object at its read
// version: the delete is accepted as Terminating, the object stays readable
// in that state, and the submitted delete event converges it to the final
// purge with the external attachment detached exactly once.
func TestVersionedDeleteConvergesToPurge(t *testing.T) {
	c, _ := newTestController(t, 5*time.Millisecond, 50*time.Millisecond, nil)
	rv := reconcileToReady(t, c, "w1", "hello")
	t.Logf("event: object Ready at rv=%d, deleting conditionally", rv)

	res := c.DeleteWithVersion("w1", rv)
	t.Logf("judgement: delete outcome=%s reason=%v newrv=%d", res.Outcome, res.Reason, res.ResourceVersion)
	if res.Outcome != storage.DeleteOutcomeTerminating || res.ResourceVersion != rv+1 {
		t.Fatalf("want Terminating at rv=%d, got %+v", rv+1, res)
	}

	got, err := c.store.Get("w1")
	if err != nil || got.AggregatedPhase() != v1.WidgetStatusTerminating {
		t.Fatalf("terminating object must stay readable as Terminating: %v %v", got, err)
	}

	drainUntil(t, c, 5*time.Second, func() bool {
		_, err := c.store.Get("w1")
		return errors.Is(err, storage.ErrNotFound)
	})
	if c.external.Attached("w1") {
		t.Fatal("external attachment still present after purge")
	}
	t.Logf("judgement: delete event converged to purge; external ops=%d (attach+detach, no duplicates)",
		c.external.OpCount("w1"))
	if c.external.OpCount("w1") != 2 {
		t.Fatalf("expected exactly 2 external ops (attach+detach), got %d", c.external.OpCount("w1"))
	}
}

// TestVersionedDeleteStaleRejectedObjectStaysReady: a delete carrying an
// outdated version is rejected, submits no event, and the object keeps
// reconciling normally afterwards.
func TestVersionedDeleteStaleRejectedObjectStaysReady(t *testing.T) {
	c, _ := newTestController(t, 5*time.Millisecond, 50*time.Millisecond, nil)
	rv := reconcileToReady(t, c, "w1", "hello")

	// A concurrent write advances the version after the caller's read.
	w, err := c.store.Update("w1", rv, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Spec.Message = "edited"
		return cur, nil
	})
	if err != nil {
		t.Fatalf("interleaved update: %v", err)
	}
	t.Logf("event: interleaved write advanced rv=%d -> %d", rv, w.Metadata.ResourceVersion)

	res := c.DeleteWithVersion("w1", rv)
	t.Logf("judgement: stale delete outcome=%s reason=%v", res.Outcome, res.Reason)
	if res.Outcome != storage.DeleteOutcomeVersionConflict || !errors.Is(res.Reason, storage.ErrVersionConflict) {
		t.Fatalf("want VersionConflict, got %+v", res)
	}

	got, _ := c.store.Get("w1")
	if got.Metadata.DeletionTimestamp != "" || got.Metadata.ResourceVersion != w.Metadata.ResourceVersion {
		t.Fatalf("rejected delete left a trace: %+v", got.Metadata)
	}
	// The object still reconciles to Ready at its current version.
	if err := c.Reconcile(context.Background(), "w1"); err != nil {
		t.Fatalf("reconcile after rejected delete: %v", err)
	}
	got, _ = c.store.Get("w1")
	t.Logf("judgement: object still reconciles, phase=%s rv=%d", got.AggregatedPhase(), got.Metadata.ResourceVersion)
	if got.AggregatedPhase() != v1.WidgetStatusReady {
		t.Fatalf("phase after rejected delete: want Ready, got %s", got.AggregatedPhase())
	}
}

// TestTerminatingObjectNotRestoredByReconcile: once the versioned delete is
// accepted, no status write can pull the object back to a normal phase, and
// a queued upsert event still converges to the final delete result.
func TestTerminatingObjectNotRestoredByReconcile(t *testing.T) {
	c, _ := newTestController(t, 5*time.Millisecond, 50*time.Millisecond, nil)
	rv := reconcileToReady(t, c, "w1", "hello")

	res := c.DeleteWithVersion("w1", rv)
	if res.Outcome != storage.DeleteOutcomeTerminating {
		t.Fatalf("delete not accepted: %+v", res)
	}

	// A direct status write attempting to restore Ready is refused.
	_, err := c.store.Update("w1", res.ResourceVersion, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Status.Phase = v1.WidgetStatusReady
		return cur, nil
	})
	if !errors.Is(err, storage.ErrDeleting) {
		t.Fatalf("restore write on terminating object: want ErrDeleting, got %v", err)
	}
	got, _ := c.store.Get("w1")
	t.Logf("judgement: restore-to-Ready write rejected reason=%v; phase=%s (stays Terminating)",
		err, got.AggregatedPhase())
	if got.AggregatedPhase() != v1.WidgetStatusTerminating {
		t.Fatalf("terminating object restored: %s", got.AggregatedPhase())
	}

	// An upsert event accepted before/around the delete still converges to
	// the deletion result instead of resurrecting the object.
	c.Submit(Event{Kind: EventUpsert, Name: "w1"})
	drainUntil(t, c, 5*time.Second, func() bool {
		_, err := c.store.Get("w1")
		return errors.Is(err, storage.ErrNotFound)
	})
	t.Logf("judgement: queued upsert event converged to purge, object gone")
}

// TestBatchDeleteViaController exercises precheck + batch execution through
// the controller: per-object results match the precheck, accepted objects
// converge to purge, rejected ones are untouched.
func TestBatchDeleteViaController(t *testing.T) {
	c, _ := newTestController(t, 5*time.Millisecond, 50*time.Millisecond, nil)
	okRV := reconcileToReady(t, c, "ok", "keep-me-deletable")
	staleRV := reconcileToReady(t, c, "stale", "will-change")

	// Interleaved write invalidates the caller's read of "stale".
	if _, err := c.store.Update("stale", staleRV, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Spec.Message = "changed"
		return cur, nil
	}); err != nil {
		t.Fatalf("interleaved update: %v", err)
	}

	intents := []storage.DeleteIntent{
		{Name: "stale", ResourceVersion: staleRV},
		{Name: "ghost", ResourceVersion: 9},
		{Name: "ok", ResourceVersion: okRV},
	}
	pre := c.CheckDeleteBatch(intents)
	for _, r := range pre {
		t.Logf("precheck name=%s outcome=%s observed-rv=%d", r.Name, r.Outcome, r.ResourceVersion)
	}

	results := c.DeleteBatch(intents)
	got := map[string]storage.DeleteOutcome{}
	for _, r := range results {
		t.Logf("execute name=%s outcome=%s reason=%v rv=%d", r.Name, r.Outcome, r.Reason, r.ResourceVersion)
		got[r.Name] = r.Outcome
	}
	if got["ok"] != storage.DeleteOutcomeTerminating ||
		got["stale"] != storage.DeleteOutcomeVersionConflict ||
		got["ghost"] != storage.DeleteOutcomeNotFound {
		t.Fatalf("unexpected batch outcomes: %v", got)
	}

	// Accepted object converges to purge; rejected object keeps running.
	drainUntil(t, c, 5*time.Second, func() bool {
		_, err := c.store.Get("ok")
		return errors.Is(err, storage.ErrNotFound)
	})
	stale, err := c.store.Get("stale")
	if err != nil {
		t.Fatalf("rejected object must survive: %v", err)
	}
	t.Logf("judgement: ok purged; stale untouched phase=%s rv=%d deletionTimestamp=%q",
		stale.AggregatedPhase(), stale.Metadata.ResourceVersion, stale.Metadata.DeletionTimestamp)
	if stale.Metadata.DeletionTimestamp != "" || stale.Spec.Message != "changed" {
		t.Fatalf("rejected object mutated: %+v", stale.Metadata)
	}
}

// TestConcurrentVersionedDeletesViaController: many callers delete the same
// Ready object with the same read-time version; exactly one is accepted and
// the accepted delete converges to a single purge.
func TestConcurrentVersionedDeletesViaController(t *testing.T) {
	c, _ := newTestController(t, 5*time.Millisecond, 50*time.Millisecond, nil)
	rv := reconcileToReady(t, c, "w1", "hello")

	const n = 16
	results := make([]storage.DeleteResult, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = c.DeleteWithVersion("w1", rv)
		}(i)
	}
	wg.Wait()

	accepted, conflicts, deletings := 0, 0, 0
	for _, r := range results {
		switch r.Outcome {
		case storage.DeleteOutcomeTerminating:
			accepted++
		case storage.DeleteOutcomeVersionConflict:
			conflicts++
		case storage.DeleteOutcomeAlreadyDeleting:
			deletings++
		default:
			t.Fatalf("unexpected outcome: %+v", r)
		}
	}
	t.Logf("event: %d concurrent versioned deletes at rv=%d -> accepted=%d version-conflict=%d already-deleting=%d",
		n, rv, accepted, conflicts, deletings)
	if accepted != 1 || conflicts+deletings != n-1 {
		t.Fatalf("exactly one delete may be accepted: accepted=%d conflicts=%d deletings=%d",
			accepted, conflicts, deletings)
	}

	drainUntil(t, c, 5*time.Second, func() bool {
		_, err := c.store.Get("w1")
		return errors.Is(err, storage.ErrNotFound)
	})
	t.Logf("judgement: single accepted delete converged to exactly one purge, external ops=%d",
		c.external.OpCount("w1"))
	if c.external.OpCount("w1") != 2 {
		t.Fatalf("expected exactly 2 external ops, got %d", c.external.OpCount("w1"))
	}
}
