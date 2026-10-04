package controller

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/highcumontoa/k8s-resource-controller-go/pkg/apis/v1"
	"github.com/highcumontoa/k8s-resource-controller-go/pkg/storage"
)

// TestDeleteEventConvergesThroughQueue: a delete accepted while the
// controller is running converges through the work queue to the final purge.
// While the deletion is in flight the object stays readable as Terminating
// and no reconcile pass can pull it back to a normal phase.
func TestDeleteEventConvergesThroughQueue(t *testing.T) {
	var buf bytes.Buffer
	c := New(Config{
		Store: storage.NewStore(), Workers: 2,
		BaseDelay: 5 * time.Millisecond, MaxDelay: 50 * time.Millisecond,
		LogWriter: io.MultiWriter(&buf, testWriter{t}),
	})
	createWidget(t, c.store, "w1", "hello")
	converge(t, c, "w1")
	ready, _ := c.store.Get("w1")
	t.Logf("event: converged name=w1 version=%d phase=%s attached=%v",
		ready.Metadata.ResourceVersion, ready.AggregatedPhase(), c.external.Attached("w1"))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, 5*time.Second) }()

	// Version-conditional delete accepted at the ready version.
	term, err := c.store.DeleteWithVersion("w1", ready.Metadata.ResourceVersion)
	if err != nil {
		t.Fatalf("version-conditional delete: %v", err)
	}
	t.Logf("event: delete accepted oldrv=%d newrv=%d deletionTimestamp=%q",
		ready.Metadata.ResourceVersion, term.Metadata.ResourceVersion, term.Metadata.DeletionTimestamp)
	if !c.Submit(Event{Kind: EventDelete, Name: "w1"}) {
		t.Fatal("delete event not accepted")
	}

	// While the deletion is in flight: readable, Terminating, and a normal
	// update cannot restore it.
	mid, err := c.store.Get("w1")
	if err != nil {
		t.Fatalf("deleting object must stay readable: %v", err)
	}
	if mid.AggregatedPhase() != v1.WidgetStatusTerminating {
		t.Fatalf("in-flight delete must report Terminating, got %s", mid.AggregatedPhase())
	}
	_, updErr := c.store.Update("w1", mid.Metadata.ResourceVersion, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Status.Phase = v1.WidgetStatusReady
		return cur, nil
	})
	if !isReject(updErr, storage.ErrDeleting) {
		t.Fatalf("update during in-flight delete: want ErrDeleting, got %v", updErr)
	}
	t.Logf("judgement: in-flight delete readable phase=%s; restore attempt rejected reason=%v",
		mid.AggregatedPhase(), updErr)

	// The accepted event converges to the final purge.
	if !waitFor(3*time.Second, func() bool {
		_, err := c.store.Get("w1")
		return isReject(err, storage.ErrNotFound)
	}) {
		t.Fatal("accepted delete event never converged to purge")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	t.Logf("judgement: delete event converged to purge; external-ops=%d (1 attach + 1 detach, no duplicates)",
		c.external.OpCount("w1"))
	if c.external.OpCount("w1") != 2 {
		t.Fatalf("want exactly 2 external ops, got %d", c.external.OpCount("w1"))
	}
	if c.external.Attached("w1") {
		t.Fatal("external attachment leaked after purge")
	}
}

// TestStaleDeleteRejectedThenFreshDeleteAccepted: a delete carrying a version
// that a concurrent write has moved past is rejected without any trace; the
// caller can re-read and re-delete at the fresh version, and the object then
// flows through the normal terminating -> purge path.
func TestStaleDeleteRejectedThenFreshDeleteAccepted(t *testing.T) {
	c, _ := newTestController(t, 5*time.Millisecond, 50*time.Millisecond, nil)
	createWidget(t, c.store, "w1", "hello")
	converge(t, c, "w1")
	ready, _ := c.store.Get("w1")
	rv := ready.Metadata.ResourceVersion

	// Concurrent write (e.g. a spec edit) moves the version.
	moved, err := c.store.Update("w1", rv, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Spec.Message = "edited"
		return cur, nil
	})
	if err != nil {
		t.Fatalf("concurrent edit: %v", err)
	}
	t.Logf("event: concurrent edit advanced version %d -> %d", rv, moved.Metadata.ResourceVersion)

	// Delete with the stale read version: rejected, no trace.
	_, delErr := c.store.DeleteWithVersion("w1", rv)
	if !isReject(delErr, storage.ErrVersionConflict) {
		t.Fatalf("stale delete: want ErrVersionConflict, got %v", delErr)
	}
	still, _ := c.store.Get("w1")
	t.Logf("judgement: stale delete rejected reason=%q; version=%d deletionTimestamp=%q (no trace)",
		delErr, still.Metadata.ResourceVersion, still.Metadata.DeletionTimestamp)
	if still.Metadata.ResourceVersion != moved.Metadata.ResourceVersion ||
		still.Metadata.DeletionTimestamp != "" {
		t.Fatal("rejected stale delete left a trace")
	}

	// Re-read, re-delete at the fresh version: accepted, terminating, and the
	// deletion reconcile converges to purge.
	term, err := c.store.DeleteWithVersion("w1", still.Metadata.ResourceVersion)
	if err != nil {
		t.Fatalf("fresh delete: %v", err)
	}
	t.Logf("event: re-delete at fresh version=%d accepted newrv=%d",
		still.Metadata.ResourceVersion, term.Metadata.ResourceVersion)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Reconcile(ctx, "w1"); err != nil {
		t.Fatalf("deletion reconcile: %v", err)
	}
	if _, err := c.store.Get("w1"); !isReject(err, storage.ErrNotFound) {
		t.Fatalf("want purged, got %v", err)
	}
	t.Logf("judgement: fresh delete flowed terminating -> purge; external-ops=%d", c.external.OpCount("w1"))
}
