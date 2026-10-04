// Command demo runs the in-memory Kubernetes-style controller against a
// handful of demo resources. Send SIGINT/SIGTERM to observe graceful drain:
// accepted events are processed (delayed retries fast-forwarded) before exit.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/highcumontoa/k8s-resource-controller-go/pkg/apis/v1"
	"github.com/highcumontoa/k8s-resource-controller-go/pkg/controller"
	"github.com/highcumontoa/k8s-resource-controller-go/pkg/storage"
)

func main() {
	var (
		flaky       = flag.Int("flaky", 2, "number of transient attach failures for demo 'flaky' (demonstrates backoff retry)")
		drain       = flag.Duration("drain", 5*time.Second, "max graceful drain time after signal")
		showRejects = flag.Bool("show-rejects", true, "also demonstrate distinguishable write rejections")
		showBatch   = flag.Bool("show-batch", true, "also demonstrate version-conditional precheck + batch delete")
	)
	flag.Parse()

	store := storage.NewStore()

	// External attach failures are pre-armed; the controller must retry with
	// the observable exponential backoff and converge without duplicate ops.
	var c *controller.Controller
	c = controller.New(controller.Config{
		Store:       store,
		Workers:     2,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    2 * time.Second,
		MaxAttempts: 5,
		LogWriter:   os.Stdout,
	})
	if *flaky > 0 {
		c.External().ArmFailures("flaky", *flaky)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	runErr := make(chan error, 1)
	go func() { runErr <- c.Run(ctx, *drain) }()

	// Simulated API clients submit change events.
	create := func(name, msg string) int64 {
		w, err := store.Create(&v1.Widget{
			Metadata: v1.ObjectMeta{Name: name},
			Spec:     v1.WidgetSpec{Message: msg},
		})
		if err != nil {
			panic(err)
		}
		c.Submit(controller.Event{Kind: controller.EventUpsert, Name: name})
		return w.Metadata.ResourceVersion
	}

	rvHealthy := create("healthy", "desired-message")
	create("flaky", "retry-me")

	if *showRejects {
		// 1) stale version
		if _, err := store.Update("healthy", rvHealthy-1, func(cur *v1.Widget) (*v1.Widget, error) {
			cur.Spec.Message = "stale write"
			return cur, nil
		}); err != nil {
			fmt.Printf("[demo] stale write rejected: %v\n", err)
		}
		// 2) not found
		if _, err := store.Update("missing", 1, func(cur *v1.Widget) (*v1.Widget, error) { return cur, nil }); err != nil {
			fmt.Printf("[demo] missing write rejected: %v\n", err)
		}
	}

	// Wait for convergence, then demonstrate finalizer-based deletion.
	deleted := false
	if !waitFor(ctx, store, "flaky", v1.WidgetStatusReady, 5*time.Second) {
		fmt.Println("[demo] shutdown requested before flaky converged; draining and exiting")
	} else if w, err := store.Get("healthy"); err == nil {
		fmt.Printf("[demo] delete healthy (rv=%d finalizers=%v)\n", w.Metadata.ResourceVersion, w.Metadata.Finalizers)
		term, err := store.Delete("healthy")
		if err != nil {
			panic(err)
		}
		c.Submit(controller.Event{Kind: controller.EventDelete, Name: "healthy"})
		deleted = true

		// Ordinary updates on a terminating object are rejected distinctly.
		if _, err := store.Update("healthy", term.Metadata.ResourceVersion, func(cur *v1.Widget) (*v1.Widget, error) {
			cur.Status.Phase = v1.WidgetStatusReady
			return cur, nil
		}); err != nil {
			fmt.Printf("[demo] write during deletion rejected: %v\n", err)
		}
	}

	if deleted && waitForGone(ctx, store, "healthy", 2*time.Second) {
		fmt.Println("[demo] healthy purged after finalization")
	} else if deleted {
		fmt.Println("[demo] shutdown while waiting for finalization")
	}

	if *showBatch && deleted {
		demoBatchDelete(c, store)
	}

	if deleted {
		fmt.Println("[demo] send Ctrl-C / SIGTERM to drain and exit (auto-exit in 1s)")
		select {
		case <-time.After(time.Second):
			stop()
		case <-ctx.Done():
		}
	}

	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		fmt.Printf("[demo] controller stopped with error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("[demo] stopped; accepted events processed=%d\n", len(c.EventLog()))
}

// demoBatchDelete showcases the version-conditional delete path: a read-only
// precheck per object, then one batch execution whose per-object results
// match the precheck. "batch-stale" is modified after the read so its delete
// is rejected with a version conflict while "batch-ok" is accepted.
func demoBatchDelete(c *controller.Controller, store *storage.Store) {
	ctx := context.Background()
	for _, name := range []string{"batch-ok", "batch-stale"} {
		if _, err := store.Create(&v1.Widget{
			Metadata: v1.ObjectMeta{Name: name},
			Spec:     v1.WidgetSpec{Message: "batch-demo"},
		}); err != nil {
			panic(err)
		}
		c.Submit(controller.Event{Kind: controller.EventUpsert, Name: name})
	}
	if !waitFor(ctx, store, "batch-stale", v1.WidgetStatusReady, 5*time.Second) {
		fmt.Println("[demo] batch objects did not converge in time; skipping batch demo")
		return
	}

	ok, _ := store.Get("batch-ok")
	stale, _ := store.Get("batch-stale")
	intents := []storage.DeleteIntent{
		{Name: "batch-ok", ResourceVersion: ok.Metadata.ResourceVersion},
		{Name: "batch-stale", ResourceVersion: stale.Metadata.ResourceVersion},
		{Name: "batch-ghost", ResourceVersion: 1},
	}

	// A concurrent write lands between the caller's read and the delete.
	if _, err := store.Update("batch-stale", stale.Metadata.ResourceVersion, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Spec.Message = "changed-after-read"
		return cur, nil
	}); err != nil {
		panic(err)
	}

	fmt.Println("[demo] delete precheck:")
	for _, r := range c.CheckDeleteBatch(intents) {
		fmt.Printf("[demo]   precheck name=%s outcome=%s reason=%v observed-rv=%d\n",
			r.Name, r.Outcome, r.Reason, r.ResourceVersion)
	}
	fmt.Println("[demo] batch delete execution:")
	for _, r := range c.DeleteBatch(intents) {
		fmt.Printf("[demo]   execute name=%s outcome=%s reason=%v rv=%d\n",
			r.Name, r.Outcome, r.Reason, r.ResourceVersion)
	}
	if waitForGone(ctx, store, "batch-ok", 2*time.Second) {
		fmt.Println("[demo] batch-ok purged after finalization; batch-stale kept (stale version), batch-ghost never existed")
	}
}

// waitFor polls until the object reaches phase or ctx ends; false means the
// wait was abandoned due to shutdown (not a demo failure in that case).
func waitFor(ctx context.Context, store *storage.Store, name, phase string, within time.Duration) bool {
	deadline := time.Now().Add(within)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for time.Now().Before(deadline) {
		if w, err := store.Get(name); err == nil && w.AggregatedPhase() == phase {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
	return false
}

func waitForGone(ctx context.Context, store *storage.Store, name string, within time.Duration) bool {
	deadline := time.Now().Add(within)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for time.Now().Before(deadline) {
		if _, err := store.Get(name); errors.Is(err, storage.ErrNotFound) {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
	return false
}
