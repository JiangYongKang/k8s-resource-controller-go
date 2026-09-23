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
		fmt.Println("[demo] healthy purged after finalization; send Ctrl-C / SIGTERM to drain and exit (auto-exit in 1s)")
		select {
		case <-time.After(time.Second):
			stop()
		case <-ctx.Done():
		}
	} else if deleted {
		fmt.Println("[demo] shutdown while waiting for finalization")
	}

	if err := <-runErr; err != nil && !errors.Is(err, context.Canceled) {
		fmt.Printf("[demo] controller stopped with error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("[demo] stopped; accepted events processed=%d\n", len(c.EventLog()))
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
