// Package controller implements the reconcile loop: single in-flight
// reconciliation per object (the workqueue coalesces duplicate change
// events), observable exponential-backoff retries, finalizer-driven deletion
// and graceful shutdown that drains every accepted event before returning.
package controller

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/highcumontoa/k8s-resource-controller-go/pkg/apis/v1"
	"github.com/highcumontoa/k8s-resource-controller-go/pkg/storage"
)

// EventKind identifies the change that produced an event.
type EventKind string

const (
	EventUpsert EventKind = "Upsert"
	EventDelete EventKind = "Delete"
)

// Event is a change notification accepted by the controller. ResourceVersion
// is stamped at accept time (zero if the object is already gone), giving the
// event log a stable, printable ordering basis.
type Event struct {
	Kind            EventKind
	Name            string
	ResourceVersion int64
	AcceptedAt      time.Time
}

// ReconcileHook optionally wraps reconcile attempts; returning an error makes
// the attempt fail deterministically (used by tests to inject failures).
type ReconcileHook func(name string, attempt int) error

// Config wires a controller.
type Config struct {
	Store   *storage.Store
	Workers int
	// BaseDelay and MaxDelay define the observable backoff policy.
	BaseDelay time.Duration
	MaxDelay  time.Duration
	// MaxAttempts bounds failed retries per object while draining after a
	// shutdown signal; 0 means unlimited during normal running.
	MaxAttempts int
	// FailHook, when non-nil and returning an error, fails the reconcile.
	FailHook ReconcileHook
	// LogWriter receives version/event/decision logs; stderr when nil.
	LogWriter io.Writer
}

// Controller runs the reconcile loop over the store.
type Controller struct {
	cfg      Config
	store    *storage.Store
	external *externalSystem
	queue    *coalescingDelayQueue
	log      *log.Logger

	acceptMu sync.Mutex
	closed   bool // set once when shutdown begins; Submit then rejects
	eventMu  sync.Mutex
	events   []Event

	statMu   sync.Mutex
	attempts map[string]int
	fails    map[string]int
}

// New constructs a controller.
func New(cfg Config) *Controller {
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = 10 * time.Millisecond
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = time.Second
	}
	w := cfg.LogWriter
	if w == nil {
		w = os.Stderr
	}
	return &Controller{
		cfg:      cfg,
		store:    cfg.Store,
		external: newExternalSystem(),
		queue:    newCoalescingDelayQueue(),
		log:      log.New(w, "controller: ", log.LstdFlags|log.Lmicroseconds),
		attempts: make(map[string]int),
		fails:    make(map[string]int),
	}
}

// Submit accepts a change event. Accepted events are appended to an in-memory
// log and enqueued; none are lost after a shutdown signal because Run drains
// the queue before returning. It returns false once shutdown has begun so
// callers can tell an accepted event from a rejected one.
func (c *Controller) Submit(ev Event) bool {
	c.acceptMu.Lock()
	if c.closed {
		c.acceptMu.Unlock()
		return false
	}
	c.acceptMu.Unlock()

	ev.AcceptedAt = time.Now()
	if w, err := c.store.Get(ev.Name); err == nil {
		ev.ResourceVersion = w.Metadata.ResourceVersion
	}
	c.eventMu.Lock()
	c.events = append(c.events, ev)
	c.eventMu.Unlock()

	c.log.Printf("accept event kind=%s name=%s rv=%d", ev.Kind, ev.Name, ev.ResourceVersion)
	c.queue.Add(ev.Name)
	return true
}

// EventLog returns a copy of the ordered accepted events for test inspection.
func (c *Controller) EventLog() []Event {
	c.eventMu.Lock()
	defer c.eventMu.Unlock()
	return append([]Event(nil), c.events...)
}

// backoff computes the n-th (zero-based) retry delay: exponential growth with
// factor 2, capped at MaxDelay. The policy is fully observable in logs.
func (c *Controller) backoff(failedAttempts int) time.Duration {
	d := c.cfg.BaseDelay << failedAttempts
	if d <= 0 || d > c.cfg.MaxDelay {
		d = c.cfg.MaxDelay
	}
	return d
}

// Run starts workers and blocks until ctx is cancelled, after which the queue
// is closed (pending delays fast-forwarded) and every still-queued accepted
// event is processed, bounded by drainTimeout. Failed attempts during the
// drain honor MaxAttempts so a permanently failing handler cannot wedge exit.
func (c *Controller) Run(ctx context.Context, drainTimeout time.Duration) error {
	var workers sync.WaitGroup
	for i := 0; i < c.cfg.Workers; i++ {
		workers.Add(1)
		go func(id int) {
			defer workers.Done()
			for {
				key, shutdown := c.queue.Get()
				if shutdown {
					return
				}
				c.processOne(ctx, key)
				c.queue.Done(key)
			}
		}(i)
	}

	<-ctx.Done()
	c.log.Printf("shutdown signal received: stop accepting events and drain")

	// From this point Submit rejects new events; every event accepted before
	// is already in the queue, which Shutdown() preserves and fast-forwards.
	c.acceptMu.Lock()
	c.closed = true
	c.acceptMu.Unlock()
	c.queue.Shutdown()

	drainCtx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()
	drainEnded := make(chan struct{})
	go func() { workers.Wait(); close(drainEnded) }()
	select {
	case <-drainEnded:
		c.log.Printf("drain complete: all accepted events processed")
		return nil
	case <-drainCtx.Done():
		c.log.Printf("drain timeout after %s: %d entries left unprocessed", drainTimeout, c.queue.Len())
		return fmt.Errorf("controller drain timed out: %d events unprocessed", c.queue.Len())
	}
}

// processOne runs one reconciliation, applies the retry policy and keeps the
// attempt/failure counters that tests assert on.
func (c *Controller) processOne(ctx context.Context, key string) {
	c.statMu.Lock()
	c.attempts[key]++
	attempt := c.attempts[key]
	c.statMu.Unlock()

	err := c.Reconcile(ctx, key)
	if err == nil {
		c.statMu.Lock()
		c.fails[key] = 0
		c.statMu.Unlock()
		return
	}

	c.statMu.Lock()
	c.fails[key]++
	fails := c.fails[key]
	draining := c.queue.ShuttingDown()
	c.statMu.Unlock()

	if draining && c.cfg.MaxAttempts > 0 && fails >= c.cfg.MaxAttempts {
		c.log.Printf("name=%s attempt=%d GIVE-UP reason=max-attempts-during-drain err=%q", key, attempt, err)
		return
	}
	delay := c.backoff(fails - 1)
	c.log.Printf("name=%s attempt=%d RETRY after=%s reason=%q", key, attempt, delay, err)
	if draining {
		// Fast-forward backoff while draining so shutdown stays bounded.
		c.queue.Add(key)
	} else {
		c.queue.AddAfter(key, delay)
	}
}

// Reconcile performs one idempotent reconciliation pass for the given object:
//   - missing object: nothing to do (already purged after its last finalizer);
//   - deleting object with our finalizer: detach external resource, remove the
//     finalizer (the store purges the object on the last finalizer);
//   - live object: ensure the finalizer, ensure the external attachment, then
//     aggregate status = Ready with the observed resource version.
//
// All external mutations are idempotent, so repeated events never produce
// repeated side effects.
func (c *Controller) Reconcile(ctx context.Context, name string) error {
	w, err := c.store.Get(name)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			c.log.Printf("name=%s decision=noop reason=object-not-found", name)
			return nil
		}
		return err
	}
	rv := w.Metadata.ResourceVersion

	if w.Metadata.DeletionTimestamp != "" {
		return c.reconcileDelete(w)
	}
	return c.reconcileUpsert(w, rv)
}

func (c *Controller) reconcileDelete(w *v1.Widget) error {
	name := w.Metadata.Name
	if !hasFinalizer(w, v1.FinalizerAttached) {
		c.log.Printf("name=%s rv=%d decision=noop reason=finalizer-already-gone", name, w.Metadata.ResourceVersion)
		return nil
	}

	// Detach is idempotent in the external system; only a real removal counts.
	if err := c.external.Detach(name); err != nil {
		return fmt.Errorf("detach: %w", err)
	}
	c.log.Printf("name=%s rv=%d decision=detached external-attached=%v", name, w.Metadata.ResourceVersion, c.external.Attached(name))

	_, err := c.store.RemoveFinalizer(name, w.Metadata.ResourceVersion, v1.FinalizerAttached)
	switch {
	case err == nil:
		c.log.Printf("name=%s rv=%d decision=finalizer-removed object-purged", name, w.Metadata.ResourceVersion)
		return nil
	case errors.Is(err, storage.ErrVersionConflict):
		// Object changed since Get: requeue, next attempt has the fresh version.
		c.log.Printf("name=%s rv=%d decision=requeue reason=stale-version", name, w.Metadata.ResourceVersion)
		return err
	case errors.Is(err, storage.ErrNotFound):
		c.log.Printf("name=%s rv=%d decision=noop reason=object-already-gone", name, w.Metadata.ResourceVersion)
		return nil
	case errors.Is(err, storage.ErrFinalizerNotFound):
		c.log.Printf("name=%s rv=%d decision=noop reason=finalizer-concurrently-removed", name, w.Metadata.ResourceVersion)
		return nil
	default:
		return err
	}
}

func (c *Controller) reconcileUpsert(w *v1.Widget, rv int64) error {
	name := w.Metadata.Name

	if err := c.ensureFinalizer(&w, rv); err != nil {
		if errors.Is(err, storage.ErrVersionConflict) {
			c.log.Printf("name=%s rv=%d decision=requeue reason=stale-version-at-finalizer", name, rv)
		}
		return err
	}
	// rv stays valid when the finalizer was already present; otherwise a
	// finalizer write advanced it, in which case the latest copy is used so
	// the following status write does not fail against our own mutation.
	if w.Metadata.ResourceVersion != rv {
		rv = w.Metadata.ResourceVersion
	}

	c.statMu.Lock()
	attempt := c.attempts[name]
	c.statMu.Unlock()
	if c.cfg.FailHook != nil {
		if herr := c.cfg.FailHook(name, attempt); herr != nil {
			c.markDegraded(name, rv, "ReconcileHookFailure", herr)
			return herr
		}
	}

	if err := c.external.EnsureAttach(name, w.Spec.Message); err != nil {
		c.markDegraded(name, rv, "AttachFailure", err)
		return fmt.Errorf("ensure attach: %w", err)
	}
	c.log.Printf("name=%s rv=%d decision=attached external-ops=%d", name, rv, c.external.OpCount(name))

	// A repeated event for an already-converged object performs no write at
	// all: duplicate change events produce neither external nor storage side
	// effects. (rv is the current version after any finalizer write above.)
	if w.Status.Phase == v1.WidgetStatusReady && w.Status.ObservedRV == w.Metadata.ResourceVersion {
		c.log.Printf("name=%s rv=%d decision=noop reason=already-in-sync external-attached=%v", name, rv, c.external.Attached(name))
		return nil
	}

	updated, err := c.store.Update(name, rv, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Status.Phase = v1.WidgetStatusReady
		cur.Status.ObservedRV = cur.Metadata.ResourceVersion
		cur.Status.Conditions = []v1.Condition{{
			Type:    "Synced",
			Status:  "True",
			Reason:  "AttachmentEnsured",
			Message: "external attachment matches desired state",
		}}
		return cur, nil
	})
	if err != nil {
		if errors.Is(err, storage.ErrVersionConflict) {
			c.log.Printf("name=%s rv=%d decision=requeue reason=stale-version-at-status", name, rv)
		}
		return err
	}
	c.log.Printf("name=%s oldrv=%d newrv=%d decision=status-ready phase=%s", name, rv, updated.Metadata.ResourceVersion, updated.AggregatedPhase())
	return nil
}

// markDegraded makes a best-effort status write recording the failure; if the
// object changed concurrently the write is refused and the caller's returned
// error drives a retry at the new version.
func (c *Controller) markDegraded(name string, rv int64, reason string, cause error) {
	_, err := c.store.Update(name, rv, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Status.Phase = v1.WidgetStatusDegraded
		cur.Status.Conditions = []v1.Condition{{
			Type:    "Synced",
			Status:  "False",
			Reason:  reason,
			Message: cause.Error(),
		}}
		return cur, nil
	})
	if err != nil {
		c.log.Printf("name=%s rv=%d decision=keep-old-status reason=%q", name, rv, err)
		return
	}
	c.log.Printf("name=%s rv=%d decision=status-degraded reason=%s cause=%q", name, rv, reason, cause)
}

// ensureFinalizer appends the managed finalizer exactly once and refreshes
// w to the post-write copy when a write actually happened.
func (c *Controller) ensureFinalizer(w **v1.Widget, expectedRV int64) error {
	name := (*w).Metadata.Name
	if hasFinalizer(*w, v1.FinalizerAttached) {
		return nil
	}
	updated, err := c.store.Update(name, expectedRV, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Metadata.Finalizers = append(cur.Metadata.Finalizers, v1.FinalizerAttached)
		return cur, nil
	})
	if err == nil {
		*w = updated
		c.log.Printf("name=%s rv=%d newrv=%d decision=finalizer-added", name, expectedRV, updated.Metadata.ResourceVersion)
	}
	return err
}

// External exposes the simulated outside world for assertions.
func (c *Controller) External() *externalSystem { return c.external }

// Attempts returns the number of reconcile attempts observed for the object.
func (c *Controller) Attempts(name string) int {
	c.statMu.Lock()
	defer c.statMu.Unlock()
	return c.attempts[name]
}

func hasFinalizer(w *v1.Widget, f string) bool {
	for _, x := range w.Metadata.Finalizers {
		if x == f {
			return true
		}
	}
	return false
}
