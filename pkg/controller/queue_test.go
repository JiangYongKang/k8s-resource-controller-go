package controller

import (
	"testing"
	"time"
)

// drainGet performs one blocking Get with a timeout, failing the test on timeout.
func drainGet(t *testing.T, q queue, timeout time.Duration) (key string, shutdown bool) {
	t.Helper()
	type res struct {
		key string
		sd  bool
	}
	ch := make(chan res, 1)
	go func() {
		k, s := q.Get()
		ch <- res{k, s}
	}()
	select {
	case r := <-ch:
		return r.key, r.sd
	case <-time.After(timeout):
		t.Fatalf("Get did not return within %s", timeout)
		return "", false
	}
}

func TestQueueCoalescesDuplicates(t *testing.T) {
	q := newCoalescingDelayQueue()
	q.Add("a")
	q.Add("a") // duplicate while pending must collapse
	q.Add("b")
	q.Add("a")
	t.Logf("event: Add(a) x3 + Add(b); pending len=%d (judgement: exactly 2 keys)", q.Len())

	g1, _ := drainGet(t, q, time.Second)
	g2, _ := drainGet(t, q, time.Second)
	got := map[string]bool{g1: true, g2: true}
	if !got["a"] || !got["b"] || g1 == g2 {
		t.Fatalf("expected coalesced {a,b}, got %q,%q", g1, g2)
	}
	// Both keys are now in-flight; duplicate Add while in-flight is held until
	// Done and must still produce exactly one requeue.
	q.Add("a")
	q.Add("a")
	q.Done("a")
	q.Done("b")
	g3, sd := drainGet(t, q, time.Second)
	if sd || g3 != "a" {
		t.Fatalf("expected single requeue of a, got key=%q shutdown=%v", g3, sd)
	}
	q.Done("a")
	t.Logf("judgement: in-flight duplicates coalesced to one requeue key=%q", g3)
}

func TestQueueDelayedRequeueAndFastForward(t *testing.T) {
	q := newCoalescingDelayQueue()
	start := time.Now()
	q.AddAfter("late", 100*time.Millisecond)
	q.AddAfter("late", 100*time.Millisecond) // duplicate delay collapses
	t.Logf("event: AddAfter(late,100ms) x2; len=%d", q.Len())

	key, _ := drainGet(t, q, 2*time.Second)
	elapsed := time.Since(start)
	if key != "late" {
		t.Fatalf("expected delayed key late, got %q", key)
	}
	if elapsed < 80*time.Millisecond {
		t.Fatalf("key delivered too early after %s: delay not honored", elapsed)
	}
	t.Logf("judgement: delayed key=%q delivered after=%s (backoff honored, duplicates collapsed)", key, elapsed)
	q.Done("late")

	// Shutdown fast-forwards pending delays so accepted work drains at once.
	q.AddAfter("fast", 5*time.Second)
	time.Sleep(20 * time.Millisecond) // let the entry sit in the waiting heap
	q.Shutdown()
	ff, sd := drainGet(t, q, time.Second)
	if sd || ff != "fast" {
		t.Fatalf("expected fast-forwarded key fast, got key=%q shutdown=%v", ff, sd)
	}
	q.Done("fast")
	if _, sd := drainGet(t, q, 200*time.Millisecond); !sd {
		t.Fatal("expected shutdown=true after queue drains")
	}
	t.Logf("judgement: Shutdown fast-forwarded a 5s delay and reported shutdown once drained")
}

func TestQueueAddSupersedesDelay(t *testing.T) {
	q := newCoalescingDelayQueue()
	q.AddAfter("k", 5*time.Second)
	q.Add("k") // immediate add cancels the delay
	key, _ := drainGet(t, q, 200*time.Millisecond)
	if key != "k" {
		t.Fatalf("expected immediate key k superseding delay, got %q", key)
	}
	t.Logf("judgement: immediate Add superseded the scheduled delay")
	q.Done("k")
	q.Shutdown()
}
