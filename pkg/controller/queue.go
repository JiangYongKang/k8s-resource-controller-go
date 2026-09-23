package controller

import (
	"container/heap"
	"sync"
	"time"
)

// queue is the small workqueue surface the controller relies on:
//   - name coalescing: at most one ready entry per key, so repeated change
//     events collapse into one reconciliation ("single-flight by key");
//   - delayed requeue (AddAfter) drives the observable per-object backoff;
//   - graceful Shutdown fast-forwards every pending delay so accepted work is
//     drained rather than lost.
type queue interface {
	Add(key string)
	AddAfter(key string, d time.Duration)
	Get() (key string, shutdown bool)
	Done(key string)
	Len() int
	ShuttingDown() bool
	Shutdown()
}

// waitEntry is one delayed key in the delaying layer.
type waitEntry struct {
	key     string
	readyAt time.Time
	index   int
}

type waitHeap []*waitEntry

func (h waitHeap) Len() int           { return len(h) }
func (h waitHeap) Less(i, j int) bool { return h[i].readyAt.Before(h[j].readyAt) }
func (h waitHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *waitHeap) Push(x any) { e := x.(*waitEntry); e.index = len(*h); *h = append(*h, e) }
func (h *waitHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return e
}

// coalescingDelayQueue combines a delaying layer (deadline heap) with a
// coalescing layer (ready FIFO + in-flight bookkeeping), mirroring the
// structure of client-go's delaying+FIFO queues in one mutex-protected type.
type coalescingDelayQueue struct {
	mu        sync.Mutex
	cond      *sync.Cond // wakes blocked Get calls
	dirty     map[string]struct{}
	inFlight  map[string]struct{}
	ready     []string
	waiting   waitHeap
	waitByKey map[string]*waitEntry
	shutdown  bool
	kick      chan struct{} // wakes the scheduler when deadlines change
}

func newCoalescingDelayQueue() *coalescingDelayQueue {
	q := &coalescingDelayQueue{
		dirty:     make(map[string]struct{}),
		inFlight:  make(map[string]struct{}),
		waitByKey: make(map[string]*waitEntry),
		kick:      make(chan struct{}, 1),
	}
	q.cond = sync.NewCond(&q.mu)
	q.waiting = waitHeap{}
	heap.Init(&q.waiting)
	go q.scheduleLoop()
	return q
}

// signalKick nudges the scheduler; a pending signal is sufficient because the
// scheduler always re-evaluates the earliest deadline after waking.
func (q *coalescingDelayQueue) signalKick() {
	select {
	case q.kick <- struct{}{}:
	default:
	}
}

// enqueueReady is the inner FIFO Add: callers must hold mu.
func (q *coalescingDelayQueue) enqueueReady(key string) {
	if _, ok := q.inFlight[key]; ok {
		q.dirty[key] = struct{}{} // requeue when Done() arrives
		return
	}
	if _, ok := q.dirty[key]; ok {
		return
	}
	q.dirty[key] = struct{}{}
	q.ready = append(q.ready, key)
	q.cond.Signal()
}

// Add enqueues key for immediate processing, superseding any pending delay.
// Duplicates while the key is already pending or being processed coalesce.
func (q *coalescingDelayQueue) Add(key string) {
	q.mu.Lock()
	if e, ok := q.waitByKey[key]; ok {
		heap.Remove(&q.waiting, e.index)
		delete(q.waitByKey, key)
	}
	q.enqueueReady(key)
	q.mu.Unlock()
}

// AddAfter schedules the key to be added after d. It only touches the
// delaying layer, so a retry scheduled for an in-flight key is not promoted
// until its deadline (Done() alone must not short-circuit the backoff).
func (q *coalescingDelayQueue) AddAfter(key string, d time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.dirty[key]; ok {
		return // already queued for immediate processing
	}
	readyAt := time.Now().Add(d)
	if e, ok := q.waitByKey[key]; ok {
		// Keep the earliest of competing deadlines.
		if readyAt.Before(e.readyAt) {
			e.readyAt = readyAt
			heap.Fix(&q.waiting, e.index)
			q.signalKick()
		}
		return
	}
	e := &waitEntry{key: key, readyAt: readyAt}
	heap.Push(&q.waiting, e)
	q.waitByKey[key] = e
	q.signalKick()
}

// Get blocks until a key is ready. After Shutdown it still hands out the
// remaining ready keys (for draining); once empty it returns shutdown=true.
func (q *coalescingDelayQueue) Get() (key string, shutdown bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.ready) == 0 {
		if q.shutdown {
			return "", true
		}
		q.cond.Wait()
	}
	key = q.ready[0]
	q.ready = q.ready[1:]
	delete(q.dirty, key)
	q.inFlight[key] = struct{}{}
	return key, false
}

// Done marks a previously Get'ed key finished; a fresh event observed while
// processing (marked dirty) causes one immediate requeue.
func (q *coalescingDelayQueue) Done(key string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.inFlight, key)
	if _, ok := q.dirty[key]; !ok {
		return
	}
	delete(q.dirty, key)
	q.ready = append(q.ready, key)
	q.dirty[key] = struct{}{}
	q.cond.Signal()
}

// Len reports all outstanding keys: ready, delayed and in-flight.
func (q *coalescingDelayQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.ready) + len(q.waitByKey) + len(q.inFlight)
}

func (q *coalescingDelayQueue) ShuttingDown() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.shutdown
}

// Shutdown fast-forwards every delayed entry to ready so accepted events are
// processed during the drain; once everything drains, Get reports shutdown.
func (q *coalescingDelayQueue) Shutdown() {
	q.mu.Lock()
	if q.shutdown {
		q.mu.Unlock()
		return
	}
	q.shutdown = true
	for q.waiting.Len() > 0 {
		e := heap.Pop(&q.waiting).(*waitEntry)
		delete(q.waitByKey, e.key)
		q.enqueueReady(e.key)
	}
	q.cond.Broadcast()
	q.mu.Unlock()
	q.signalKick()
}

// promoteDue moves every due delayed entry into the ready FIFO.
func (q *coalescingDelayQueue) promoteDue() {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := time.Now()
	for q.waiting.Len() > 0 && !q.waiting[0].readyAt.After(now) {
		e := heap.Pop(&q.waiting).(*waitEntry)
		delete(q.waitByKey, e.key)
		q.enqueueReady(e.key)
	}
}

// scheduleLoop wakes at the earliest delayed deadline (or immediately when
// kicked); on shutdown it exits after Shutdown fast-forwards the heap.
func (q *coalescingDelayQueue) scheduleLoop() {
	for {
		q.mu.Lock()
		if q.shutdown {
			q.mu.Unlock()
			return
		}
		if q.waiting.Len() == 0 {
			q.mu.Unlock()
			<-q.kick // wait for AddAfter or Shutdown
			continue
		}
		d := time.Until(q.waiting[0].readyAt)
		q.mu.Unlock()

		if d <= 0 {
			q.promoteDue()
			continue
		}
		t := time.NewTimer(d)
		select {
		case <-t.C:
			q.promoteDue()
		case <-q.kick:
			// Earliest deadline may have changed; re-evaluate.
		}
		t.Stop()
	}
}
