package controller

import (
	"errors"
	"fmt"
	"sync"
)

// ErrExternalNotFound is returned when an external attachment is absent.
var ErrExternalNotFound = errors.New("external attachment not found")

// externalSystem models the outside world the controller reconciles against.
// Its attachment table is the place where "side effects" become observable;
// every mutating operation is guarded so retried reconciliation stays
// idempotent: a re-encountered desired state never produces a second call.
type externalSystem struct {
	mu       sync.Mutex
	attached map[string]string // key -> desired message
	opCount  map[string]int    // real mutating calls observed per key
	failAdd  map[string]int    // remaining transient EnsureAttach failures
}

func newExternalSystem() *externalSystem {
	return &externalSystem{
		attached: make(map[string]string),
		opCount:  make(map[string]int),
		failAdd:  make(map[string]int),
	}
}

// ArmFailures pre-arms n transient failures for the next n EnsureAttach calls
// on key that do not find an existing attachment.
func (e *externalSystem) ArmFailures(key string, n int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.failAdd[key] += n
}

// EnsureAttach creates the attachment if missing; an existing attachment is
// left untouched (no duplicated side effect, no op counted). A pre-armed
// transient failure consumes one failure slot and changes nothing.
func (e *externalSystem) EnsureAttach(key, message string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.attached[key]; ok {
		return nil
	}
	if e.failAdd[key] > 0 {
		e.failAdd[key]--
		return fmt.Errorf("external EnsureAttach transient failure for %q", key)
	}
	e.attached[key] = message
	e.opCount[key]++
	return nil
}

// Detach removes the attachment; absence is not an error (idempotent), and
// only a real removal counts as an operation.
func (e *externalSystem) Detach(key string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.attached[key]; !ok {
		return nil
	}
	delete(e.attached, key)
	e.opCount[key]++
	return nil
}

// Attached reports whether the attachment exists.
func (e *externalSystem) Attached(key string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.attached[key]
	return ok
}

// OpCount returns how many real mutating calls were made for the key. This is
// the test-observable proof that retried events caused no duplicate effects.
func (e *externalSystem) OpCount(key string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.opCount[key]
}
