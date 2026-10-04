package storage

import (
	"sort"
	"time"

	"github.com/highcumontoa/k8s-resource-controller-go/pkg/apis/v1"
)

// DeleteIntent is one caller-supplied deletion intention: the object name plus
// the resource version the caller observed when it decided to delete.
type DeleteIntent struct {
	Name       string
	ExpectedRV int64
}

// DeleteCheckResult is the read-only precheck verdict for one intent.
type DeleteCheckResult struct {
	Name    string
	Allowed bool
	Reason  error // nil when Allowed; otherwise one of the Err* sentinels
}

// DeleteResult is the execution outcome for one intent in a batch delete.
type DeleteResult struct {
	Name     string
	Accepted bool
	Purged   bool  // object physically gone (no finalizers) after acceptance
	Reason   error // nil when Accepted; otherwise one of the Err* sentinels
}

// DeleteWithVersion deletes name only when expectedRV matches the current
// resource version. Rejections are checked before any mutation and are
// distinguishable via errors.Is:
//   - ErrNotFound:        no object carries the name;
//   - ErrVersionConflict: a concurrent write (including a winning concurrent
//     delete) moved the version past the caller's read;
//   - ErrDeleting:        the caller's version IS current, but the object is
//     already terminating.
//
// A rejected delete leaves no trace: no deletion timestamp is set and no new
// resource version is allocated. An accepted delete keeps the semantics of
// Delete: finalizer-bearing objects become terminating (still readable),
// finalizer-less objects are purged, and the version advances by one.
func (s *Store) DeleteWithVersion(name string, expectedRV int64) (*v1.Widget, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cur, ok := s.items[name]
	if reason := checkDeleteLocked(cur, ok, expectedRV); reason != nil {
		return nil, reason
	}
	return s.applyDeleteLocked(cur), nil
}

// applyDeleteLocked performs the accepted-delete mutation shared by Delete
// and DeleteWithVersion. Callers must hold mu and must have already decided
// the delete is allowed. Returns the terminating object, or nil when the
// object had no finalizers and was purged immediately.
func (s *Store) applyDeleteLocked(cur *v1.Widget) *v1.Widget {
	name := cur.Metadata.Name
	cur.Metadata.DeletionTimestamp = time.Now().UTC().Format(time.RFC3339Nano)
	cur.Metadata.ResourceVersion = s.nextRV()

	if len(cur.Metadata.Finalizers) == 0 {
		delete(s.items, name)
		return nil
	}
	return cur.DeepCopy()
}

// checkDeleteLocked is the single read-only predicate behind both the
// precheck and the batch execution, so a precheck verdict and the execution
// of the same intent can only diverge when a concurrent write lands in
// between. Callers must hold mu (read or write). Returns nil when the intent
// would be accepted right now. The version is judged before the terminating
// state so that a caller holding a stale read always hears "your version is
// stale" (ErrVersionConflict), while ErrDeleting is reserved for the case
// where the caller's version is current but a delete is already in flight.
func checkDeleteLocked(cur *v1.Widget, ok bool, expectedRV int64) error {
	if !ok {
		return ErrNotFound
	}
	if cur.Metadata.ResourceVersion != expectedRV {
		return ErrVersionConflict
	}
	if cur.Metadata.DeletionTimestamp != "" {
		return ErrDeleting
	}
	return nil
}

// PrecheckDelete evaluates every intent read-only and returns one verdict per
// intent, sorted by name (stable for repeated execution of the same input).
// It never mutates stored state and never allocates a resource version.
func (s *Store) PrecheckDelete(intents []DeleteIntent) []DeleteCheckResult {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]DeleteCheckResult, 0, len(intents))
	for _, in := range intents {
		cur, ok := s.items[in.Name]
		reason := checkDeleteLocked(cur, ok, in.ExpectedRV)
		out = append(out, DeleteCheckResult{
			Name:    in.Name,
			Allowed: reason == nil,
			Reason:  reason,
		})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// BatchDelete executes every intent independently and returns one result per
// intent, sorted by name. Each object is judged and mutated under its own
// critical section, so a concurrent change to one object can never affect the
// verdict of another object in the same batch. Every intent produces an
// explicit result — the batch never silently skips an entry and never
// collapses failures into one vague error. Re-executing the same batch is
// safe: rejected deletes (already terminating / already gone) mutate nothing
// and allocate no version.
func (s *Store) BatchDelete(intents []DeleteIntent) []DeleteResult {
	out := make([]DeleteResult, 0, len(intents))
	for _, in := range intents {
		out = append(out, s.deleteOne(in))
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// deleteOne judges and executes a single intent atomically.
func (s *Store) deleteOne(in DeleteIntent) DeleteResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	cur, ok := s.items[in.Name]
	if reason := checkDeleteLocked(cur, ok, in.ExpectedRV); reason != nil {
		return DeleteResult{Name: in.Name, Reason: reason}
	}
	terminating := s.applyDeleteLocked(cur)
	return DeleteResult{
		Name:     in.Name,
		Accepted: true,
		Purged:   terminating == nil,
	}
}
