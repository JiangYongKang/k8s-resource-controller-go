package storage

import (
	"errors"
	"sort"
)

// DeleteIntent is one caller-supplied deletion request: the object name plus
// the resource version the caller observed when it decided to delete.
type DeleteIntent struct {
	Name            string
	ResourceVersion int64
}

// DeleteOutcome classifies the per-object verdict of a delete precheck or a
// batch delete execution. The outcomes are mutually exclusive and stable
// across repeated evaluations of unchanged state.
type DeleteOutcome string

const (
	// DeleteOutcomePurged means the object had no finalizers and was removed
	// immediately (execution only; a precheck reports DeleteOutcomeReady).
	DeleteOutcomePurged DeleteOutcome = "Purged"
	// DeleteOutcomeTerminating means the delete was accepted and the object
	// entered the finalizer-guarded deletion flow; it stays readable.
	DeleteOutcomeTerminating DeleteOutcome = "Terminating"
	// DeleteOutcomeReady is the precheck verdict "the delete would be
	// accepted right now".
	DeleteOutcomeReady DeleteOutcome = "Ready"
	// DeleteOutcomeNotFound means no object carries the name.
	DeleteOutcomeNotFound DeleteOutcome = "NotFound"
	// DeleteOutcomeAlreadyDeleting means the object is already terminating.
	DeleteOutcomeAlreadyDeleting DeleteOutcome = "AlreadyDeleting"
	// DeleteOutcomeVersionConflict means the supplied version is stale: the
	// object was modified (or deleted and recreated) since the read.
	DeleteOutcomeVersionConflict DeleteOutcome = "VersionConflict"
)

// DeleteResult is the per-object verdict of a precheck or an executed batch
// delete. Reason carries the distinguishing error for rejections
// (ErrNotFound / ErrDeleting / ErrVersionConflict) and is nil for accepted
// deletes. ResourceVersion reports, per case: the observed current version
// for prechecks and rejections against existing objects, and the version the
// object reached after an accepted delete (0 once purged).
type DeleteResult struct {
	Name            string
	Outcome         DeleteOutcome
	Reason          error
	ResourceVersion int64
}

// checkDeleteLocked evaluates one intent against the current state; the
// caller must hold at least the read lock.
func (s *Store) checkDeleteLocked(intent DeleteIntent) DeleteResult {
	cur, ok := s.items[intent.Name]
	if !ok {
		return DeleteResult{Name: intent.Name, Outcome: DeleteOutcomeNotFound, Reason: ErrNotFound}
	}
	r := DeleteResult{Name: intent.Name, ResourceVersion: cur.Metadata.ResourceVersion}
	switch {
	case cur.Metadata.DeletionTimestamp != "":
		r.Outcome = DeleteOutcomeAlreadyDeleting
		r.Reason = ErrDeleting
	case cur.Metadata.ResourceVersion != intent.ResourceVersion:
		r.Outcome = DeleteOutcomeVersionConflict
		r.Reason = ErrVersionConflict
	default:
		r.Outcome = DeleteOutcomeReady
	}
	return r
}

// CheckDelete performs a read-only precheck of one delete intent: it reports
// whether DeleteWithVersion would be accepted right now, without mutating
// any state or consuming a resource version.
func (s *Store) CheckDelete(intent DeleteIntent) DeleteResult {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.checkDeleteLocked(intent)
}

// CheckDeleteBatch prechecks a group of intents against one consistent
// snapshot and returns one verdict per intent, sorted by name. It is
// read-only; a later execution can still be rejected if a concurrent write
// lands in between.
func (s *Store) CheckDeleteBatch(intents []DeleteIntent) []DeleteResult {
	s.mu.RLock()
	defer s.mu.RUnlock()
	results := make([]DeleteResult, 0, len(intents))
	for _, in := range intents {
		results = append(results, s.checkDeleteLocked(in))
	}
	sort.SliceStable(results, func(i, j int) bool { return results[i].Name < results[j].Name })
	return results
}

// DeleteBatch executes a group of version-conditional deletes. Every intent
// is evaluated independently against the current state: one object's
// rejection never affects the others, and every intent yields an explicit
// result. Results are sorted by name, so re-submitting the same intents
// against unchanged state yields identical conclusions (and no repeated side
// effects: an already accepted delete shows up as AlreadyDeleting/NotFound
// and consumes no further version).
func (s *Store) DeleteBatch(intents []DeleteIntent) []DeleteResult {
	results := make([]DeleteResult, 0, len(intents))
	for _, in := range intents {
		results = append(results, s.executeOne(in))
	}
	sort.SliceStable(results, func(i, j int) bool { return results[i].Name < results[j].Name })
	return results
}

// executeOne runs a single version-conditional delete and translates the
// store outcome into a DeleteResult.
func (s *Store) executeOne(intent DeleteIntent) DeleteResult {
	w, err := s.DeleteWithVersion(intent.Name, intent.ResourceVersion)
	r := DeleteResult{Name: intent.Name, Reason: err}
	switch {
	case err == nil && w == nil:
		r.Outcome = DeleteOutcomePurged
	case err == nil:
		r.Outcome = DeleteOutcomeTerminating
		r.ResourceVersion = w.Metadata.ResourceVersion
	case errors.Is(err, ErrNotFound):
		r.Outcome = DeleteOutcomeNotFound
	case errors.Is(err, ErrDeleting):
		r.Outcome = DeleteOutcomeAlreadyDeleting
		if cur, gerr := s.Get(intent.Name); gerr == nil {
			r.ResourceVersion = cur.Metadata.ResourceVersion
		}
	case errors.Is(err, ErrVersionConflict):
		r.Outcome = DeleteOutcomeVersionConflict
		if cur, gerr := s.Get(intent.Name); gerr == nil {
			r.ResourceVersion = cur.Metadata.ResourceVersion
		}
	}
	return r
}
