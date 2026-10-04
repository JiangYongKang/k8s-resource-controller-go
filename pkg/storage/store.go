// Package storage is the in-process, concurrency-safe resource store with
// optimistic concurrency control and finalizer-aware deletion semantics.
package storage

import (
	"errors"
	"sort"
	"sync"

	"github.com/highcumontoa/k8s-resource-controller-go/pkg/apis/v1"
)

// Distinguishable rejection reasons returned by the store. Callers can tell
// them apart with errors.Is; no rejected write mutates any stored state.
var (
	// ErrNotFound is returned when no object carries the given name.
	ErrNotFound = errors.New("object not found")
	// ErrAlreadyExists is returned when Create targets a live name.
	ErrAlreadyExists = errors.New("object already exists")
	// ErrVersionConflict is returned when the supplied resource version is stale.
	ErrVersionConflict = errors.New("resource version conflict (stale)")
	// ErrDeleting is returned when a mutating write targets a deleting object.
	ErrDeleting = errors.New("object is being deleted")
	// ErrFinalizerNotFound is returned when a removal names an absent finalizer.
	ErrFinalizerNotFound = errors.New("finalizer not present")
)

// Store is a mutex-protected map of widgets acting as the local "etcd".
// Every accepted mutation advances a monotonic resource-version counter.
type Store struct {
	mu      sync.RWMutex
	items   map[string]*v1.Widget
	rvAlloc int64
}

// NewStore creates an empty store.
func NewStore() *Store {
	return &Store{items: make(map[string]*v1.Widget)}
}

// nextRV must be called with mu held.
func (s *Store) nextRV() int64 {
	s.rvAlloc++
	return s.rvAlloc
}

// Create inserts a new widget. A name already in use is rejected distinctly:
// ErrDeleting if the existing object is terminating, ErrAlreadyExists else.
func (s *Store) Create(w *v1.Widget) (*v1.Widget, error) {
	if w == nil {
		return nil, errors.New("cannot create nil widget")
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if cur, ok := s.items[w.Metadata.Name]; ok {
		if cur.Metadata.DeletionTimestamp != "" {
			return nil, ErrDeleting
		}
		return nil, ErrAlreadyExists
	}
	stored := w.DeepCopy()
	stored.Metadata.ResourceVersion = s.nextRV()
	if stored.Status.Phase == "" {
		stored.Status.Phase = v1.WidgetStatusProgressing
	}
	s.items[stored.Metadata.Name] = stored
	return stored.DeepCopy(), nil
}

// Get returns a deep copy of the named widget.
func (s *Store) Get(name string) (*v1.Widget, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cur, ok := s.items[name]
	if !ok {
		return nil, ErrNotFound
	}
	return cur.DeepCopy(), nil
}

// Update replaces the object via mutate when expectedRV matches the current
// resource version. The object handed to mutate is a private copy, so a
// returning error or panic leaves stored state untouched. Rejection order:
// not-found, deleting, stale-version.
func (s *Store) Update(name string, expectedRV int64, mutate func(cur *v1.Widget) (*v1.Widget, error)) (out *v1.Widget, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cur, ok := s.items[name]
	if !ok {
		return nil, ErrNotFound
	}
	if cur.Metadata.DeletionTimestamp != "" {
		return nil, ErrDeleting
	}
	if cur.Metadata.ResourceVersion != expectedRV {
		return nil, ErrVersionConflict
	}

	next, merr := mutate(cur.DeepCopy())
	if merr != nil {
		return nil, merr
	}
	if next == nil {
		return nil, errors.New("mutate returned nil widget")
	}
	// Identity and bookkeeping stay under the store's control.
	next.Metadata.Name = name
	next.Metadata.DeletionTimestamp = ""
	next.Metadata.ResourceVersion = s.nextRV()
	// A status-bearing write observes the version it itself produces, so a
	// later reconciliation can detect "already in sync at current version".
	if next.Status.Phase != "" {
		next.Status.ObservedRV = next.Metadata.ResourceVersion
	}
	s.items[name] = next
	return next.DeepCopy(), nil
}

// Delete marks the object for deletion by setting DeletionTimestamp. An
// object without finalizers is purged immediately. Deleting a missing object
// is ErrNotFound; deleting an already terminating object is ErrDeleting.
// The accepted-delete mutation itself lives in applyDeleteLocked so that
// DeleteWithVersion shares exactly the same semantics.
func (s *Store) Delete(name string) (*v1.Widget, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cur, ok := s.items[name]
	if !ok {
		return nil, ErrNotFound
	}
	if cur.Metadata.DeletionTimestamp != "" {
		return nil, ErrDeleting
	}
	return s.applyDeleteLocked(cur), nil
}

// RemoveFinalizer drops a finalizer when expectedRV is current. When the last
// finalizer of a terminating object is removed the object is purged and the
// returned widget is nil. Missing objects, stale versions and absent
// finalizers are each rejected with a distinct error.
func (s *Store) RemoveFinalizer(name string, expectedRV int64, finalizer string) (*v1.Widget, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cur, ok := s.items[name]
	if !ok {
		return nil, ErrNotFound
	}
	if cur.Metadata.ResourceVersion != expectedRV {
		return nil, ErrVersionConflict
	}
	idx := -1
	for i, f := range cur.Metadata.Finalizers {
		if f == finalizer {
			idx = i
			break
		}
	}
	if idx < 0 {
		return nil, ErrFinalizerNotFound
	}
	cur.Metadata.Finalizers = append(cur.Metadata.Finalizers[:idx], cur.Metadata.Finalizers[idx+1:]...)
	cur.Metadata.ResourceVersion = s.nextRV()

	if cur.Metadata.DeletionTimestamp != "" && len(cur.Metadata.Finalizers) == 0 {
		delete(s.items, name)
		return nil, nil
	}
	return cur.DeepCopy(), nil
}

// List returns deep copies of all stored widgets in name order.
func (s *Store) List() []*v1.Widget {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*v1.Widget, 0, len(s.items))
	for _, w := range s.items {
		out = append(out, w.DeepCopy())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Metadata.Name < out[j].Metadata.Name })
	return out
}
