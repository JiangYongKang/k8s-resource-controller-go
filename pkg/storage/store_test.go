package storage

import (
	"errors"
	"sync"
	"testing"

	"github.com/highcumontoa/k8s-resource-controller-go/pkg/apis/v1"
)

func mkWidget(name, msg string) *v1.Widget {
	return &v1.Widget{
		Metadata: v1.ObjectMeta{Name: name},
		Spec:     v1.WidgetSpec{Message: msg},
	}
}

func TestCreateGetVersioning(t *testing.T) {
	s := NewStore()
	w, err := s.Create(mkWidget("w1", "hello"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Logf("created name=w1 version=%d (version must be >0)", w.Metadata.ResourceVersion)
	if w.Metadata.ResourceVersion == 0 {
		t.Fatal("resource version not allocated")
	}
	if _, err := s.Create(mkWidget("w1", "again")); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate create: want ErrAlreadyExists, got %v", err)
	} else {
		t.Logf("judgement: duplicate create rejected with reason=%v (distinguishable)", err)
	}
}

func TestUpdateOptimisticConcurrency(t *testing.T) {
	s := NewStore()
	w, _ := s.Create(mkWidget("w1", "v0"))
	rv := w.Metadata.ResourceVersion

	// A stale version must be rejected and leave state untouched.
	_, err := s.Update("w1", rv-1, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Spec.Message = "stale"
		return cur, nil
	})
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale update: want ErrVersionConflict, got %v", err)
	}
	got, _ := s.Get("w1")
	t.Logf("stale write version=%d rejected reason=%v, surviving message=%q (must stay %q)",
		rv-1, err, got.Spec.Message, "v0")
	if got.Spec.Message != "v0" {
		t.Fatal("rejected stale write mutated stored state")
	}

	updated, err := s.Update("w1", rv, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Spec.Message = "v1"
		return cur, nil
	})
	if err != nil {
		t.Fatalf("current-version update: %v", err)
	}
	t.Logf("accepted update oldrv=%d newrv=%d message=%q", rv, updated.Metadata.ResourceVersion, updated.Spec.Message)
	if updated.Metadata.ResourceVersion == rv || updated.Spec.Message != "v1" {
		t.Fatal("accepted update did not advance state/version")
	}

	// Now the old version is stale.
	_, err = s.Update("w1", rv, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Spec.Message = "late"
		return cur, nil
	})
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("second use of old rv: want ErrVersionConflict, got %v", err)
	}
	t.Logf("judgement: reused rv=%d rejected with %v", rv, err)
}

func TestUpdateMissingAndMutatorError(t *testing.T) {
	s := NewStore()
	if _, err := s.Update("ghost", 1, nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing update: want ErrNotFound, got %v", err)
	}
	w, _ := s.Create(mkWidget("w1", "v0"))
	boom := errors.New("mutator boom")
	_, err := s.Update("w1", w.Metadata.ResourceVersion, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Spec.Message = "should-not-persist"
		return nil, boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("mutator error: want %v, got %v", boom, err)
	}
	got, _ := s.Get("w1")
	if got.Spec.Message != "v0" {
		t.Fatalf("failed mutator changed state: %q", got.Spec.Message)
	}
	t.Logf("judgement: failing mutator rejected with %v and state stayed %q", boom, got.Spec.Message)
}

func TestFinalizerDeletionLifecycle(t *testing.T) {
	s := NewStore()
	w, _ := s.Create(mkWidget("w1", "v0"))
	rv := w.Metadata.ResourceVersion

	// Attach a finalizer through an ordinary update.
	w, err := s.Update("w1", rv, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Metadata.Finalizers = []string{v1.FinalizerAttached}
		return cur, nil
	})
	if err != nil {
		t.Fatalf("add finalizer: %v", err)
	}
	rvWithFinalizer := w.Metadata.ResourceVersion
	t.Logf("finalizer attached version=%d finalizers=%v", rvWithFinalizer, w.Metadata.Finalizers)

	// Delete marks terminating; object remains readable.
	d, err := s.Delete("w1")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	t.Logf("delete marked terminating version=%d deletionTimestamp=%q finalizers=%d",
		d.Metadata.ResourceVersion, d.Metadata.DeletionTimestamp, len(d.Metadata.Finalizers))
	if d.Metadata.DeletionTimestamp == "" {
		t.Fatal("deletion timestamp not set")
	}
	if got, err := s.Get("w1"); err != nil {
		t.Fatalf("terminating object must stay readable: %v", err)
	} else if got.AggregatedPhase() != v1.WidgetStatusTerminating {
		t.Fatalf("aggregated phase: want Terminating, got %s", got.AggregatedPhase())
	}

	// Ordinary updates against a terminating object are rejected distinctly
	// and cannot restore it to a normal phase.
	_, err = s.Update("w1", d.Metadata.ResourceVersion, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Status.Phase = v1.WidgetStatusReady
		return cur, nil
	})
	if !errors.Is(err, ErrDeleting) {
		t.Fatalf("update deleting object: want ErrDeleting, got %v", err)
	}
	t.Logf("judgement: update while terminating rejected reason=%v (object cannot be restored)", err)

	// Repeated delete is rejected, distinct from not-found.
	if _, err := s.Delete("w1"); !errors.Is(err, ErrDeleting) {
		t.Fatalf("repeat delete: want ErrDeleting, got %v", err)
	}
	t.Logf("judgement: repeat delete rejected reason=%v", err)

	// A stale finalizer removal is a version conflict.
	if _, err := s.RemoveFinalizer("w1", d.Metadata.ResourceVersion-1, v1.FinalizerAttached); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale finalizer removal: want ErrVersionConflict, got %v", err)
	}
	// An unknown finalizer is a distinct rejection.
	if _, err := s.RemoveFinalizer("w1", d.Metadata.ResourceVersion, "other/finalizer"); !errors.Is(err, ErrFinalizerNotFound) {
		t.Fatalf("missing finalizer: want ErrFinalizerNotFound, got %v", err)
	} else {
		t.Logf("judgement: absent finalizer rejected reason=%v", err)
	}

	// Correct removal purges the object.
	gone, err := s.RemoveFinalizer("w1", d.Metadata.ResourceVersion, v1.FinalizerAttached)
	if err != nil {
		t.Fatalf("remove finalizer: %v", err)
	}
	if gone != nil {
		t.Fatalf("object should be purged with last finalizer, got %+v", gone)
	}
	if _, err := s.Get("w1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("purged object: want ErrNotFound, got %v", err)
	} else {
		t.Logf("judgement: last finalizer removed -> object purged, get reason=%v", err)
	}

	// Deleting a missing object is not-found (distinct from terminating).
	if _, err := s.Delete("w1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delete missing: want ErrNotFound, got %v", err)
	}
}

func TestDeleteWithoutFinalizersPurgesImmediately(t *testing.T) {
	s := NewStore()
	w, _ := s.Create(mkWidget("w1", "v0"))
	t.Logf("created version=%d with no finalizers", w.Metadata.ResourceVersion)
	out, err := s.Delete("w1")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if out != nil {
		t.Fatalf("finalizer-less object must purge at once, got %+v", out)
	}
	if _, err := s.Get("w1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	} else {
		t.Logf("judgement: immediate purge verified, get reason=%v", err)
	}
}

// TestConcurrentWritesOnlyOneWins hammers one object version from many
// goroutines: exactly one writer may win; every loser sees the same
// distinguishable conflict reason and no state corruption occurs.
func TestConcurrentWritesOnlyOneWins(t *testing.T) {
	s := NewStore()
	w, _ := s.Create(mkWidget("w1", "init"))
	rv := w.Metadata.ResourceVersion

	const n = 32
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins, conflicts, others := 0, 0, 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := s.Update("w1", rv, func(cur *v1.Widget) (*v1.Widget, error) {
				cur.Spec.Message = "concurrent"
				return cur, nil
			})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				wins++
			case errors.Is(err, ErrVersionConflict):
				conflicts++
			default:
				others++
				t.Errorf("unexpected write result: %v", err)
			}
		}(i)
	}
	wg.Wait()
	t.Logf("event: %d concurrent writes against version=%d -> wins=%d conflicts=%d others=%d (judgement: wins must be exactly 1)",
		n, rv, wins, conflicts, others)
	if wins != 1 || conflicts != n-1 || others != 0 {
		t.Fatalf("unexpected outcome wins=%d conflicts=%d others=%d", wins, conflicts, others)
	}
	got, _ := s.Get("w1")
	if got.Spec.Message != "concurrent" || got.Metadata.ResourceVersion != rv+1 {
		t.Fatalf("state after concurrent writes: msg=%q rv=%d", got.Spec.Message, got.Metadata.ResourceVersion)
	}
}
