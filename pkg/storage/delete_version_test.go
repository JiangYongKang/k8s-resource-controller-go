package storage

import (
	"errors"
	"sync"
	"testing"

	"github.com/highcumontoa/k8s-resource-controller-go/pkg/apis/v1"
)

// TestDeleteWithVersionAccepted covers the accepted path: an object with a
// finalizer enters the readable terminating state and its version advances
// exactly one step; an object without finalizers is purged at once.
func TestDeleteWithVersionAccepted(t *testing.T) {
	s := NewStore()

	w, _ := s.Create(mkWidget("guarded", "v0"))
	w, err := s.Update("guarded", w.Metadata.ResourceVersion, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Metadata.Finalizers = []string{v1.FinalizerAttached}
		return cur, nil
	})
	if err != nil {
		t.Fatalf("add finalizer: %v", err)
	}
	rv := w.Metadata.ResourceVersion

	d, err := s.DeleteWithVersion("guarded", rv)
	if err != nil {
		t.Fatalf("version-conditional delete: %v", err)
	}
	t.Logf("event: delete accepted name=guarded oldrv=%d newrv=%d deletionTimestamp=%q",
		rv, d.Metadata.ResourceVersion, d.Metadata.DeletionTimestamp)
	if d.Metadata.ResourceVersion != rv+1 {
		t.Fatalf("accepted delete must advance version by one: old=%d new=%d", rv, d.Metadata.ResourceVersion)
	}
	got, err := s.Get("guarded")
	if err != nil {
		t.Fatalf("terminating object must stay readable: %v", err)
	}
	if got.AggregatedPhase() != v1.WidgetStatusTerminating {
		t.Fatalf("aggregated phase: want Terminating, got %s", got.AggregatedPhase())
	}
	t.Logf("judgement: object still readable, aggregated phase=%s", got.AggregatedPhase())

	// No finalizers: immediate purge at the current version.
	p, _ := s.Create(mkWidget("plain", "v0"))
	prv := p.Metadata.ResourceVersion
	out, err := s.DeleteWithVersion("plain", prv)
	if err != nil {
		t.Fatalf("delete finalizer-less object: %v", err)
	}
	if out != nil {
		t.Fatalf("finalizer-less object must purge at once, got %+v", out)
	}
	if _, err := s.Get("plain"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("purged object: want ErrNotFound, got %v", err)
	}
	t.Logf("judgement: finalizer-less delete at rv=%d purged immediately, get reason=%v", prv, ErrNotFound)
}

// TestDeleteWithVersionRejectionsLeaveNoTrace pins the three distinguishable
// rejections and proves a rejected delete mutates nothing: no deletion
// timestamp, no version consumption, object still fully usable afterwards.
func TestDeleteWithVersionRejectionsLeaveNoTrace(t *testing.T) {
	s := NewStore()

	// 1) Missing object -> ErrNotFound.
	if _, err := s.DeleteWithVersion("ghost", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing object: want ErrNotFound, got %v", err)
	}
	t.Logf("judgement: delete of missing object rejected reason=%v", ErrNotFound)

	w, _ := s.Create(mkWidget("w1", "v0"))
	rv := w.Metadata.ResourceVersion

	// 2) Stale version -> ErrVersionConflict, and no trace is left behind.
	if _, err := s.DeleteWithVersion("w1", rv+100); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("stale version: want ErrVersionConflict, got %v", err)
	}
	got, _ := s.Get("w1")
	t.Logf("event: stale delete rv=%d rejected reason=%v; surviving rv=%d deletionTimestamp=%q (must be unchanged/empty)",
		rv+100, ErrVersionConflict, got.Metadata.ResourceVersion, got.Metadata.DeletionTimestamp)
	if got.Metadata.ResourceVersion != rv {
		t.Fatalf("rejected delete consumed a version: rv=%d want %d", got.Metadata.ResourceVersion, rv)
	}
	if got.Metadata.DeletionTimestamp != "" {
		t.Fatalf("rejected delete set deletion timestamp: %q", got.Metadata.DeletionTimestamp)
	}

	// The object is still fully usable: an ordinary update succeeds at the
	// old version, proving the rejected delete neither marked nor bumped it.
	w, err := s.Update("w1", rv, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Spec.Message = "v1"
		return cur, nil
	})
	if err != nil {
		t.Fatalf("update after rejected delete: %v", err)
	}
	t.Logf("judgement: object still writable after rejection, rv=%d message=%q",
		w.Metadata.ResourceVersion, w.Spec.Message)

	// 3) Already terminating -> ErrDeleting, distinct from the two above.
	w, err = s.Update("w1", w.Metadata.ResourceVersion, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Metadata.Finalizers = []string{v1.FinalizerAttached}
		return cur, nil
	})
	if err != nil {
		t.Fatalf("add finalizer: %v", err)
	}
	d, err := s.DeleteWithVersion("w1", w.Metadata.ResourceVersion)
	if err != nil {
		t.Fatalf("first delete: %v", err)
	}
	if _, err := s.DeleteWithVersion("w1", d.Metadata.ResourceVersion); !errors.Is(err, ErrDeleting) {
		t.Fatalf("second delete: want ErrDeleting, got %v", err)
	}
	got, _ = s.Get("w1")
	t.Logf("judgement: repeat delete rejected reason=%v; rv stays %d (no extra version consumed)",
		ErrDeleting, got.Metadata.ResourceVersion)
	if got.Metadata.ResourceVersion != d.Metadata.ResourceVersion {
		t.Fatalf("repeat delete consumed a version: rv=%d want %d",
			got.Metadata.ResourceVersion, d.Metadata.ResourceVersion)
	}

	// A stale-version delete against the terminating object still reports
	// ErrDeleting (deleting check precedes the version check), keeping the
	// "already in deletion flow" signal unambiguous.
	if _, err := s.DeleteWithVersion("w1", d.Metadata.ResourceVersion-1); !errors.Is(err, ErrDeleting) {
		t.Fatalf("stale delete of terminating object: want ErrDeleting, got %v", err)
	}
	t.Logf("judgement: terminating object reports %v even for stale versions", ErrDeleting)
}

// TestDeleteWithVersionStaleAfterConcurrentWrite simulates the classic
// read-modify-delete race: the caller deletes with the version it read, but
// another writer commits first, so the delete must be refused.
func TestDeleteWithVersionStaleAfterConcurrentWrite(t *testing.T) {
	s := NewStore()
	w, _ := s.Create(mkWidget("w1", "v0"))
	readRV := w.Metadata.ResourceVersion
	t.Logf("event: caller reads name=w1 at rv=%d", readRV)

	// A concurrent writer commits between the read and the delete.
	w, err := s.Update("w1", readRV, func(cur *v1.Widget) (*v1.Widget, error) {
		cur.Spec.Message = "concurrent-write"
		return cur, nil
	})
	if err != nil {
		t.Fatalf("concurrent update: %v", err)
	}
	t.Logf("event: concurrent writer advanced rv=%d -> %d", readRV, w.Metadata.ResourceVersion)

	_, err = s.DeleteWithVersion("w1", readRV)
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("delete with read-time version: want ErrVersionConflict, got %v", err)
	}
	got, _ := s.Get("w1")
	t.Logf("judgement: delete rejected reason=%v; object survives rv=%d message=%q deletionTimestamp=%q",
		err, got.Metadata.ResourceVersion, got.Spec.Message, got.Metadata.DeletionTimestamp)
	if got.Spec.Message != "concurrent-write" || got.Metadata.DeletionTimestamp != "" {
		t.Fatal("rejected delete left a trace or clobbered the concurrent write")
	}
}

// TestConcurrentDeletesOnlyOneWins hands the same read-time version to many
// goroutines deleting one object: at most one delete is accepted, every
// loser gets a distinguishable rejection (stale version or already
// deleting), and the version advances exactly once.
func TestConcurrentDeletesOnlyOneWins(t *testing.T) {
	for _, withFinalizer := range []bool{true, false} {
		name := "guarded"
		if !withFinalizer {
			name = "plain"
		}
		t.Run(name, func(t *testing.T) {
			s := NewStore()
			w, _ := s.Create(mkWidget("w1", "init"))
			if withFinalizer {
				var err error
				w, err = s.Update("w1", w.Metadata.ResourceVersion, func(cur *v1.Widget) (*v1.Widget, error) {
					cur.Metadata.Finalizers = []string{v1.FinalizerAttached}
					return cur, nil
				})
				if err != nil {
					t.Fatalf("add finalizer: %v", err)
				}
			}
			readRV := w.Metadata.ResourceVersion

			const n = 32
			var wg sync.WaitGroup
			var mu sync.Mutex
			wins, conflicts, deletings, notFounds, others := 0, 0, 0, 0, 0
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := s.DeleteWithVersion("w1", readRV)
					mu.Lock()
					defer mu.Unlock()
					switch {
					case err == nil:
						wins++
					case errors.Is(err, ErrVersionConflict):
						conflicts++
					case errors.Is(err, ErrDeleting):
						deletings++
					case errors.Is(err, ErrNotFound):
						// Finalizer-less objects are purged by the winner, so
						// late losers can legitimately observe "not found".
						notFounds++
					default:
						others++
						t.Errorf("unexpected delete result: %v", err)
					}
				}()
			}
			wg.Wait()
			t.Logf("event: %d concurrent deletes at read-rv=%d -> wins=%d version-conflicts=%d already-deleting=%d not-found=%d others=%d",
				n, readRV, wins, conflicts, deletings, notFounds, others)
			if wins != 1 || others != 0 || conflicts+deletings+notFounds != n-1 {
				t.Fatalf("judgement failed: wins=%d conflicts=%d deletings=%d notFounds=%d others=%d",
					wins, conflicts, deletings, notFounds, others)
			}
			if withFinalizer && notFounds != 0 {
				t.Fatalf("terminating object must stay readable, yet %d losers saw not-found", notFounds)
			}

			got, err := s.Get("w1")
			if withFinalizer {
				// Terminating: exactly one version step beyond the read version.
				if err != nil {
					t.Fatalf("terminating object must stay readable: %v", err)
				}
				t.Logf("judgement: final rv=%d (read-rv=%d, exactly one step), phase=%s",
					got.Metadata.ResourceVersion, readRV, got.AggregatedPhase())
				if got.Metadata.ResourceVersion != readRV+1 {
					t.Fatalf("version advanced more than once: rv=%d read-rv=%d",
						got.Metadata.ResourceVersion, readRV)
				}
			} else {
				// Purged: gone, and a recreate proves the losers consumed nothing.
				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("purged object: want ErrNotFound, got %v", err)
				}
				nw, cerr := s.Create(mkWidget("w1", "recreated"))
				if cerr != nil {
					t.Fatalf("recreate after purge: %v", cerr)
				}
				t.Logf("judgement: object purged; recreated at rv=%d (read-rv=%d, exactly one delete step in between)",
					nw.Metadata.ResourceVersion, readRV)
				if nw.Metadata.ResourceVersion != readRV+2 {
					t.Fatalf("unexpected version allocation: recreated rv=%d read-rv=%d",
						nw.Metadata.ResourceVersion, readRV)
				}
			}
		})
	}
}
