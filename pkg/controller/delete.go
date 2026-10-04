package controller

import (
	"github.com/highcumontoa/k8s-resource-controller-go/pkg/storage"
)

// DeleteWithVersion deletes one object conditionally on the caller's
// read-time resource version. A rejection (not-found / already-deleting /
// stale-version) is reported in the result and leaves no trace. When the
// delete is accepted and the object enters the finalizer-guarded
// terminating state, a delete event is submitted so the reconcile loop
// converges it to the final purge; a finalizer-less object is already
// purged and needs no event.
func (c *Controller) DeleteWithVersion(name string, expectedRV int64) storage.DeleteResult {
	results := c.DeleteBatch([]storage.DeleteIntent{{Name: name, ResourceVersion: expectedRV}})
	return results[0]
}

// DeleteBatch executes a group of version-conditional deletes through the
// store and submits a delete event for every object that entered the
// terminating state, so accepted deletions converge via the reconcile loop
// even while earlier events for the same objects are still queued. The
// per-object results are returned sorted by name; rejected intents produce
// no events and no state changes.
func (c *Controller) DeleteBatch(intents []storage.DeleteIntent) []storage.DeleteResult {
	results := c.store.DeleteBatch(intents)
	for _, r := range results {
		switch r.Outcome {
		case storage.DeleteOutcomeTerminating:
			c.log.Printf("name=%s rv=%d decision=delete-accepted outcome=%s",
				r.Name, r.ResourceVersion, r.Outcome)
			c.Submit(Event{Kind: EventDelete, Name: r.Name})
		case storage.DeleteOutcomePurged:
			c.log.Printf("name=%s decision=delete-accepted outcome=%s", r.Name, r.Outcome)
		default:
			c.log.Printf("name=%s decision=delete-rejected outcome=%s reason=%q",
				r.Name, r.Outcome, r.Reason)
		}
	}
	return results
}

// CheckDeleteBatch runs the read-only delete precheck for a group of
// intents; see storage.Store.CheckDeleteBatch. The verdicts are logged so a
// caller can audit why each object would or would not be deletable.
func (c *Controller) CheckDeleteBatch(intents []storage.DeleteIntent) []storage.DeleteResult {
	results := c.store.CheckDeleteBatch(intents)
	for _, r := range results {
		c.log.Printf("precheck name=%s outcome=%s reason=%v observed-rv=%d",
			r.Name, r.Outcome, r.Reason, r.ResourceVersion)
	}
	return results
}
