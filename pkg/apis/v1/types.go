// Package v1 defines the in-memory Kubernetes-style resource types.
package v1

const (
	// WidgetStatusReady means the object and its external resource are in sync.
	WidgetStatusReady = "Ready"
	// WidgetStatusProgressing means reconciliation has not converged yet.
	WidgetStatusProgressing = "Progressing"
	// WidgetStatusTerminating means the object is blocked on finalizer removal.
	WidgetStatusTerminating = "Terminating"
	// WidgetStatusDegraded means the last reconciliation attempt failed.
	WidgetStatusDegraded = "Degraded"

	// FinalizerAttached is the only finalizer managed by this controller.
	FinalizerAttached = "widgets.example.com/attached"
)

// ObjectMeta carries the identity, versioning, finalizer and deletion metadata.
type ObjectMeta struct {
	Name              string
	ResourceVersion   int64
	Finalizers        []string
	DeletionTimestamp string
}

// WidgetSpec is the user-desired state.
type WidgetSpec struct {
	Message string
}

// Condition describes one aspect of the reconciled status.
type Condition struct {
	Type    string
	Status  string
	Reason  string
	Message string
}

// WidgetStatus is the observed state aggregated by the reconciler.
type WidgetStatus struct {
	Phase      string
	ObservedRV int64
	Conditions []Condition
}

// Widget is a single stored resource.
type Widget struct {
	Metadata ObjectMeta
	Spec     WidgetSpec
	Status   WidgetStatus
}

// AggregatedPhase returns the externally meaningful phase: once a deletion is
// requested the object reports Terminating regardless of its last reconciled
// status, and cannot be "restored" to Ready by a normal status update (the
// store rejects mutating writes against deleting objects).
func (w *Widget) AggregatedPhase() string {
	if w.Metadata.DeletionTimestamp != "" {
		return WidgetStatusTerminating
	}
	return w.Status.Phase
}

// DeepCopy returns an independent copy of the widget.
func (w *Widget) DeepCopy() *Widget {
	if w == nil {
		return nil
	}
	out := *w
	out.Metadata.Finalizers = append([]string(nil), w.Metadata.Finalizers...)
	out.Status.Conditions = make([]Condition, len(w.Status.Conditions))
	copy(out.Status.Conditions, w.Status.Conditions)
	return &out
}
