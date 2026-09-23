package controller

import "time"

// Phase 是对象生命周期阶段。
type Phase string

const (
	// PhasePending 已创建但尚未完成首次对账。
	PhasePending Phase = "Pending"
	// PhaseReady 对账成功，终态就绪。
	PhaseReady Phase = "Ready"
	// PhaseReconciling 对账进行中。
	PhaseReconciling Phase = "Reconciling"
	// PhaseTerminating 对象已收到删除请求，等待终结器清除。
	PhaseTerminating Phase = "Terminating"
)

// FinalizerName 是本控制器使用的终结器名称。
const FinalizerName = "example.local/finalizer"

// ObjectSpec 是对象期望状态。
type ObjectSpec struct {
	// Message 对账成功后写入状态的期望消息，用于演示副作用。
	Message string
	// FailTimes 让前 N 次普通对账返回失败，用于演示失败重试（由外部执行器解释）。
	FailTimes int
	// FinalizeFailTimes 让前 N 次终结返回失败，用于演示终结重试。
	FinalizeFailTimes int
}

// ObjectStatus 是对象实际状态。
type ObjectStatus struct {
	Phase            Phase
	Message          string
	ObservedGen      int64
	Attempts         int
	FinalizeAttempts int
	Conditions       []Condition
	// SideEffectDone 标记副作用是否已执行（去重）。
	SideEffectDone bool
	// FinalizeDone 标记终结副作用是否已执行（去重）。
	FinalizeDone bool
	LastError    string
}

// Condition 描述一条对象状态条件，参考 K8s 的 Condition 风格。
type Condition struct {
	Type    string
	Status  string
	Reason  string
	Message string
	Time    time.Time
}

// Object 是内存存储中的资源对象。
type Object struct {
	Name       string
	Generation int64
	Spec       ObjectSpec
	Status     ObjectStatus
	Finalizers []string
	// DeletionTimestamp 非零表示对象处于待终结状态。
	DeletionTimestamp time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// Clone 返回对象的深拷贝。
func (o *Object) Clone() *Object {
	if o == nil {
		return nil
	}
	cp := *o
	if o.Finalizers != nil {
		cp.Finalizers = append([]string(nil), o.Finalizers...)
	}
	if o.Status.Conditions != nil {
		cp.Status.Conditions = append([]Condition(nil), o.Status.Conditions...)
	}
	return &cp
}

// StatusSummary 是聚合后的对外可读状态。
type StatusSummary struct {
	Name           string
	Generation     int64
	Phase          Phase
	Ready          bool
	SideEffectDone bool
	Finalizers     []string
	Deleting       bool
	Attempts       int
	Message        string
}

// EventKind 是事件类型。
type EventKind string

const (
	EventCreated EventKind = "Created"
	EventUpdated EventKind = "Updated"
	EventDeleted EventKind = "Deleted"
	EventRetry   EventKind = "Retry"
)

// Event 是写入存储后产生的对账事件。
type Event struct {
	Kind       EventKind
	Name       string
	Generation int64
	// ResourceVersion 是产生该事件时的版本，仅用于日志判定依据。
	ResourceVersion int64
	EnqueueAt       time.Time
}
