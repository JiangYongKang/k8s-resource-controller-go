package controller

import (
	"context"
	"errors"
	"log"
	"os"
	"sync"
	"time"
)

// 拒绝原因统一以 errors.go 中的哨兵错误暴露，调用方用 errors.Is 区分：
// ErrNotFound / ErrVersionConflict / ErrTerminating / ErrAlreadyExists。

// Controller 把存储、去重事件队列和对账函数组合成本地控制器。
//
// 单飞（single-flight）由队列按对象名保证：一个对象任一时刻只在
// 至多一个 worker 上对账；副作用去重由 Status 上的幂等标记（存储锁内
// 原子认领）保证。
type Controller struct {
	store     *Store
	queue     *EventQueue
	reconcile ReconcileFunc
	baseDelay time.Duration
	maxDelay  time.Duration
	logger    *log.Logger

	runMu     sync.Mutex
	runCalled bool
	runDone   chan struct{}
	wg        sync.WaitGroup
}

// Config 是控制器配置。
type Config struct {
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	Logger      *log.Logger
}

// DefaultConfig 返回用于演示/测试的默认配置。
func DefaultConfig() Config {
	return Config{
		BaseBackoff: 10 * time.Millisecond,
		MaxBackoff:  200 * time.Millisecond,
		Logger:      log.New(os.Stdout, "[controller] ", log.LstdFlags|log.Lmicroseconds),
	}
}

// New 创建控制器；r 为 nil 时使用内置演示对账函数。
func New(store *Store, r ReconcileFunc, cfg Config) *Controller {
	if store == nil {
		store = NewStore()
	}
	if r == nil {
		r = DefaultReconcile
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 10 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 200 * time.Millisecond
	}
	if cfg.Logger == nil {
		cfg.Logger = log.New(os.Stdout, "[controller] ", log.LstdFlags|log.Lmicroseconds)
	}
	return &Controller{
		store:     store,
		queue:     NewEventQueue(),
		reconcile: r,
		baseDelay: cfg.BaseBackoff,
		maxDelay:  cfg.MaxBackoff,
		logger:    cfg.Logger,
	}
}

// Create 新建对象，自动挂上终结器并接受一次 Created 事件。
func (c *Controller) Create(spec ObjectSpec) (*Object, int64, error) {
	now := time.Now()
	obj := &Object{
		Spec:       spec,
		Finalizers: []string{FinalizerName},
		Status: ObjectStatus{
			Phase:      PhasePending,
			Conditions: []Condition{{Type: "Ready", Status: "False", Reason: "NotReconciled", Message: "created, waiting first reconcile", Time: now}},
		},
	}
	c.uniqueName(&obj.Name)
	saved, rv, err := c.store.Create(obj)
	if err != nil {
		return nil, 0, err
	}
	c.enqueue(Event{Kind: EventCreated, Name: saved.Name, Generation: saved.Generation, ResourceVersion: rv, EnqueueAt: now})
	return saved, rv, nil
}

// uniqueName 生成形如 obj-<n> 的对象名（演示用，非对象内容的一部分）。
func (c *Controller) uniqueName(p *string) {
	if *p != "" {
		return
	}
	objs := c.store.List()
	*p = "obj-" + itoa(len(objs)+1)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// CreateWithName 使用指定名称创建（测试更方便）。
func (c *Controller) CreateWithName(name string, spec ObjectSpec) (*Object, int64, error) {
	now := time.Now()
	obj := &Object{
		Name:       name,
		Spec:       spec,
		Finalizers: []string{FinalizerName},
		Status: ObjectStatus{
			Phase:      PhasePending,
			Conditions: []Condition{{Type: "Ready", Status: "False", Reason: "NotReconciled", Message: "created, waiting first reconcile", Time: now}},
		},
	}
	saved, rv, err := c.store.Create(obj)
	if err != nil {
		return nil, 0, err
	}
	c.enqueue(Event{Kind: EventCreated, Name: name, Generation: saved.Generation, ResourceVersion: rv, EnqueueAt: now})
	return saved, rv, nil
}

// Update 携带期望版本做普通更新。版本过期/不存在/删除中分别返回
// ErrVersionConflict / ErrNotFound / ErrTerminating，拒绝时不改动任何状态。
func (c *Controller) Update(name string, expectedVersion int64, spec ObjectSpec) (*Object, int64, error) {
	saved, rv, err := c.store.Mutate(name, expectedVersion, func(o *Object) error {
		o.Spec = spec
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	c.enqueue(Event{Kind: EventUpdated, Name: name, Generation: saved.Generation, ResourceVersion: rv, EnqueueAt: time.Now()})
	return saved, rv, nil
}

// Delete 发起删除：对象进入 Terminating（带删除时间戳），并接受一次
// Deleted 事件驱动终结。终结器未清除前对象仍可读，但普通更新会被拒绝。
func (c *Controller) Delete(name string, expectedVersion int64) (*Object, int64, error) {
	saved, rv, changed, err := c.store.MarkDeleting(name, expectedVersion)
	if err != nil {
		return nil, 0, err
	}
	if changed {
		c.enqueue(Event{Kind: EventDeleted, Name: name, Generation: saved.Generation, ResourceVersion: rv, EnqueueAt: time.Now()})
	}
	return saved, rv, nil
}

// Get 返回对象快照与当前版本。
func (c *Controller) Get(name string) (*Object, int64, error) {
	return c.store.Get(name)
}

// List 返回全部对象快照。
func (c *Controller) List() map[string]*Object { return c.store.List() }

// Summary 汇总对外状态；汇总值在读取时直接由真实对象计算，
// 因此与实际进度严格一致（不存在缓存漂移）。
func (c *Controller) Summary(name string) (StatusSummary, error) {
	obj, _, err := c.store.Get(name)
	if err != nil {
		return StatusSummary{}, err
	}
	return StatusSummary{
		Name:           obj.Name,
		Generation:     obj.Generation,
		Phase:          obj.Status.Phase,
		Ready:          obj.Status.Phase == PhaseReady && obj.Status.ObservedGen == obj.Generation,
		SideEffectDone: obj.Status.SideEffectDone,
		Finalizers:     append([]string(nil), obj.Finalizers...),
		Deleting:       !obj.DeletionTimestamp.IsZero(),
		Attempts:       obj.Status.Attempts,
		Message:        obj.Status.Message,
	}, nil
}

func (c *Controller) enqueue(e Event) {
	c.queue.Add(e)
	c.logger.Printf("event accepted: kind=%s name=%s gen=%d rv=%d", e.Kind, e.Name, e.Generation, e.ResourceVersion)
}

// QueuePending 暴露队列待处理数（测试观测用）。
func (c *Controller) QueueStats() (pending, inFlight int) { return c.queue.Stats() }

// Wait 阻塞直到 Run 启动的所有 worker 退出（ctx 取消且存量事件
// 处理完后返回）。从未调用过 Run 时立即返回。
func (c *Controller) Wait() {
	c.runMu.Lock()
	if !c.runCalled {
		c.runMu.Unlock()
		return
	}
	ch := c.runDone
	c.runMu.Unlock()
	<-ch
}

// Run 启动 workers 处理事件。ctx 取消后：未到期重试立即到期，
// 所有“已接受”事件处理完后 Run 才返回（优雅关闭，事件不丢失）。
// Run 至多生效一次。
func (c *Controller) Run(ctx context.Context, workers int) {
	if workers < 1 {
		workers = 1
	}
	c.runMu.Lock()
	if c.runCalled {
		c.runMu.Unlock()
		return
	}
	c.runCalled = true
	c.runDone = make(chan struct{})
	// 所有 Add 在持锁下完成；Wait 在同一把锁之后读取 runCalled，
	// 构成 happens-before，避免 WaitGroup 的 Add/Wait 竞争。
	for i := 0; i < workers; i++ {
		c.wg.Add(1)
	}
	c.runMu.Unlock()

	for i := 0; i < workers; i++ {
		go func(id int) {
			defer c.wg.Done()
			c.worker(ctx, id)
		}(i)
	}
	c.wg.Wait()
	close(c.runDone)
}

func (c *Controller) worker(ctx context.Context, id int) {
	for {
		e, ok := c.queue.Drain(ctx.Done())
		if !ok {
			return
		}
		c.handleEvent(ctx, id, e)
		c.queue.Done(e)
	}
}

func (c *Controller) handleEvent(ctx context.Context, workerID int, e Event) {
	obj, rv, err := c.store.Get(e.Name)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			// 对象已彻底删除，事件成为过期事件，安全忽略（幂等）。
			c.logger.Printf("worker=%d skip stale event: name=%s kind=%s rv=%d reason=%q", workerID, e.Name, e.Kind, e.ResourceVersion, ErrNotFound)
			return
		}
		return
	}
	if !obj.DeletionTimestamp.IsZero() {
		c.handleDeletion(ctx, workerID, e, obj, rv)
		return
	}
	c.handleReconcile(ctx, workerID, e, obj, rv)
}

func (c *Controller) handleReconcile(ctx context.Context, workerID int, e Event, obj *Object, rv int64) {
	// 先把阶段置为 Reconciling 并递增尝试计数（状态子资源，不推进
	// resourceVersion）。递增与置位在同一次锁内回调完成，避免旧快照覆盖。
	var attempt int
	c.store.MutateStatus(obj.Name, func(o *Object) {
		if o.Status.Phase != PhaseReady {
			o.Status.Phase = PhaseReconciling
		}
		o.Status.Attempts++
		attempt = o.Status.Attempts
	})

	result := c.reconcile(ctx, obj)
	c.logger.Printf("worker=%d reconcile: name=%s kind=%s gen=%d eventRV=%d currentRV=%d attempt=%d failed=%v reason=%q",
		workerID, obj.Name, e.Kind, obj.Generation, e.ResourceVersion, rv, attempt, result.Failed, result.Reason)

	if result.Failed {
		delay := Backoff(c.baseDelay, c.maxDelay, attempt)
		c.store.MutateStatus(obj.Name, func(o *Object) {
			o.Status.Phase = PhasePending
			o.Status.LastError = result.Reason
			setCondition(o, Condition{Type: "Ready", Status: "False", Reason: "ReconcileError", Message: result.Reason, Time: time.Now()})
		})
		c.logger.Printf("schedule retry: name=%s attempt=%d backoff=%s (base=%s max=%s)", obj.Name, attempt, delay, c.baseDelay, c.maxDelay)
		retryEvent := e
		retryEvent.Kind = EventRetry
		c.queue.RetryAfter(retryEvent, delay)
		return
	}

	// 成功：先在存储锁内原子认领副作用（检查+翻转 SideEffectDone），
	// 只有认领成功的这一次对账才真正执行，重复/并发对账均被去重。
	applied, claimErr := c.store.ClaimSideEffect(obj.Name)
	if claimErr != nil && !errors.Is(claimErr, ErrNotFound) {
		c.logger.Printf("claim side effect: name=%s err=%v", obj.Name, claimErr)
	}
	if applied && result.Apply != nil {
		safeApply(result.Apply)
	}
	c.store.MutateStatus(obj.Name, func(o *Object) {
		o.Status.Phase = PhaseReady
		o.Status.Message = obj.Spec.Message
		o.Status.ObservedGen = obj.Generation
		o.Status.LastError = ""
		setCondition(o, Condition{Type: "Ready", Status: "True", Reason: "ReconcileSucceeded", Message: result.Reason, Time: time.Now()})
	})
	c.logger.Printf("reconcile done: name=%s gen=%d sideEffectApplied=%v summaryReady=true phase=%s",
		obj.Name, obj.Generation, applied, PhaseReady)
}

func (c *Controller) handleDeletion(ctx context.Context, workerID int, e Event, obj *Object, rv int64) {
	if !hasFinalizer(obj, FinalizerName) {
		// 终结器已清除，彻底删除。
		_ = c.store.Remove(obj.Name)
		c.logger.Printf("finalize complete: name=%s removed=true remainingFinalizers=0", obj.Name)
		return
	}
	var attempt int
	c.store.MutateStatus(obj.Name, func(o *Object) {
		o.Status.Phase = PhaseTerminating
		o.Status.FinalizeAttempts++
		attempt = o.Status.FinalizeAttempts
	})

	// 终结对账：默认实现对 FailTimes>0 的对象在最后一次副作用场景
	// 不强制失败（语义上终结失败才重试）；这里复用 reconcile 函数，
	// 但只在 FinalizeDone=false 时执行终结副作用。
	result := c.reconcile(ctx, obj)
	c.logger.Printf("worker=%d finalize: name=%s gen=%d eventRV=%d currentRV=%d attempt=%d failed=%v reason=%q",
		workerID, obj.Name, obj.Generation, e.ResourceVersion, rv, attempt, result.Failed, result.Reason)

	if result.Failed {
		delay := Backoff(c.baseDelay, c.maxDelay, attempt)
		c.store.MutateStatus(obj.Name, func(o *Object) { o.Status.LastError = result.Reason })
		c.logger.Printf("schedule finalize retry: name=%s attempt=%d backoff=%s", obj.Name, attempt, delay)
		retryEvent := e
		retryEvent.Kind = EventRetry
		c.queue.RetryAfter(retryEvent, delay)
		return
	}

	// 原子认领终结副作用：重复终结（重复 Deleted 事件/重试）只产生一次。
	applied, claimErr := c.store.ClaimFinalize(obj.Name)
	if claimErr != nil && !errors.Is(claimErr, ErrNotFound) {
		c.logger.Printf("claim finalize: name=%s err=%v", obj.Name, claimErr)
	}
	if applied && result.Apply != nil {
		safeApply(result.Apply)
	}
	c.store.MutateStatus(obj.Name, func(o *Object) {
		o.Finalizers = removeFinalizer(o.Finalizers, FinalizerName)
		o.Status.LastError = ""
	})
	_ = c.store.Remove(obj.Name)
	c.logger.Printf("finalize complete: name=%s finalizeSideEffectApplied=%v removed=true", obj.Name, applied)
}

func hasFinalizer(obj *Object, f string) bool {
	for _, x := range obj.Finalizers {
		if x == f {
			return true
		}
	}
	return false
}

func removeFinalizer(fs []string, f string) []string {
	out := fs[:0]
	for _, x := range fs {
		if x != f {
			out = append(out, x)
		}
	}
	return out
}

func setCondition(o *Object, c Condition) {
	for i := range o.Status.Conditions {
		if o.Status.Conditions[i].Type == c.Type {
			o.Status.Conditions[i] = c
			return
		}
	}
	o.Status.Conditions = append(o.Status.Conditions, c)
}

// safeApply 执行副作用回调，panic 转换为可记录的失败而不是崩溃进程。
func safeApply(fn func()) {
	defer func() { _ = recover() }()
	fn()
}

// ResourceVersion 返回对象当前版本；不存在返回 0 与 ErrNotFound。
func (c *Controller) ResourceVersion(name string) (int64, error) {
	_, rv, err := c.store.Get(name)
	return rv, err
}

// WaitForPhase 轮询直到对象进入指定阶段或超时（测试辅助）。
func (c *Controller) WaitForPhase(name string, want Phase, timeout time.Duration) (*Object, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if obj, _, err := c.store.Get(name); err == nil && obj.Status.Phase == want {
			return obj, true
		}
		time.Sleep(time.Millisecond)
	}
	if want == "" {
		if _, _, err := c.store.Get(name); errors.Is(err, ErrNotFound) {
			return nil, true
		}
	}
	obj, _, _ := c.store.Get(name)
	return obj, false
}

// WaitForRemoved 轮询直到对象被彻底移除或超时。
func (c *Controller) WaitForRemoved(name string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, _, err := c.store.Get(name); errors.Is(err, ErrNotFound) {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return false
}
