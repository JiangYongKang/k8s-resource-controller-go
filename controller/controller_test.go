package controller

import (
	"context"
	"errors"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

func newTestController(t *testing.T) *Controller {
	t.Helper()
	logger := log.New(os.Stdout, "["+t.Name()+"] ", log.LstdFlags|log.Lmicroseconds)
	c := New(NewStore(), nil, Config{
		BaseBackoff: 5 * time.Millisecond,
		MaxBackoff:  80 * time.Millisecond,
		Logger:      logger,
	})
	ctx, cancel := context.WithCancel(context.Background())
	go c.Run(ctx, 1)
	t.Cleanup(func() {
		cancel()
		c.Wait() // 等待优雅退出，避免 worker 泄漏影响后续测试
	})
	return c
}

// TestConcurrentWritesConflict 多个 goroutine 用同一个版本并发更新：
// 恰好一个成功，其余全部以 ErrVersionConflict 被拒，被拒写入不改变状态。
func TestConcurrentWritesConflict(t *testing.T) {
	c := newTestController(t)
	obj, rv, err := c.CreateWithName("w1", ObjectSpec{Message: "v0"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("created name=w1 gen=%d rv=%d", obj.Generation, rv)

	const n = 16
	var wg sync.WaitGroup
	var mu sync.Mutex
	success, conflicts, other := 0, 0, 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, newRV, err := c.Update("w1", rv, ObjectSpec{Message: "v" + itoa(i)})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				success++
				t.Logf("writer=%d accepted oldRV=%d newRV=%d verdict=success", i, rv, newRV)
			case errors.Is(err, ErrVersionConflict):
				conflicts++
				t.Logf("writer=%d rejected oldRV=%d verdict=ErrVersionConflict (currentRV differs)", i, rv)
			default:
				other++
				t.Logf("writer=%d rejected verdict=%v", i, err)
			}
		}(i)
	}
	wg.Wait()

	t.Logf("concurrent result: success=%d conflicts=%d other=%d (writers=%d)", success, conflicts, other, n)
	if success != 1 {
		t.Fatalf("expected exactly 1 success, got %d", success)
	}
	if conflicts != n-1 {
		t.Fatalf("expected %d conflicts, got %d", n-1, conflicts)
	}
	if other != 0 {
		t.Fatalf("unexpected rejection reasons: %d", other)
	}

	got, curRV, _ := c.Get("w1")
	t.Logf("final state gen=%d currentRV=%d message=%q; rejected writes must not change spec", got.Generation, curRV, got.Spec.Message)
	if got.Generation != 2 {
		t.Fatalf("generation should be 2, got %d", got.Generation)
	}
}

// TestRejectReasonDistinction 顺序固定地制造各类拒绝，验证原因彼此可区分：
// ErrNotFound / ErrVersionConflict / ErrTerminating / ErrAlreadyExists。
func TestRejectReasonDistinction(t *testing.T) {
	c := newTestController(t)

	// 1) 更新不存在对象 -> ErrNotFound
	if _, _, err := c.Update("ghost", 1, ObjectSpec{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	t.Logf("update missing object verdict=ErrNotFound")

	_, rv, err := c.CreateWithName("w2", ObjectSpec{Message: "a"})
	if err != nil {
		t.Fatal(err)
	}
	// 1b) 重复创建同名对象 -> ErrAlreadyExists
	if _, _, err := c.CreateWithName("w2", ObjectSpec{Message: "dup"}); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("want ErrAlreadyExists, got %v", err)
	}
	t.Logf("duplicate create verdict=ErrAlreadyExists (state untouched)")
	if got, _, _ := c.Get("w2"); got.Spec.Message != "a" {
		t.Fatalf("rejected create mutated state: %q", got.Spec.Message)
	}

	// 2) 过期版本 -> ErrVersionConflict
	if _, _, err := c.Update("w2", rv-1, ObjectSpec{Message: "b"}); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("want ErrVersionConflict, got %v", err)
	}
	t.Logf("stale rv=%d (current=%d) verdict=ErrVersionConflict", rv-1, rv)
	if got, _, _ := c.Get("w2"); got.Spec.Message != "a" {
		t.Fatalf("rejected write mutated state: %q", got.Spec.Message)
	}

	// 一次成功更新推进版本。
	_, rv2, err := c.Update("w2", rv, ObjectSpec{Message: "c"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("valid update rv=%d -> rv=%d verdict=success", rv, rv2)

	// 等待更新事件对账完成，避免后续手动标记删除时残留事件抢先触发终结。
	waitQueueIdle(c, time.Second)

	// 3) 删除中的写入语义：通过存储构造一个稳定的 Terminating 窗口
	// （此刻无残留事件，终结不会抢先发生），断言 ErrTerminating 与判定顺序。
	termObj, termRV, changed, err := c.store.MarkDeleting("w2", rv2)
	if err != nil || !changed {
		t.Fatalf("MarkDeleting: obj=%v changed=%v err=%v", termObj, changed, err)
	}
	t.Logf("deletion marked rv=%d deleting=true object still readable", termRV)

	// 删除中的对象仍可读，终结器仍在。
	readable, readRV, _ := c.Get("w2")
	if readable == nil || readable.DeletionTimestamp.IsZero() || len(readable.Finalizers) != 1 {
		t.Fatalf("terminating object should remain readable with finalizer: %+v", readable)
	}
	t.Logf("terminating object readable: phase=%s finalizers=%v rv=%d", readable.Status.Phase, readable.Finalizers, readRV)

	_, _, err = c.Update("w2", termRV, ObjectSpec{Message: "resurrect"})
	if !errors.Is(err, ErrTerminating) {
		t.Fatalf("want ErrTerminating, got %v", err)
	}
	t.Logf("normal update while terminating rv=%d verdict=ErrTerminating (cannot restore via normal update)", termRV)

	// 过期版本 + 删除中：固定判定顺序 notfound -> stale -> terminating，
	// 因此必须优先报 ErrVersionConflict。
	_, _, err = c.Update("w2", rv2, ObjectSpec{Message: "old"})
	if !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("want ErrVersionConflict before terminating check, got %v", err)
	}
	t.Logf("stale rv while terminating verdict=ErrVersionConflict (order: notfound->stale->terminating)")
}

// waitQueueIdle 轮询直到队列没有待处理与在飞事件。
func waitQueueIdle(c *Controller, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if p, inf := c.QueueStats(); p == 0 && inf == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
}

// TestFailedReconcileBackoff 前 3 次对账失败，第 4 次成功：
// 验证退避序列可观测、最终 Ready 且副作用恰好执行一次。
func TestFailedReconcileBackoff(t *testing.T) {
	logger := log.New(os.Stdout, "["+t.Name()+"] ", log.LstdFlags|log.Lmicroseconds)
	c := New(NewStore(), nil, Config{
		BaseBackoff: 5 * time.Millisecond,
		MaxBackoff:  80 * time.Millisecond,
		Logger:      logger,
	})
	ctx, cancel := context.WithCancel(context.Background())
	go c.Run(ctx, 1)

	var effectMu sync.Mutex
	effects := 0
	// 按对象名过滤：其他测试控制器退出收尾时触发的 hook 不计入本测试。
	SetSideEffectHook(func(phase string, obj *Object) {
		if obj.Name != "r1" {
			return
		}
		effectMu.Lock()
		effects++
		effectMu.Unlock()
		t.Logf("side effect phase=%s name=%s total=%d", phase, obj.Name, effects)
	})
	// 清理顺序很重要：先取消并等待 worker 完全退出，再恢复全局 hook，
	// 否则退出期间的收尾对账可能触发后续测试仍在使用的 hook。
	defer func() {
		cancel()
		c.Wait()
		SetSideEffectHook(nil)
	}()

	_, rv, err := c.CreateWithName("r1", ObjectSpec{Message: "done", FailTimes: 3})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("created name=r1 rv=%d failTimes=3 -> attempts 1..3 fail with backoff 5ms,10ms,20ms", rv)

	obj, ok := c.WaitForPhase("r1", PhaseReady, 3*time.Second)
	if !ok {
		t.Fatal("never reached Ready")
	}
	t.Logf("ready: attempts=%d observedGen=%d gen=%d phase=%s lastError=%q",
		obj.Status.Attempts, obj.Status.ObservedGen, obj.Generation, obj.Status.Phase, obj.Status.LastError)
	if obj.Status.Attempts != 4 {
		t.Fatalf("want 4 attempts, got %d", obj.Status.Attempts)
	}
	if !obj.Status.SideEffectDone {
		t.Fatal("SideEffectDone must be true")
	}
	effectMu.Lock()
	defer effectMu.Unlock()
	if effects != 1 {
		t.Fatalf("side effect must run exactly once despite retries, got %d", effects)
	}

	base, max := 5*time.Millisecond, 80*time.Millisecond
	want := []time.Duration{5 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond, 80 * time.Millisecond, 80 * time.Millisecond}
	for i, w := range want {
		if got := Backoff(base, max, i+1); got != w {
			t.Fatalf("backoff attempt %d: want %s got %s", i+1, w, got)
		}
	}
	t.Logf("backoff sequence verified: %v..., capped at %s", want[:4], max)
}

// TestEventDeduplication 连续更新与手工灌入的重复事件不会产生重复副作用。
func TestEventDeduplication(t *testing.T) {
	c := newTestController(t)

	var effectMu sync.Mutex
	effects := 0
	// 按对象名过滤：其他测试控制器退出收尾时触发的 hook 不计入本测试。
	SetSideEffectHook(func(phase string, obj *Object) {
		if obj.Name != "d1" {
			return
		}
		effectMu.Lock()
		effects++
		effectMu.Unlock()
	})
	defer func() { SetSideEffectHook(nil) }()

	_, _, err := c.CreateWithName("d1", ObjectSpec{Message: "v0"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.WaitForPhase("d1", PhaseReady, time.Second); !ok {
		t.Fatal("not ready")
	}
	for i := 1; i <= 10; i++ {
		_, curRV, _ := c.Get("d1")
		_, newRV, err := c.Update("d1", curRV, ObjectSpec{Message: "v" + itoa(i)})
		if err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
		t.Logf("update i=%d rv=%d->%d", i, curRV, newRV)
	}
	// 手工灌 50 个重复事件，模拟“重复产生的变更事件”。
	for i := 0; i < 50; i++ {
		c.queue.Add(Event{Kind: EventUpdated, Name: "d1", Generation: 99, ResourceVersion: 99})
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		p, inf := c.QueueStats()
		if p == 0 && inf == 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	obj, _, _ := c.Get("d1")
	effectMu.Lock()
	defer effectMu.Unlock()
	t.Logf("after 10 updates + 50 duplicate events: reconcile side effects=%d sideEffectDone=%v phase=%s",
		effects, obj.Status.SideEffectDone, obj.Status.Phase)
	if effects != 1 {
		t.Fatalf("reconcile side effect expected exactly once, got %d", effects)
	}
	if obj.Spec.Message != "v10" {
		t.Fatalf("final spec should come from last accepted update, got %q", obj.Spec.Message)
	}
}

// TestFinalizeRetryAndDedup 删除时前 2 次终结失败，第 3 次成功：
// 验证终结按退避重试、期间对象仍可读且处于 Terminating、终结副作用
// 恰好一次，重复 Deleted 事件不产生重复副作用。
func TestFinalizeRetryAndDedup(t *testing.T) {
	logger := log.New(os.Stdout, "["+t.Name()+"] ", log.LstdFlags|log.Lmicroseconds)
	c := New(NewStore(), nil, Config{
		BaseBackoff: 5 * time.Millisecond,
		MaxBackoff:  80 * time.Millisecond,
		Logger:      logger,
	})
	ctx, cancel := context.WithCancel(context.Background())
	go c.Run(ctx, 1)
	defer func() {
		cancel()
		c.Wait()
	}()

	var mu sync.Mutex
	finalizeHits := 0
	SetSideEffectHook(func(phase string, obj *Object) {
		if obj.Name != "f1" || phase != "finalize" {
			return
		}
		mu.Lock()
		finalizeHits++
		mu.Unlock()
		t.Logf("finalize side effect hit=%d", finalizeHits)
	})
	defer func() { SetSideEffectHook(nil) }()

	_, _, err := c.CreateWithName("f1", ObjectSpec{Message: "x", FinalizeFailTimes: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.WaitForPhase("f1", PhaseReady, time.Second); !ok {
		t.Fatal("not ready before delete")
	}

	// 立刻重复发 3 次删除请求：只有第一次推进状态，其余幂等折叠。
	var changedCount int
	for i := 0; i < 3; i++ {
		_, curRV, gerr := c.Get("f1")
		if gerr != nil {
			break
		}
		_, _, derr := c.Delete("f1", curRV)
		if derr != nil {
			t.Logf("duplicate delete i=%d verdict=%v", i, derr)
			break
		}
		changedCount++
		t.Logf("delete request i=%d accepted rv=%d", i, curRV)
	}
	t.Logf("delete requests that were accepted by API=%d (only the first transitions state)", changedCount)

	// 删除处理中：对象仍可读，终结器仍在，阶段为 Terminating。
	time.Sleep(2 * time.Millisecond)
	termObj, termRV, gerr := c.Get("f1")
	if gerr != nil {
		t.Fatalf("terminating object must remain readable: %v", gerr)
	}
	if termObj.DeletionTimestamp.IsZero() || termObj.Status.Phase != PhaseTerminating {
		t.Fatalf("want Terminating object, got %+v", termObj)
	}
	t.Logf("during deletion: readable phase=%s finalizeAttempts=%d finalizers=%v rv=%d",
		termObj.Status.Phase, termObj.Status.FinalizeAttempts, termObj.Finalizers, termRV)

	// 删除中的普通更新仍被拒，且不能把对象恢复为正常。
	if _, _, err := c.Update("f1", termRV, ObjectSpec{Message: "revive"}); !errors.Is(err, ErrTerminating) {
		t.Fatalf("want ErrTerminating, got %v", err)
	}
	t.Logf("update during terminating verdict=ErrTerminating (cannot restore normal state)")

	if !c.WaitForRemoved("f1", 3*time.Second) {
		t.Fatal("object not removed after finalize retries")
	}
	if _, _, err := c.Get("f1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound after removal, got %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	t.Logf("finalize finished: sideEffects=%d (expect exactly 1)", finalizeHits)
	if finalizeHits != 1 {
		t.Fatalf("finalize side effect must run once despite retries/duplicate deletes, got %d", finalizeHits)
	}

	// 汇总：删除后 Summary 报 NotFound。
	if _, err := c.Summary("f1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("summary after removal want ErrNotFound, got %v", err)
	}
}

// TestGracefulShutdownNoEventLoss 取消（模拟退出信号）前接受的事件
// 必须在 Run 返回前全部处理完，即使事件需要失败重试。
func TestGracefulShutdownNoEventLoss(t *testing.T) {
	logger := log.New(os.Stdout, "["+t.Name()+"] ", log.LstdFlags|log.Lmicroseconds)
	c := New(NewStore(), nil, Config{
		BaseBackoff: 5 * time.Millisecond,
		MaxBackoff:  80 * time.Millisecond,
		Logger:      logger,
	})
	ctx, cancel := context.WithCancel(context.Background())
	doneRun := make(chan struct{})
	go func() {
		c.Run(ctx, 2)
		close(doneRun)
	}()

	// 一个会失败两次的对象 + 一个正常对象；接受后立即发出退出信号。
	_, _, err := c.CreateWithName("s1", ObjectSpec{Message: "a", FailTimes: 2})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = c.CreateWithName("s2", ObjectSpec{Message: "b"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("events accepted for s1(failTimes=2) and s2; canceling immediately to simulate SIGTERM")
	cancel()

	select {
	case <-doneRun:
		t.Logf("Run returned after draining accepted events")
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after shutdown")
	}

	// 退出前已接受事件全部落地：s2 Ready；s1 也经历失败重试后 Ready。
	obj2, _, err := c.Get("s2")
	if err != nil || obj2.Status.Phase != PhaseReady || !obj2.Status.SideEffectDone {
		t.Fatalf("s1 events lost? obj=%+v err=%v", obj2, err)
	}
	obj1, _, err := c.Get("s1")
	if err != nil || obj1.Status.Phase != PhaseReady || obj1.Status.Attempts != 3 {
		t.Fatalf("accepted failing event lost or not retried: obj=%+v err=%v attempts want 3", obj1, err)
	}
	t.Logf("post-shutdown state: s1 phase=%s attempts=%d sideEffectDone=%v; s2 phase=%s sideEffectDone=%v",
		obj1.Status.Phase, obj1.Status.Attempts, obj1.Status.SideEffectDone, obj2.Status.Phase, obj2.Status.SideEffectDone)
}

// TestSummaryConsistency 汇总状态必须与真实进度一致。
func TestSummaryConsistency(t *testing.T) {
	c := newTestController(t)

	// 先直接在存储中造对象但不入队，制造确定的“尚未对账”初始状态。
	now := time.Now()
	obj := &Object{
		Name:       "sum",
		Spec:       ObjectSpec{Message: "m"},
		Finalizers: []string{FinalizerName},
		Status: ObjectStatus{
			Phase:      PhasePending,
			Conditions: []Condition{{Type: "Ready", Status: "False", Reason: "NotReconciled", Time: now}},
		},
	}
	saved, rv, err := c.store.Create(obj)
	if err != nil {
		t.Fatal(err)
	}
	s0, err := c.Summary("sum")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("summary before any event: phase=%s ready=%v deleting=%v finalizers=%d rv=%d", s0.Phase, s0.Ready, s0.Deleting, len(s0.Finalizers), rv)
	if s0.Ready || s0.Phase != PhasePending {
		t.Fatalf("initial summary should be Pending/not-ready: %+v", s0)
	}

	// 投递事件并等待对账完成。
	c.enqueue(Event{Kind: EventCreated, Name: "sum", Generation: saved.Generation, ResourceVersion: rv, EnqueueAt: now})
	if _, ok := c.WaitForPhase("sum", PhaseReady, time.Second); !ok {
		t.Fatal("not ready")
	}
	s1, _ := c.Summary("sum")
	t.Logf("summary after reconcile: phase=%s ready=%v sideEffectDone=%v deleting=%v attempts=%d",
		s1.Phase, s1.Ready, s1.SideEffectDone, s1.Deleting, s1.Attempts)
	if !s1.Ready || !s1.SideEffectDone || s1.Deleting {
		t.Fatalf("summary inconsistent with progress: %+v", s1)
	}
}

// TestMultiWorkerSingleFlight 多 worker 下不同对象并行对账，但同一对象
// 即使有大量重复事件也不会产生重复副作用（严格单飞 + 原子认领）。
func TestMultiWorkerSingleFlight(t *testing.T) {
	logger := log.New(os.Stdout, "["+t.Name()+"] ", log.LstdFlags|log.Lmicroseconds)
	c := New(NewStore(), nil, Config{BaseBackoff: time.Millisecond, MaxBackoff: 20 * time.Millisecond, Logger: logger})
	ctx, cancel := context.WithCancel(context.Background())
	go c.Run(ctx, 4)
	defer func() {
		cancel()
		c.Wait()
	}()

	const objects = 8
	var mu sync.Mutex
	hits := map[string]int{}
	SetSideEffectHook(func(phase string, obj *Object) {
		if phase != "reconcile" {
			return
		}
		mu.Lock()
		hits[obj.Name]++
		mu.Unlock()
	})
	defer func() { SetSideEffectHook(nil) }()

	for i := 0; i < objects; i++ {
		name := "m" + itoa(i)
		if _, _, err := c.CreateWithName(name, ObjectSpec{Message: name}); err != nil {
			t.Fatal(err)
		}
	}
	// 对每个对象灌入 100 条重复事件。
	for i := 0; i < objects; i++ {
		name := "m" + itoa(i)
		for j := 0; j < 100; j++ {
			c.queue.Add(Event{Kind: EventUpdated, Name: name, Generation: int64(j + 1)})
		}
	}
	// 等待全部对象 Ready 且队列空闲。
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		allReady := true
		for i := 0; i < objects; i++ {
			if o, _, err := c.Get("m" + itoa(i)); err != nil || o.Status.Phase != PhaseReady || !o.Status.SideEffectDone {
				allReady = false
			}
		}
		if p, inf := c.QueueStats(); allReady && p == 0 && inf == 0 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	t.Logf("multi-worker: %d objects, 100 duplicate events each, side effects per object=%v", objects, hits)
	if len(hits) != objects {
		t.Fatalf("want exactly %d objects with one effect each, got %d", objects, len(hits))
	}
	for name, n := range hits {
		if n != 1 {
			t.Fatalf("object %s side effect ran %d times, want 1", name, n)
		}
	}
}
