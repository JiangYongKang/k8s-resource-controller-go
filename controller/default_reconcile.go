package controller

import (
	"context"
	"sync"
)

var (
	sideEffectMu   sync.RWMutex
	sideEffectHook func(phase string, obj *Object)
)

// SetSideEffectHook 注入副作用回调（测试/演示用，nil 取消）。
// 并发安全：控制器运行期间也可设置。回调只在原子认领成功后被调用一次。
func SetSideEffectHook(fn func(phase string, obj *Object)) {
	sideEffectMu.Lock()
	sideEffectHook = fn
	sideEffectMu.Unlock()
}

func invokeSideEffectHook(phase string, obj *Object) {
	sideEffectMu.RLock()
	fn := sideEffectHook
	sideEffectMu.RUnlock()
	if fn != nil {
		fn(phase, obj)
	}
}

// DefaultReconcile 是内置演示对账函数：
//
//   - 删除中（Terminating）：成功完成终结；终结副作用只执行一次
//     （由调用方结合 FinalizeDone 的原子认领去重）。
//   - 普通状态：Spec.FailTimes 大于已观测到的尝试次数时返回失败，
//     用于确定性地演示“失败 -> 指数退避重试”。
//
// 注意：失败判定只读入参快照；Attempts 由控制器在调用前后维护。
func DefaultReconcile(ctx context.Context, obj *Object) ReconcileResult {
	if !obj.DeletionTimestamp.IsZero() {
		if obj.Spec.FinalizeFailTimes > 0 && obj.Status.FinalizeAttempts < obj.Spec.FinalizeFailTimes {
			remaining := obj.Spec.FinalizeFailTimes - obj.Status.FinalizeAttempts
			return ReconcileResult{
				Failed: true,
				Reason: "injected finalize failure: remaining=" + itoa(remaining),
			}
		}
		return ReconcileResult{
			Failed: false,
			Reason: "finalize cleanup ok",
			Apply:  func() { invokeSideEffectHook("finalize", obj) },
		}
	}

	if obj.Spec.FailTimes > 0 && obj.Status.Attempts < obj.Spec.FailTimes {
		remaining := obj.Spec.FailTimes - obj.Status.Attempts
		return ReconcileResult{
			Failed: true,
			Reason: "injected failure: remaining=" + itoa(remaining),
		}
	}

	return ReconcileResult{
		Failed: false,
		Reason: "side effect applied idempotently",
		Apply:  func() { invokeSideEffectHook("reconcile", obj) },
	}
}
