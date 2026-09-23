package controller

import (
	"context"
	"math"
	"time"
)

// ReconcileFunc 执行一次对账。实现必须做到：同一次对账内先检查
// 副作用是否已完成（幂等标记），再执行副作用，使重复对账不会产生
// 重复副作用。失败通过 ReconcileResult{Failed:true} 返回，由控制器
// 按 Backoff 观测的指数策略安排重试。
type ReconcileFunc func(ctx context.Context, obj *Object) ReconcileResult

// ReconcileResult 是对账结果。
type ReconcileResult struct {
	// Failed 为 true 表示本次对账失败，需要退避后重试。
	Failed bool
	// Reason 失败/成功原因，写入 conditions 与日志，便于判定依据可观测。
	Reason string
	// Apply 在存储写锁之外调用，用于产生外部副作用（演示用回调）。
	// 控制器只在 SideEffectDone 仍为 false 时调用它。
	Apply func()
}

// Backoff 计算可观测的指数退避：base * 2^(attempt-1)，封顶 max。
// attempt 从 1 开始（第 1 次失败后等待 base，第 2 次 2*base ...）。
func Backoff(base, max time.Duration, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := float64(base) * math.Pow(2, float64(attempt-1))
	if d > float64(max) || d < 0 {
		return max
	}
	return time.Duration(d)
}
