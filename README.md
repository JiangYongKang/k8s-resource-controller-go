# 本地 Kubernetes 风格资源控制器（Go）

一个**纯进程内**的、Kubernetes controller 风格的资源控制器：用带版本号的
内存存储替代真实集群，用 workqueue 风格的去重事件队列驱动对账（reconcile），
用终结器（finalizer）表达删除语义。目标是让对象在**并发写入、对账失败、
删除处理**下的状态变化**确定、可复现、可解释**。

只依赖 Go 标准库；`go.mod` 中无任何第三方依赖。

## 目录结构

```
controller/
  types.go             对象/事件/状态等类型定义
  errors.go            四类彼此可区分的拒绝原因（哨兵错误）
  store.go             带乐观并发控制（OCC）的进程内存储
  queue.go             去重 + 单飞 + 延迟重试的内存事件队列
  reconcile.go         对账函数签名与指数退避 Backoff
  default_reconcile.go 内置演示对账（可注入失败与副作用回调）
  controller.go        组装以上组件，提供 Create/Update/Delete/Get/Summary/Run
cmd/demo/main.go       端到端演示（失败重试 -> Ready -> 终结删除 -> 优雅退出）
controller/*_test.go   8 个单测（并发冲突、重试、去重、终结、优雅关闭、汇总）
```

## 快速验证

```bash
# 全部单测
go test ./...

# 带竞态检测并反复运行（结论稳定可复现）
go test ./... -race -count=100

# 查看打印了版本、事件与判定依据的详细日志
go test ./controller/ -race -v -count=1

# 端到端演示
go run ./cmd/demo
```

演示程序依次展示：创建（前 2 次对账确定性失败）→ 按指数退避重试到
`Ready` → 发起删除进入 `Terminating` → 终结完成彻底移除 → 优雅退出。
运行期间按下 `Ctrl-C`（SIGINT/SIGTERM）同样会走优雅关闭路径。

## 1. 对象状态流转（生命周期）

```
                 Create
                   |
                   v
              +---------+   普通对账成功    +-------+
              | Pending | ---------------> | Ready |
              +---------+                   +-------+
                   ^                            |
        对账失败(退避重试)        普通对账中/新更新 | 对账进行
                   +------------> Reconciling <--+
                   |
        Delete(删除请求，打 DeletionTimestamp，挂终结器)
                   v
            +-------------+   终结器仍在：终结对账
            | Terminating |   - 失败 -> 指数退避重试（对象仍可读）
            +-------------+   - 成功 -> 移除终结器 -> 彻底删除(NotFound)
```

- **Pending**：已创建但尚未完成首次对账（或上次对账失败后等待重试）。
- **Reconciling**：正在对账（瞬时状态，写入 status 子资源，不推进版本）。
- **Ready**：对账成功，`ObservedGen == Generation` 且副作用已完成。
- **Terminating**：已收到删除请求、`DeletionTimestamp` 非零，等待终结器清除。
  在此期间对象**仍可 Get/List**，但**普通 Update 一律被拒绝**，因此无法被
  “恢复”为正常状态；终结器清空后对象才被彻底移除，此后读取得到
  `ErrNotFound`。

汇总状态（`Summary`）在读取时**直接由真实对象即时计算**（不经过任何异步
缓存），所以汇总与实际进度严格一致：`Ready = (phase==Ready &&
ObservedGen==Generation)`。

## 2. 乐观并发控制（OCC）与版本语义

- 每次 `Create/Update/Delete` 返回当前的 **resourceVersion**（单调递增整数）。
  `Update`/`Delete` 必须携带**期望版本**。
- 写入在存储的写锁内按**固定顺序**判定，原因彼此可区分：

  | 情况 | 返回错误 | 说明 |
  |---|---|---|
  | 对象不存在（或已彻底删除） | `ErrNotFound` | 写入前对象不在存储中 |
  | 携带版本 ≠ 当前版本 | `ErrVersionConflict` | 版本过期，乐观冲突 |
  | 对象处于 Terminating | `ErrTerminating` | 删除中的普通写入被拒 |
  | 创建同名已存在对象 | `ErrAlreadyExists` | 仅 Create 路径 |

  当“版本过期”与“删除中”同时成立时，**优先**返回 `ErrVersionConflict`
  （判定顺序固定为 `notfound -> stale -> terminating`），调用方永远能得到
  唯一确定的原因，使用 `errors.Is(err, controller.ErrXxx)` 区分。

- **被拒绝的写入不改动任何已有状态**：实现上先在对象的**克隆**上执行变更，
  只有全部校验通过后才用克隆替换原对象；`mut` 返回错误或校验失败时原对象
  与版本号保持不变。
- **resourceVersion 只随“规格/元数据”写入推进**；控制器后台写的 status
  子资源（阶段、尝试次数、Conditions）不推进对外版本（类似 K8s 的 status
  子资源），因此后台对账不会让用户手里的版本意外过期。
- `Delete` 对同一版本重复调用是**幂等**的：已在 Terminating 则返回当前
  快照且不再产生新版本/新事件。

## 3. 对账：单飞、去重、退避重试

### 单飞（single-flight）
事件队列按对象名管理三个集合：

- `pending`：等待处理，每个名字**至多一条**事件，重复 `Add` 折叠为代数
  更新者（去重）；
- `processing`：正在对账的名字。名字在此集合中时**绝不会再次被弹出**，
  所以同一对象任一时刻只有一个对账在推进（即使配置多个 worker）；
- `deferred`：对账进行中新到达的事件在此折叠，该次对账 `Done` 后原子地
  转入 `pending` 补处理——更新**不丢失**，也**不并发**。

### 重复事件不产生重复副作用
两层保护：

1. 队列折叠 + 单飞：重复/折叠的变更事件不会造成重复对账；
2. 存储锁内的**原子认领**：成功对账时先调用 `ClaimSideEffect`
   （将 `SideEffectDone` 从 false 原子翻转为 true），**只有赢得认领的
   这一次**才真正执行副作用回调；终结路径同理用 `ClaimFinalize`/
   `FinalizeDone`。因此重复终结也不会重复产生副作用。

### 失败重试与可观测退避
对账返回 `ReconcileResult{Failed:true, Reason}` 后，事件进入延迟堆，按

```
Backoff(base, max, attempt) = min(base * 2^(attempt-1), max)     // attempt 从 1 起
```

调度重试（默认 base=10ms、max=200ms；测试用 5ms/80ms）。每次失败、每次
重试调度都会打印 `name / attempt / backoff / base / max / reason`，
退避序列完全可观测；`Backoff` 是纯函数，可直接单测断言
（5ms→10ms→20ms→40ms→80ms→封顶 80ms）。

内置演示对账用 `Spec.FailTimes` / `Spec.FinalizeFailTimes` **确定性**地
注入前 N 次失败，从而稳定复现“失败 → 退避 → 重试成功”。

## 4. 终结器删除语义

- `Create` 时自动挂上终结器 `example.local/finalizer`。
- `Delete` 不立即移除对象，而是：校验版本 → 打 `DeletionTimestamp`
  （进入 Terminating，推进版本）→ 入队一个 Deleted 事件。
- 终结对账：失败则退避重试（期间对象保持 Terminating、仍可读）；成功则
  **原子认领终结副作用（只执行一次）→ 移除终结器 → 彻底删除对象**。
- Terminating 期间普通 Update 被 `ErrTerminating` 拒绝，对象不能被普通
  更新“复活”。

## 5. 优雅关闭：已接受事件不丢失

`Run(ctx, workers)` 在 `ctx` 取消（演示程序绑定 SIGINT/SIGTERM）后：

1. 队列停止接受**新**事件（此前已接受的事件不受影响）；
2. 所有未到期的**重试立即到期**；
3. `pending/deferred/timers` 中的存量事件全部处理完、且没有在飞对账后，
   worker 才退出，`Run`/`Wait` 才返回。

注意这是“进程内、进程生命周期内”的不丢失：不模拟跨进程持久化（与题目
“用进程内存储替代真实集群”一致）。

## 6. 单测如何打印版本、事件与判定依据

测试日志（`go test -v`）对每个关键动作都打印判定依据，例如：

- 并发写：`writer=i accepted oldRV=1 newRV=2 verdict=success` 或
  `verdict=ErrVersionConflict (currentRV differs)`，并汇总
  `success=1 conflicts=15 other=0`；
- 事件：`event accepted: kind=Updated name=w2 gen=2 rv=2`；
- 对账：`reconcile ... eventRV=2 currentRV=2 attempt=3 failed=false reason=...`；
- 重试：`schedule retry: name=r1 attempt=1 backoff=5ms (base=5ms max=80ms)`；
- 终结：`finalize complete: name=f1 finalizeSideEffectApplied=true removed=true`；
- 收尾：`Run returned after draining accepted events`。

| 测试 | 覆盖点 |
|---|---|
| `TestConcurrentWritesConflict` | 16 goroutine 同版本并发更新：恰 1 成功、15 冲突，被拒写不改状态 |
| `TestRejectReasonDistinction` | 四类拒绝原因 + 判定顺序 + Terminating 仍可读且不可恢复 |
| `TestFailedReconcileBackoff` | 前 3 次失败第 4 次成功、退避序列、副作用仅一次 |
| `TestEventDeduplication` | 10 次更新 + 50 条重复事件，副作用仍只一次，最终规格来自最后一次接受的更新 |
| `TestFinalizeRetryAndDedup` | 终结失败重试、Terminating 中读取/拒绝更新、重复删除、终结副作用仅一次 |
| `TestGracefulShutdownNoEventLoss` | 接受事件后立即取消，Run 返回前失败事件也被重试到终态 |
| `TestSummaryConsistency` | 汇总在对账前后与真实进度一致 |
| `TestMultiWorkerSingleFlight` | 4 worker × 8 对象 × 每对象 100 重复事件，每对象副作用恰 1 次 |

## 7. 设计取舍说明

- 进程内存储 + `sync.RWMutex`：确定性强、无外部依赖；不提供跨进程持久性。
- 副作用回调 `SetSideEffectHook` 带读写锁保护，便于演示/测试注入，控制器
  运行期间设置也是并发安全的。
- 对账函数是注入式接口（`ReconcileFunc`），默认实现仅用于演示；替换它即可
  承载真实业务逻辑，OCC/队列/终结/退避机制无需改动。
