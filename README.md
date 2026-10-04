# 本地 Kubernetes 风格资源控制器（Go，进程内存储）

不依赖真实集群：用带互斥锁的内存 `map` 充当 “etcd”，用内存附件表充当 “外部系统”，
完整实现 Kubernetes 风格的 **乐观并发控制（resourceVersion）**、**工作队列（单飞去重 +
延迟退避）**、**终结器（finalizer）删除语义** 与 **优雅关停排空**。所有写入都在
`sync.RWMutex` / 单把队列锁下完成，状态变化确定、可解释、可复现（全部测试在
`-race -count=N` 下稳定）。

## 目录结构

| 路径 | 职责 |
| --- | --- |
| `pkg/apis/v1/types.go` | `Widget` 资源、版本/终结器/删除时间戳元数据、状态条件、`AggregatedPhase()` 汇总 |
| `pkg/storage/store.go` | 进程内存储：单调 resourceVersion、乐观并发、删除中拒绝、终结器移除/清除、带版本条件的删除 |
| `pkg/storage/batch.go` | 删除预检（只读）与批量删除：逐对象判定、按名字稳定排序、幂等重复提交 |
| `pkg/controller/queue.go` | 两层工作队列（延迟堆 + 合并 FIFO）：按键单飞去重、`AddAfter` 退避、关停快进 |
| `pkg/controller/external.go` | 模拟外部系统：幂等 `EnsureAttach`/`Detach`，真实副作用计数，可预置瞬时失败 |
| `pkg/controller/controller.go` | 对账循环：finalizer 装配/摘除、状态聚合、指数退避重试、信号后排空 |
| `pkg/controller/delete.go` | 控制器侧删除入口：版本条件删除/批量删除 + 为被接受删除自动提交删除事件 |
| `cmd/demo/main.go` | 可运行演示（含 SIGINT/SIGTERM 优雅关停、预检与批量删除演示） |

## 状态流转

```
                         Create
                           │
                           ▼
                  ┌─────────────────┐
        Update ──▶│   Progressing   │（初始相位；尚未对账成功）
                  └────────┬────────┘
             reconcile 成功│（加 finalizer → 幂等 EnsureAttach → 写 Ready 状态）
                           ▼
                  ┌─────────────────┐
   重复 Upsert 事件│      Ready      │◀── 已是 Ready 且 ObservedRV=当前RV：
        （无写入）│ finalizer 存在  │     不再产生任何外部/存储副作用
                  └────────┬────────┘
                    Delete │（带 finalizer：设置 deletionTimestamp，不清除对象）
                           ▼
                  ┌─────────────────┐
                  │   Terminating   │── 仍可 Get；普通 Update 一律被拒（ErrDeleting）
                  └────────┬────────┘
       reconcile 删除路径  │（幂等 Detach → RemoveFinalizer）
                           ▼
                     对象被清除（最后一个 finalizer 移除时立即 purge → Get 返回 ErrNotFound）
```

- 任意失败的对账把聚合相位写为 `Degraded`（尽力而为；若期间被并发改动则放弃本次状态写，
  按新版本重试），成功后恢复 `Ready`。
- `Delete` 时**没有 finalizer** 的对象立即物理删除（与 K8s 一致）。
- 对外相位通过 `Widget.AggregatedPhase()` 汇总：只要 `deletionTimestamp != ""` 即为
  `Terminating`，普通状态更新无法把删除中的对象“恢复”为正常状态（存储层直接拒绝）。

## 并发与写入语义（乐观并发控制）

- 每次被接受的写操作都从存储的单调计数器领取一个新的 `resourceVersion`；对象经
  `Get`/`List` 返回的是深拷贝，调用方持有的副本不影响存储内部状态。
- `Update(name, expectedRV, mutate)` 与 `RemoveFinalizer(name, expectedRV, ...)` 必须携带
  读取时的版本；仅当版本完全一致才提交。`mutate` 在私有拷贝上执行：返回 error 或
  `panic` 都不会改动任何已有状态。
- 拒绝判定顺序（原因彼此可区分，统一用 `errors.Is` 判别）：

| 场景 | 返回原因 | 状态是否改变 |
| --- | --- | --- |
| 对象不存在（Update/Delete/RemoveFinalizer） | `storage.ErrNotFound` | 否 |
| Create 名字已存在且存活 | `storage.ErrAlreadyExists` | 否 |
| Create 命中删除中的同名对象 / Update 删除中对象 / 重复 Delete | `storage.ErrDeleting` | 否 |
| 携带版本与当前版本不一致 | `storage.ErrVersionConflict` | 否 |
| 移除一个并不存在的 finalizer | `storage.ErrFinalizerNotFound` | 否 |
| `mutate` 自身返回错误 | 该错误原样返回 | 否 |

## 带版本条件的删除（DeleteWithVersion）

`Delete(name)` 是无条件删除；`DeleteWithVersion(name, expectedRV)` 是乐观并发版本：
调用方把**读取时拿到的 resourceVersion** 一起提交，仅当该版本仍与存储一致时才接受删除。

- **被接受时**与 `Delete` 语义完全一致：带 finalizer 的对象进入可读的 `Terminating`
  （版本前进一步），无 finalizer 的对象立即清除。
- **被拒绝时不留任何痕迹**：不设置 `deletionTimestamp`、不消耗新版本号、对象保持可写。
  三种拒绝彼此可区分（判定顺序：不存在 → 删除中 → 版本过期）：

| 拒绝场景 | 返回原因 |
| --- | --- |
| 对象根本不存在 | `storage.ErrNotFound` |
| 对象已在删除流程中（即使携带的版本也过期） | `storage.ErrDeleting` |
| 读取后被其它写入改过（版本不一致） | `storage.ErrVersionConflict` |

- 多个调用方拿着**同一份读取结果**并发删除同一对象时，最多一个成功；失败者拿到
  `ErrVersionConflict` / `ErrDeleting`（对象已被清除时可能是 `ErrNotFound`），均可区分。
- 控制器入口 `Controller.DeleteWithVersion` 在删除被接受且对象进入 `Terminating` 时自动
  提交删除事件，由对账循环收敛到最终清除；被拒绝的删除不产生事件。

## 删除预检与批量删除

- `CheckDeleteBatch(intents)`：**只读预检**。对一组 `{Name, ResourceVersion}` 意图在同一
  快照下给出逐对象判定（`Ready` / `NotFound` / `AlreadyDeleting` / `VersionConflict`），
  不改动任何状态、不消耗版本号。预检之后若发生并发写入，真正执行时仍以执行时刻的状态
  为准（可能从 `Ready` 变为拒绝）。
- `DeleteBatch(intents)`：**批量执行**。每个意图独立判定、互不影响——某个对象被并发改动
  只会改变它自己的结果；每个对象都有明确的 `DeleteResult`（成功为 `Terminating` 或
  `Purged`，失败带可区分的 `Reason`），不会整批静默跳过，也不只回一个笼统错误。
- 结果**按名字稳定排序**；同一组输入在状态不变时重复执行得到一致结论。重复提交已被接受
  的删除会得到 `AlreadyDeleting` / `NotFound`，不会重复删除、不会把版本号推进两次。
- 控制器入口 `Controller.DeleteBatch` / `Controller.CheckDeleteBatch` 行为相同，并为每个
  进入 `Terminating` 的对象自动提交删除事件，让已接受的删除经对账循环收敛到清除。

### 兼容范围

本轮全部为**新增 API**，既有行为不变：`Create` / `Update` / `Delete` / `RemoveFinalizer` /
`Get` / `List` 的语义、错误集合与判定顺序保持原样，原有对账、退避、关停排空逻辑不受影响。
`DeleteWithVersion` 与 `Delete` 共享同一份删除语义（仅多了版本前置校验），预检/批量接口
只是其组合与只读视图；新旧删除方式可以混用——例如无条件 `Delete` 进入 `Terminating` 的
对象，在批量预检中同样报告 `AlreadyDeleting`。

## 对账、去重与退避重试

- **同一对象任一时刻只有一个对账**：工作队列对 key 做合并——待处理中重复 `Add`
  折叠为一项；正在处理时到达的事件只记一次，`Done` 后至多再排一次（见
  `TestSingleInFlightReconcilePerObject`：100 个重复事件，最大并发对账=1）。
- **重复事件无重复副作用**：`EnsureAttach` 仅在附件不存在时真正创建，`Detach` 仅在
  附件存在时真正删除，均计入 `OpCount`。已收敛对象的重复对账不写状态、不 bump 版本。
- **失败重试**：对账失败后按 `BaseDelay << 失败次数`（2 倍指数，封顶 `MaxDelay`）
  `AddAfter` 重排队；每次重试都打印
  `name=… attempt=… RETRY after=… reason=…`，策略完全可观测。成功后失败计数归零。
- **删除对账**：幂等摘除外部附件后移除受管 finalizer；遇到版本冲突以新版本重新排队，
  `ErrNotFound` / finalizer 已被并发移除都按 no-op 处理，重复终结不会重复 Detach。

## 关停与“已接受事件不丢失”

`Submit` 与 `Run` 的约定：

- 事件一旦被 `Submit` 接受（追加到事件日志并入队），就保证会被处理。
- 收到退出信号（`ctx` 取消 / SIGINT / SIGTERM）后：
  1. 控制器置关闭位，此后 `Submit` 返回 `false`（调用方可明确观测“未接受”，不会被静默丢弃）；
  2. `Shutdown()` 把堆中**所有延迟项立即快进**到就绪队列（退避不再阻塞退出）；
  3. worker 排空队列；排空期间失败按 `MaxAttempts` 上限重试（超过打印
     `GIVE-UP reason=max-attempts-during-drain`），保证永久失败也不会卡住退出；
  4. 排空受 `drainTimeout` 约束，超时返回错误并报告剩余条目数。

对应测试：`TestShutdownDrainsAcceptedEvents`（150 个事件全部收敛）、
`TestShutdownDrainBoundedRetriesWithFastForward`（1 小时退避在关停时快进且重试有界）。

## 本地验证

要求 Go 1.25+（仅使用标准库，无第三方依赖）。

```bash
# 全量测试（竞态检测；重复多次验证可复现）
go test ./... -race -count=3

# 查看带版本/事件/判定依据的单测日志
go test ./... -race -v -count=1

# 端到端演示：瞬时失败退避、finalizer 删除、三类拒绝、预检与批量删除、信号排空
go run ./cmd/demo

# 可选参数
go run ./cmd/demo -flaky 3 -drain 5s -show-batch=true
```

演示输出中可以观察到：`RETRY after=100ms/200ms…` 的退避序列、`finalizer-added →
attached → status-ready` 的正常流转、`decision=detached → finalizer-removed object-purged`
的删除流转、三类写入拒绝（`resource version conflict (stale)` / `object not found` /
`object is being deleted`），以及预检/批量删除的逐对象判定（`precheck name=… outcome=…`
与 `execute name=… outcome=…`，含 `Ready→Terminating`、`VersionConflict`、`NotFound`）。

## 测试与场景对照

| 需求场景 | 测试 |
| --- | --- |
| 并发写入冲突，仅一个成功且原因可区分 | `storage.TestConcurrentWritesOnlyOneWins` |
| 版本过期/对象不存在/删除中拒绝互不混淆 | `storage.TestFinalizerDeletionLifecycle`、`controller.TestDeletionReasonsAreDistinguishable` |
| 被拒绝写入不改动状态 | `storage.TestUpdateOptimisticConcurrency`、`TestUpdateMissingAndMutatorError` |
| 带版本条件的删除：接受语义 + 三类拒绝不留痕迹 | `storage.TestDeleteWithVersionAccepted`、`TestDeleteWithVersionRejectionsLeaveNoTrace`、`TestDeleteWithVersionStaleAfterConcurrentWrite` |
| 同一读取结果并发删除，最多一个成功 | `storage.TestConcurrentDeletesOnlyOneWins`、`controller.TestConcurrentVersionedDeletesViaController` |
| 删除预检只读、逐对象判定、按名字排序 | `storage.TestCheckDeleteBatchIsReadOnlyAndSorted` |
| 批量删除与预检一致、逐对象独立、并发互不影响 | `storage.TestDeleteBatchMatchesPrecheck`、`TestDeleteBatchConcurrentPerObjectIndependence`、`controller.TestBatchDeleteViaController` |
| 批量重复提交幂等、结论确定、版本不重复推进 | `storage.TestDeleteBatchRepeatSubmissionIsIdempotent`、`TestDeleteBatchDeterministicConclusions` |
| 版本条件删除经对账收敛到清除、待终结不可恢复 | `controller.TestVersionedDeleteConvergesToPurge`、`TestTerminatingObjectNotRestoredByReconcile`、`TestVersionedDeleteStaleRejectedObjectStaysReady` |
| 同对象单飞、重复事件不重复副作用 | `controller.TestSingleInFlightReconcilePerObject`、`TestDuplicateEventsNoDuplicateSideEffects` |
| 失败按退避策略重试直至成功 | `controller.TestFailureRetryWithObservableBackoff` |
| 终结器删除：待终结可读、不可恢复、清除即删除 | `controller.TestDeletionFlowWithFinalizer` |
| 信号后已接受事件不丢失、延迟快进、重试有界 | `controller.TestShutdown*`、`controller.TestQueueDelayedRequeueAndFastForward` |
