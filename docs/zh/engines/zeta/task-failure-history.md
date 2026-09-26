# 任务失败历史设计

本文档提出 [GH-11667](https://github.com/apache/seatunnel/issues/11667) 的第一版后端契约。规范的 STIP 讨论位于 [STIP-33](https://github.com/apache/seatunnel/issues/11735)。目前仅为设计，不代表 API 已经实现。

自 `e944f1e3` 以来的变更：

- attempt key 的来源、唯一写入方以及各类上报的解析方式改为表格（规范规则 R1 和 R2）；
- 每种被丢弃操作留下的结果改为表格（R3），顺序表述只覆盖已被队列接受的操作；
- 每个历史 map 以一行列出其写入方、隔离条件、过期、清理负责方和持久化（R4）；
- REST 解析顺序改为处理作业状态缺失情况的决策表（R5）；
- 同一作业的条目创建和接管先于任何捕获，包括 `JobMaster.init` 期间的 master 恢复捕获（“所有权”一节和 R4）；
- 序列化兼容 fixture 作为第一个实现切片交付，早于任何字段的增加（“兼容性”）。

## 问题

当前 Job Detail 页面只展示一条异常文本。当 pipeline 多次恢复，或者同一个作业中的不同 task group 发生失败时，这些信息不足以定位问题。运维人员需要在不搜索每个 worker 日志的情况下，看到哪个 pipeline attempt 和 task group 失败以及失败时间。第一版按 pipeline 和 task group 归因，不按 worker 地址归因。

当前引擎状态有三个相关限制：

- `PhysicalPlan` 只保留 sub-plan 上报的第一条错误；
- `TaskExecutionState` 携带格式化后的异常文本，但没有持久化的失败标识；
- finished job 历史只保存最终错误文本，不保存到达终态前的失败序列。

## 范围

第一阶段实现提供有界的、作业级别的失败历史，并通过同一个 REST 契约查询运行中和已完成作业。

它：

- 按 pipeline 执行 attempt 对失败进行分组；
- 标识上报失败的 pipeline，以及涉及的 task group；
- 记录不属于 task group 失败的 pipeline 级失败，例如 checkpoint 失败和资源分配失败；
- 在信息可用时保留 task 元数据，但不将 worker 地址存储为失败历史归因字段；
- 以结构化方式保留时间、消息、堆栈和异常类型，不解析展示文本；
- 在 active master 切换和 pipeline 恢复后继续保留；
- 随现有 finished job 历史策略过期；
- 为兼容性保留现有单一 `errorMsg` 字段。

第一阶段不增加日志聚合、分布式追踪或无界异常归档。Web UI 在后端契约确定后单独实现。

与 GH-11667 相比，第一版做出以下明确取舍：

- 按照本设计的安全评审，不按主机或 worker 地址归因；
- 从记录跳转到 worker 日志依赖 [#11662](https://github.com/apache/seatunnel/issues/11662) 以及另行商定的逻辑 worker 标识；
- attempt 已运行的时长由 `attemptStartedAt` 推导，失败发生时间为 `timestamp`。

## Attempt 模型

Attempt 属于 pipeline，而不是单个 task。

`SubPlan.pipelineRestoreNum` 参与 `job.retry.times` 判断。它只保存在内存中，新的 active master 重建 `SubPlan` 时会从 `0` 开始。将其用于历史记录，要么会在 failover 后丢失 attempt，要么在持久化后改变重试预算。因此失败历史使用独立的诊断标识。`canRestorePipeline()` 和 `job.retry.times` 从不读取它，任何恢复步骤都不会等待历史操作。恢复资格、恢复时机和可用性保持不变。

### Attempt key

一次 pipeline 执行的持久化标识称为 attempt key，即该 pipeline 在 `runningJobStateTimestampsIMap` 中的 `CREATED` 时间戳。当前 `dev` 只在两处写入该槽位：

- `SubPlan` 构造函数首次创建 pipeline 状态时；
- 恢复前的 `SubPlan.resetPipelineState()`，先写时间戳，再将 pipeline 状态设为 `CREATED`。

构造函数会保留已有值，因此新的 active master 重建 `SubPlan` 时，仍在运行的执行保持原有 key。

key 总是在写入或部署的时刻被复制并存入失败历史，之后绝不再从 `runningJobStateTimestampsIMap` 读取，因为每次恢复都会覆盖该槽位。

- `SubPlan` 在内存中保存当前 key：构造函数从已持久化的值设置，`resetPipelineState()` 以最后一次成功写入的值设置；
- 写入 key 时如果时间戳数组不存在，则 key 未知，此时捕获的记录 `attempt` 和 `attemptStartedAt` 均为 null。

为使 key 唯一且有序，`resetPipelineState()` 写入 `max(now, previousCreated + 1)` 而不是 `now`，其中 `previousCreated` 读取自同一个已持久化的时间戳数组。因此每个 pipeline 的 key 严格递增，包括在时钟不一致的不同 active master 之间。

该值是公开的：自 #11982 起，`/job-info` 以 `diagnostics.pipelines[].stateTimestamps.CREATED` 返回它。只有当一次重置与上一次落在同一毫秒内，或时钟回拨之后，该值才会与墙钟时间不同，此时它最多比 pipeline 的 `SCHEDULED` 时间戳晚一个回拨幅度。`rest-api-v2` 文档中该字段的说明将注明 `CREATED` 在多次恢复之间严格递增。这是本设计对现有值的唯一改动。

### 部署标识

`PhysicalVertex` 部署 task group 时，记录该次部署的 `(executionId, attemptKey)`。`executionId` 是 `getTaskGroupImmutableInformation()` 已经通过 flake ID 生成器为每次部署生成的 ID。vertex 保存当前部署和上一次部署；`reset()` 将当前部署移入上一次部署槽位。

这些配对保存在 master 内存中，因此新的 active master 会重建它们：新的 active master 上的 `JobMaster.init`（`restart = true`）重建计划时，`PhysicalVertex.initStateFuture` 在检查 task group 是否仍在执行之前，为每个状态为 `DEPLOYING`、`RUNNING`、`FAILING` 或 `CANCELING` 的 vertex，在 `SubPlan` 当前 key 下设置一个 `executionId` 未知的占位部署。同一 master 上恢复 pipeline 时也会运行 `initStateFuture`，此时不设置占位部署，它引起的转换也不带失败上下文，因此不产生记录。在 failover 中存活的 task group 仍在 worker 上执行，worker 会跳过重新部署（`TaskExecutionService`），并继续上报旧 master 分配的 `executionId`；新 master 不认识该 ID，于是归入占位部署。

### Attempt 编号

每个 pipeline 在历史条目中有一张 attempt 表和一个下一编号计数器。attempt 表将每个 attempt key 映射到其 attempt 编号和被抑制的失败数，每个 pipeline 最多 256 个 key；表满时淘汰最小的 key，淘汰不会改变计数器。attempt 表独立于记录淘汰。

条目创建以及 failover 后的接管都会登记每个 pipeline 的当前 key。每条记录在与追加相同的原子更新中解析其 key：

- 已在表中的 key 使用其编号；
- 大于表中所有 key 的 key 取计数器当前值，计数器随之递增；
- 不在表中且小于表中最大 key 的 key 来自较晚到达或已被淘汰的旧执行，以 `attempt = null` 保存，并保留其 `attemptStartedAt`；已有编号永不改变。

`resetPipelineState()` 之后，master 还会为新 key 提交一次尽力而为的 `registerAttempt`，使没有记录任何失败的执行也获得编号。如果该操作被丢弃，下一次有记录的执行取下一个编号。因此 `attempt` 是失败历史所观察到的执行序号，`attemptStartedAt` 才是精确标识。

这是 attempt 递增的对账形式：每条记录都携带自己的 key，因此正确分组从不依赖于递增在恢复后的执行失败之前完成。重试，或早于登记到达的记录，都解析为同一个编号。

### 上报归因

- worker 上报通过 `TaskExecutionState` 上新增的可为空字段，携带产生该上报的部署的 `executionId`；
- 如果该 ID 与 vertex 的当前或上一次部署匹配，就归入该部署的 key；
- vertex 不认识的 ID 在存在占位部署时归入占位部署，否则来自更早的部署，从历史中丢弃并记录 WARN 日志；
- 不带 `executionId` 的上报使用 vertex 的当前部署，包括节点丢失、master 恢复以及早于该字段的 worker 的上报；
- 在部署标识产生之前就失败的部署使用 `SubPlan` 的当前 key；
- 没有当前部署的 vertex（例如已重置为 `CREATED` 但尚未重新部署）不产生 task group 记录。

在当前 `dev` 上，引擎本身会将迟到的 `FAILED` 上报接受到已重置为 `CREATED` 的 vertex 中，因为 `updateTaskState` 只拒绝离开终态的转换。`executionId` 归因会将这类上报保留在产生它的执行之下。让引擎忽略过期上报是另一项修复，不在本设计范围内。

### 崩溃窗口

下表遵循当前 `dev`。新的 active master 只有在恢复的作业离开 pending 队列后才执行 `restorePipelineState()`：它取消并重新调度状态低于 `RUNNING` 的 pipeline；以 `FAILED` 或 `CANCELED` 结束的 pipeline，只有在新 master 上 `canRestorePipeline()` 成立时才会恢复，而此时内存中的重试计数器已从 `0` 重新开始。

| Active master 失败时机 | 新 master 看到的持久化状态 | `dev` 上的引擎行为 | 诊断结果 |
|---|---|---|---|
| `resetPipelineState()` 写入状态之前 | `FAILED` 或 `CANCELED`；`CREATED` 槽位可能已有一个未使用的 key | 若 `canRestorePipeline()` 成立则再次恢复，写入新 key | 未使用的 key 从未部署；接管时可能登记它，否则不获得编号 |
| 重置之后、任何部署之前 | `CREATED`，key 为 `K1` | 取消，然后以 `K2` 恢复 | `K1` 在接管时登记；`K2` 取下一个编号 |
| 调度或部署期间 | `SCHEDULED` 或 `DEPLOYING`，key 为 `K1` | 取消，然后以 `K2` 恢复 | 部署失败仍属于 `K1`；`K2` 取下一个编号 |
| `RUNNING` 且所有 task group 存活 | `RUNNING`，key 为 `K` | 继续运行；重建的 `SubPlan` 保留 `K` | 没有新 key；失败通过占位部署解析到 `K` |
| `RUNNING` 且部分 task group 被判定丢失 | `RUNNING`，key 为 `K` | `initStateFuture` 将这些 task group 标记为 `FAILING`；pipeline 失败并可能恢复 | 每个 task group 在 `K` 下获得一条无异常文本的 `MASTER_RECOVERY` 记录。该检查在任何 `ExecutionException` 时都会判定 task group 丢失，因此记录可能是现有检查的误判 |
| `FAILING` 或 `CANCELING` 期间 | `FAILING` 或 `CANCELING`，key 为 `K` | 取消剩余 task，结束 pipeline，并可能恢复 | worker 上报通过占位部署解析到 `K` |
| 恢复的作业在 pending 队列中等待 | 任意 | 在此窗口上报的 worker 收到 `JobNotFoundException` 并停止重试 | 这些上报会丢失。这是现有引擎行为；master 从未收到的内容无法被记录 |

### 隔离

当前 `dev` 没有持久化的 master epoch；`resetPipelineState()` 和 `updatePipelineState()` 写入 `runningJobStateIMap` 时不检查所有者。失败历史依赖与其所描述的 pipeline 状态相同的单一 active master 假设，不声称更强的过期 master 隔离。在该假设下：

- key 严格递增且已有编号永不改变，旧 master 的迟到操作不会打乱 attempt 顺序，最坏情况只是保存一条 `attempt = null` 的记录；
- 历史消费线程在每次操作前检查本节点仍是 active master；`clearCoordinatorService()` 丢弃其队列及内存中的合并状态；
- 来自上一个作业实例或终态之后的写入，会被“存储与保留”中的所有者和终态检查拒绝。

出现第二个 active master 的脑裂情况不在第一版保证范围内，这与现有作业状态一致。

## 失败记录

建议的 REST 响应为：

```json
{
  "jobId": "123456789",
  "attempts": [
    {
      "pipelineId": 1,
      "attempt": 2,
      "attemptStartedAt": 1753574380000,
      "suppressedCount": 0
    }
  ],
  "failures": [
    {
      "sequence": 7,
      "timestamp": 1753574400000,
      "jobId": "123456789",
      "pipelineId": 1,
      "attempt": 2,
      "attemptStartedAt": 1753574380000,
      "scope": "TASK_GROUP",
      "source": "WORKER",
      "taskGroupId": 4,
      "taskId": null,
      "taskName": "mysql-source -> transform",
      "exceptionType": "java.sql.SQLException",
      "exceptionFingerprint": "5f3a9c1e20b47d86",
      "message": "Connection reset",
      "messageOriginalBytes": 16,
      "messageTruncated": false,
      "stackTrace": "java.sql.SQLException: Connection reset\n...",
      "stackTraceOriginalBytes": 43,
      "stackTraceTruncated": false
    }
  ]
}
```

`attempts` 按存储内容列出每个 pipeline 的 attempt 表，包括没有保留失败的 attempt。`suppressedCount` 是该 attempt 中未被单独记录的失败数（见“捕获与去重”）。

失败记录字段规则：

- `scope` 为 `TASK_GROUP` 或 `PIPELINE`；
- `source` 标识捕获路径：`TASK_GROUP` 为 `WORKER`、`NODE_LOSS`、`DEPLOY` 或 `MASTER_RECOVERY`；`PIPELINE` 为 `CHECKPOINT`、`RESOURCE` 或 `ENGINE`；
- `TASK_GROUP` 记录包含 `taskGroupId`，`PIPELINE` 记录中为 null；
- `attempt` 是 Attempt 模型中的 attempt 编号；`attemptStartedAt` 是 attempt key，即引擎为该次执行创建或重置 pipeline 的时间；二者仅在 Attempt 模型所述情况下为 null；
- `timestamp` 是 active master 接受该失败时的时间；attempt 已运行的时长为 `timestamp - attemptStartedAt`；由于 master 切换前后时钟可能不同，Web UI 将负值时长显示为 `0`；
- `messageTruncated` 和 `stackTraceTruncated` 是必填布尔值；`messageOriginalBytes` 和 `stackTraceOriginalBytes` 是脱敏后、截断前的 UTF-8 字节长度，定义见下文“文本处理”；文本不可用时为 null；不持久化脱敏前的原始长度；
- `exceptionType` 和 `exceptionFingerprint` 来自结构化失败传输，均不通过解析格式化文本推断。指纹为 16 个十六进制字符（64 位）的哈希，输入为根因的类名以及其前五个栈帧各自的声明类名和方法名；不包含消息、行号，也不使用在 JDK 8 与 JDK 11 上格式不同的 `StackTraceElement.toString()`；生成的 lambda 类后缀会被规范化。没有 `Throwable` 的路径（例如 `NODE_LOSS` 和 `MASTER_RECOVERY`）指纹为 null。指纹用于在 Web UI 中归并重复原因，不是安全标识；
- `message` 的 UTF-8 存储上限为 4 KiB，`stackTrace` 为 64 KiB。截断必须保持有效 UTF-8。截断后的消息保留前缀；截断后的堆栈同时保留开头和结尾，以保留异常本身和最深层原因。

字段分为两组：

| 分组 | 字段 | 保证 |
|---|---|---|
| 稳定契约 | `sequence`、`timestamp`、`jobId`、`pipelineId`、`attempt`、`attemptStartedAt`、`scope`、`source`、`taskGroupId`、`messageTruncated`、`stackTraceTruncated`，以及 `attempts` 的全部字段 | 始终存在且含义如文档所述；`attempt`、`attemptStartedAt` 和 `taskGroupId` 仅在上述情况下可为 null |
| 尽力而为的遥测 | `taskId`、`taskName`、`exceptionType`、`exceptionFingerprint`、`message`、`messageOriginalBytes`、`stackTrace`、`stackTraceOriginalBytes` | 捕获路径提供时存在；旧路径或合成路径可能为 null |

第一版在运行中历史、已完成历史和 REST 响应中都没有 worker 地址归因字段。

## 捕获与去重

捕获发生在引擎自身的状态转换内部，因此先于该转换可能触发的任何 pipeline 重置。

- **task group 失败（`WORKER`、`NODE_LOSS`、`MASTER_RECOVERY`）。** 在 `PhysicalVertex.updateTaskState` 的同一个同步转换中，写入 `FAILED` 或 `FAILING` 状态之后、`stateProcess()` 和 task future 完成之前进行捕获。
  - `updateStateByExecutionService` 将上报中的结构化字段和 `executionId` 传入该转换；
  - 节点丢失（`CoordinatorService.makeTasksFailed`）和 master 恢复（`JobMaster.init` 以 `restart = true` 运行期间的 `initStateFuture`）使用同一个转换，不带 `executionId`；
  - pipeline 结束回调在作业执行器上异步运行，此时捕获已经提交，因此被接受的捕获排在重置提交的所有操作之前；被拒绝的捕获按 R3 处理。
- **部署失败（`DEPLOY`）。** `PhysicalVertex` 中的部署失败路径（`deploy` 和 `deployOnRemote`）向 `makeTaskGroupFailing` 传入 `DEPLOY` 失败上下文，并在 `FAILING` 转换被接受之后捕获。
  - `TaskDeployState.failed(Throwable)` 在 `Throwable` 可用处以字符串形式携带原始失败的类名、消息、堆栈和指纹；在 worker 上，这发生在响应跨越 Hazelcast RPC 边界之前；
  - master 端的失败将 `ExecutionException` 和 `CompletionException` 解包为原始原因。实时 `Throwable` 永不跨越该边界，因此 master 不需要连接器特定的异常类；
  - 该记录的 `exceptionType` 标识原始原因，而不是 `TaskGroupDeployException`。
- **pipeline 失败（`CHECKPOINT`、`RESOURCE`、`ENGINE`）。**
  - `CHECKPOINT` 在 `CheckpointCoordinator.handleCoordinatorError` 中、coordinator 首次进入 `FAILED` 时捕获，此时 `Throwable` 可用且 pipeline 尚未被取消；
  - `RESOURCE` 在 `SCHEDULED` 状态下资源分配失败时捕获；
  - `ENGINE` 取自其余 `makePipelineFailing` 原因（状态更新和恢复错误）。
- 每个捕获点都传入标明其 `source` 的失败上下文；没有失败上下文的 `FAILING` 或 `FAILED` 转换不产生记录，包括取消以及 `updateTaskState` 内部的错误处理。

捕获运行在 Hazelcast 操作线程、成员事件线程、作业执行器、failover 后恢复作业的线程以及 checkpoint coordinator 线程上。它只做常量级工作加一次有界复制：先检查每个 attempt 的合并上限，超过上限后不再处理任何文本；每个文本字段最多复制前 256 KiB；然后调用非阻塞的 `offer`。脱敏、截断和序列化在下文的消费线程上执行，绝不在捕获线程上执行。

去重 key：

- `TASK_GROUP`：`(pipelineId, attemptKey, taskGroupId)`；
- `PIPELINE`：`(pipelineId, attemptKey, source)`。

第一次投递创建记录并获得作业级 `sequence`；之后相同 key 的投递被忽略，且不消耗 sequence。已保留的记录就是有界的 key 集合，因此记录被淘汰后，延迟到达的重复投递可能再次被记录。恢复后的失败具有不同的 attempt key，保持为单独的记录。

master 会合并失败风暴：每个 pipeline attempt 最多提交 20 条记录；该 attempt 中更多的失败只在 master 内存中递增计数，计数总值以绝对值随该 pipeline 的下一次操作或作业的终态操作一起发送；历史为该 attempt 保留收到的最大总值，因此重试的操作不会重复计数。因此高并行度下的节点丢失，每个 attempt 最多产生 20 次记录操作。该计数是尽力而为的，failover 时会丢失。

历史记录属于尽力而为的诊断能力：

- 每个 active master 的 `CoordinatorService` 拥有一个有界 FIFO 队列和一个专用消费线程；
- 队列最多容纳 1,000 个操作、16 MiB 已捕获文本，且每个作业最多 100 个排队操作；超过任一上限的 `offer` 被丢弃；
- 消费线程在 Hazelcast 操作线程和作业调度执行器之外运行每个 `EntryProcessor`；严格按顺序处理队列：只对 Hazelcast 判定为可重试的失败，在取下一个操作之前原地重试当前操作，最多三次，然后丢弃该操作；
- 完成处理从不改变 task 或 pipeline 状态，从不回调 `JobMaster` 或 `SubPlan`，也从不重新进入失败或恢复处理；
- task 失败、恢复决策或恢复后的执行都不依赖任何失败历史操作。

每个被丢弃或失败的历史操作都以 WARN 级别记录日志，并按作业限流。日志行只包含 `jobId`、`pipelineId`、attempt key、`taskGroupId` 或 source、异常类名以及固定的原因代码，从不包含消息、堆栈或 task 名称文本。历史值、记录和操作类重写 `toString()`，不输出这些文本。

### 文本处理

文本按以下固定顺序处理：

1. 捕获端每个字段最多保留前 256 KiB。该上限只去掉后缀，因此可能丢掉秘密的值，但绝不会暴露缺少键的值。worker 在发送新的结构化字段之前应用同样的上限；
2. 消费线程将截取后的整段文本作为一个字符串进行脱敏，包括多行模式；
3. 消费线程根据脱敏后的文本记录 `*OriginalBytes`；
4. 消费线程在 UTF-8 边界（堆栈则在行边界）将脱敏后的文本截断到上述上限，并明确标记省略的范围。

第 2 步之前不对文本做任何截断、开窗或切片。之后的再次截断（例如已完成快照的上限）作用于已脱敏的文本。master 会对从 worker 收到的所有结构化文本进行脱敏，从不依赖 worker 已经脱敏。脱敏是幂等的。

## 存储与保留

失败历史使用两个独立的 Hazelcast map：`engine_runningJobFailureHistory` 和 `engine_finishedJobFailureHistory`，均以 `jobId` 为 key。

- 两个 map 使用与现有作业状态 map 相同的默认 Hazelcast `MapConfig`，本设计不增加备份、持久化或外部历史后端；
- `FileMapStore.init` 已将 `engine_runningJobMetrics` 排除在 IMap 存储之外；它以同样方式排除这两个历史 map，即使运维人员为 `map.engine*` 配置了 `map-store`；
- 这使大体积的诊断值不会写入 HDFS 或 S3 IMap 存储，也不会在分区线程上同步写穿；同时保证 TTL 过期有效，因为 `MapStore` 中的条目在过期时既不会被删除，也不会带着 TTL 重新加载；
- 因此失败历史在整个集群重启后不会保留；这种重启后从 IMap 存储恢复的作业没有失败历史条目；
- REST 表示与该存储选择无关。

### 所有权

每个运行中条目和每个已完成快照都以 `JobInfo` 中的 `(jobId, initializationTimestamp)` 作为所有者。当前 `dev` 使用同一标识判断清理所有权（`isCleanupOwnedByCurrentJob` 和 `JobCleanupRecord.ownerInitializationTimestamp`）。`JobInfo` 和 `JobCleanupRecord` 都不变。

- 新提交（包括复用 job ID 的 savepoint 启动）的 `JobMaster` 初始化会提交一个创建操作；其他操作都不会创建条目；
- `JobMaster.init` 在 `PlanUtils.fromLogicalDAG` 返回后立即提交创建操作（failover 后为接管操作），早于 `initCheckPointManager` 和 `initStateFuture`，因为后者在新的 active master 上可能捕获 `MASTER_RECOVERY` 失败。在当前 `dev` 上，`submitJob` 和 `restoreJobFromMasterActiveSwitch` 都在作业进入 pending 队列之前调用 `init`，并且在作业离开该队列之前不会运行其他捕获点。因此被接受的创建或接管操作排在该作业所有捕获之前。如果它被拒绝，该作业之后的操作以 `NO_ENTRY` 或 `OWNER_MISMATCH` 被拒绝（R3）；
- 消费线程按顺序执行创建操作：如果运行中条目属于其他所有者且尚未完成终结，先按“终态处理”对其终结；然后创建新条目并登记每个 pipeline 的当前 key。如果终结失败，仍创建新条目，并记录上一个作业实例历史丢失的日志。创建操作从不删除其他所有者的已完成快照；新所有者结束后，其快照会替换它（R4）；
- active master 恢复时，只有当现有条目的所有者与恢复的 `JobInfo` 匹配且条目未进入终态时才接管，接管时登记每个 pipeline 的当前 key；
- 没有匹配条目时，例如功能部署之前启动的作业或创建操作被丢弃的作业，该作业不记录失败历史，REST 返回已知作业的空列表；
- 追加和 attempt 操作从不创建条目。

所有者标识的唯一性仅取决于 `initializationTimestamp`。发生冲突需要 savepoint 启动的初始化恰好落在上一个作业实例的同一毫秒，这需要时钟回拨。现有清理所有权具有相同的限制，本设计没有扩大它。

### 上限

运行中历史对每个作业保留：

- 最多 100 条失败记录；
- `message`、`stackTrace`、`taskName` 和 `exceptionType` 合计最多 1 MiB 的 UTF-8 文本；每个 `taskName` 和 `exceptionType` 最多 1 KiB，并同样应用尽力而为的脱敏；
- attempt 表，每个 pipeline 最多 256 个 key，每个 key 仅占少量字节。

超过上限时，从最旧的记录开始淘汰，直到记录数和文本总量都满足限制；每个 pipeline attempt 的第一条记录（通常是根因）在所有其他记录之后才会被淘汰。作业运行期间不设置 TTL。

已完成快照保留 attempt 表和全部已保留记录，每条堆栈最多保留 16 KiB（开头和结尾），作业文本总量最多 256 KiB，淘汰顺序相同，并保留截断标记和原始字节长度。

最坏情况下的集群堆内存为：

`(运行中作业数 × 1 MiB + history-job-expire-minutes 内结束的作业数 × 256 KiB) × (1 + 备份数) + 每个 active master 16 MiB 的排队文本`

在默认值（24 小时、1 个备份）下，每天 1,000 个失败作业的已完成快照约占 0.5 GiB。需要硬性全局上限的运维人员可以为 `engine_finishedJobFailureHistory` 配置 Hazelcast 淘汰策略；快照被淘汰后，返回已知作业的空列表。

每次 `EntryProcessor` 操作都会在分区线程上反序列化并重新序列化该作业的值（最多约 1 MiB），备份也会重复一次；合并机制将其频率限制为每个 pipeline attempt 最多 20 次记录操作。

这些上限是常量，而不是用户选项。如果运行经验表明这些上限不足，后续可以增加配置项。

### 终态隔离

终态隔离由历史 `EntryProcessor` 实施。每个追加和 attempt 操作在与其修改相同的单 key 原子更新中检查：条目存在；条目所有者等于操作的 `(jobId, initializationTimestamp)`；条目未进入终态。否则操作不写入直接返回，因此不存在的条目永远不会被重建。

Processor 是确定性的：消费线程在首次提交操作前计算当前时间和 TTL，并在该操作的重试中复用相同的值，因此主副本和备份得到相同结果，重试也不会延长截止时间。

### 终态处理

终态处理包含三个幂等步骤：

1. **`markTerminal(owner, terminalTime, ttl)`**：当所有者匹配且条目未进入终态时，设置 `terminal = true`、保存 `terminalTime`，并通过 `ExtendedMapEntry.setValue(value, ttl, unit)` 设置条目 TTL，其中 `ttl = terminalTime + history-job-expire-minutes - now`；TTL 不为正时删除条目。已进入终态的条目原样返回且不调用 `setValue`，因为普通的 `setValue` 会清除 TTL；不调用它，任何重试或恢复都无法延长截止时间。该 processor 返回冻结后的记录；
2. **写入已完成快照**：使用冻结后的记录、所有者以及相同的剩余 TTL；该步骤为尽力而为；
3. **`removeIfOwner(owner)`**：仅当所有者匹配且条目已进入终态时删除条目。该步骤只在第 2 步成功或已找到同一所有者的快照之后运行；否则终态条目保留，直到下一次终结提交或清扫重试第 2 步，或直到其 TTL 到期。

所有终结都通过同一个 FIFO 队列，排在队列此前接受的所有操作之后，因此在终结之前被接受的捕获会先于条目进入终态被应用。被拒绝或重试耗尽的捕获不存在于历史中，并记录日志（R3）。终态隔离只依赖所有者、终态标记和 `terminalTime`，从不依赖之前的每条记录是否都已存储。调用方在提交之前确定 `terminalTime`。如果已存在同一所有者的已完成快照，终结会跳过第 2 步。

已完成快照保存其所有者、冻结后的记录和 attempt 表。只有在不存在快照，或现有快照所有者的 `initializationTimestamp` 更小时才写入，因此较早的作业实例永远不会覆盖较新的作业实例。

复用 job ID 的 savepoint 启动在结束后可能没有自己的快照，有两种情况：其创建操作被丢弃，或在其运行中条目过期之前每一次写入快照都失败。在这两种情况下，其 `JobInfo` 删除后，如果上一个作业实例的快照仍存在，REST 会返回该快照，直到其 TTL 到期。两种情况都会以 WARN 记录，并且是历史可能属于同一 job ID 的较早作业实例的仅有情况。

`JobMaster.cleanJob()` 在其他终态工作之前、在独立的 `try` 中首先提交终结。以下四处也会以尽力而为的方式提交终结：

- `processPendingJobCleanup`，使用 `ownerInitializationTimestamp`，在 `cleanupPendingJobStateMaps` 之前；
- `cleanupTerminalZombieJob`，使用 `JobInfo` 的初始化时间戳，在其状态 key 删除之前；
- `cleanupPendingJobStateForRestore`，在 savepoint 启动初始化新作业实例之前；
- 历史清扫。清扫每 60 秒在现有 `pipelineCleanupScheduler` 上运行，且只在 active master 上运行；它终结所有者在 `runningJobInfoIMap` 中没有匹配 `JobInfo` 的运行中条目，以及作业状态为终态的运行中条目；缺失的作业状态不是终态。所有者没有 `JobInfo`、未进入终态且其作业从未到达终态的条目是孤立条目，例如提交失败遗留的条目；对于孤立条目，清扫执行 `markTerminal` 和 `removeIfOwner` 而跳过第 2 步，二者都以所有者为隔离条件。其他所有终结都会写入快照，即使快照为空；每次运行在仍有此类条目时持续处理，时间预算为 5 秒。

终结失败或被丢弃时只记录日志，不向这些路径抛出异常；它不会使 `JobCleanupRecord` 保持待处理、延迟现有状态清理或导致 savepoint 启动提交失败。清扫会重试它。

`terminalTime` 按以下顺序确定，且所有读取都在任何状态 key 被删除之前完成：

1. `markTerminal` 保存的值；
2. `runningJobStateTimestampsIMap` 中作业的终态时间戳；
3. `engine_finishedJobState` 中的 `JobState.finishTime`；
4. 仅当以上都不存在时，使用终结时的时间，并记录日志。

`JobCleanupRecord` 不做扩展：增加第三个 key 集合会改变一个已有 HA 持久化实例的共享 `IdentifiedDataSerializable`；`IMAP_PENDING_JOB_CLEANUP` 每个 job ID 只保存一条记录；诊断失败还可能使现有状态清理保持待处理。

Master failover 会保留历史 map 已确认的记录，以及 sequence 和 attempt 表。active master 失败时仍在队列中的操作可能丢失。该诊断路径从不为了等待历史确认而延迟 task 失败或恢复决策。

## REST 契约

建议的端点为：

```text
GET /job-info/{jobId}/failures?limit=100
```

行为：

- `failures` 按 `sequence` 降序返回，最多 `limit` 条；`attempts` 不受 `limit` 限制；
- `limit` 默认值为 100，更大的值被限制为 100；
- 运行中和已完成作业使用同一个响应模型；
- 响应通过流式 JSON writer 写出，其大小受 JSON 转义后的已存储文本上限约束。

端点使用“规范规则”中的决策表 R5 解析作业。终态作业从不读取运行中记录，包括在 `JobInfo` 仍存在的 `state-cleanup-delay-ms` 窗口内。复用 job ID 的 savepoint 启动运行期间，R5 只返回所有者与其 `JobInfo` 匹配的记录和快照；结束后的情况见“终态处理”中说明的限制。

`JobInfoServlet` 当前将解码后的完整路径作为一个数字 job ID 解析，同时处理已弃用的 `/running-job/*` 别名。Jetty 的解码路径会将 `%2F` 转为 `/`、去掉 `;` 参数并解析点路径段，因此新路由从不基于它进行路由。failures 路由的匹配规则如下：

- 处理器读取 `getRequestURI()`，要求前缀 `getContextPath() + getServletPath()` 按字面匹配；
- 其余部分必须完全匹配 `^/([0-9]{1,19})/failures$`：只允许 ASCII 数字，`failures` 区分大小写；
- 包含 `%`、`;`、`\`、空路径段、`.` 或 `..` 路径段，或以斜杠结尾的 URI 返回 `404`；
- job ID 必须能解析为正的有符号 64 位值，否则返回 `400`；
- 只有匹配成功的路径才读取 `limit`：必须匹配 `^[0-9]{1,10}$` 且为正数；空的或重复的 `limit` 返回 `400`，未知查询参数被忽略；
- 该路由只处理 `GET`，其他方法由处理器自身返回 `405`。

单 ID 路由 `/job-info/{jobId}` 和 `/running-job/{jobId}` 保持现有行为，包括非法 ID 的现有 `400` 响应。这些映射下的其他路径形状，包括 `/running-job/{jobId}/failures` 和 `/failures` 后的额外路径段，都返回 `404`。禁止前缀或子字符串匹配。这明确将非法多段路径从当前数字解析的 `400` 改为 `404`。

failures 路由自行写出所有非 200 响应：

- 使用 `setStatus` 和固定的 JSON 响应体，例如 `{"status":"fail","message":"Not found"}`；
- 从不调用 `sendError`，因为 Jetty 默认错误页会回显 URI，并可能包含堆栈；
- 从不根据请求输入或异常文本构造响应体；
- 捕获 map 读取中的 `RuntimeException`，返回固定的 `503` 响应体，详情只写入服务端日志；
- 响应设置 `Content-Type: application/json; charset=UTF-8` 和 `X-Content-Type-Options: nosniff`。

当前 `/job-info/{jobId}` 的行为及其 `errorMsg` 字段保持不变，包括未知作业的响应。新端点定义自己的 `404`，使调用方能够区分未知作业和没有失败记录的已知作业。该端点会写入 `rest-api-v2` 的中英文文档。

## 安全与输入校验

**认证。** 该端点位于现有 `JobInfoServlet` 映射下，因此继承引擎 REST API 的 `BasicAuthFilter` 边界，不新增端点专用的认证机制。

- REST 认证默认关闭（`enable-basic-auth: false`），而 HTTP 默认在 8080 端口开启；默认情况下，能访问该端口的任何人都可以读取失败历史；
- 现有边界是一个没有角色的共享凭据，不提供按作业或租户的授权；需要更细粒度访问控制的部署必须在网关或网络边界实施；
- 该路由直接处理请求，不使用 async 或 forward 分发，因为过滤器只注册在 `REQUEST` 分发上。

**其他访问路径。** 脱敏是纵深防御，而不是访问边界。同样的失败信息仍可通过以下途径以未脱敏形式获得：

- `/job-info/{jobId}` 现有的 `errorMsg` 字段；
- 节点日志端点；
- 任何能访问 Hazelcast 成员端口的客户端。开源版 Hazelcast 没有成员或客户端认证，此类客户端可以读取所有引擎 map，包括失败历史。成员端口必须处于可信网络中。

随发行包提供的 `hazelcast.yaml` 已启用 `DATA` 端点组，因此启用 Hazelcast 内置 REST API 会在 `BasicAuthFilter` 之外暴露 map 值。允许读取失败历史的人，也必须被信任可以访问这些路径。

异常文本还可能包含记录值和远端服务的响应，脱敏并不针对这些内容；失败历史在作业结束后最多保留 `history-job-expire-minutes`。引擎安全文档将列出新端点和这些注意事项。

**脱敏。** 尽力而为的脱敏按“文本处理”中定义的顺序，在每一次写入运行中 map 或已完成快照之前执行，覆盖 worker 上报、部署、节点丢失、master 恢复、pipeline 和终态快照路径。实现将 `DryRunConnectFailureMessageSanitizer` 的模式提取为共享工具，`redact` 与 `truncate` 分为两个函数，并扩展如下：

- **敏感键名**：在由 `[A-Za-z0-9._-]` 组成的键中任意位置匹配以下词，支持 snake、kebab、点分和驼峰写法，键可以带引号：
  - `password`、`passwd`、`pwd`、`passphrase`；
  - `secret`、`client secret`；
  - `token`、`session token`、`security token`、`auth token`；
  - `credential`、`signature`；
  - `private key`、`access key`、`account key`、`shared access key`、`api key`；
  - `cookie`、`jaas`。

  例如 `db_password`、`ssl.keystore.password`、`fs.s3a.secret.key`、`fs.azure.account.key.*`、`aws_secret_access_key`、`accessKeySecret` 和 `SharedAccessKey`。
- **值**：`=` 或 `:` 之后的值，可以是带转义引号的双引号字符串、单引号字符串、花括号值，或到空白、`&`、`,`、`}`、`)` 为止的内容。
- **整体掩码**：
  - `sasl.jaas.config` 的完整值，以及 JAAS 文本中单独出现的 `password="..."`；
  - 连接字符串（Azure、ODBC、SQL Server）中的敏感 `Key=Value;`；
  - JDBC URL，只保留 `jdbc:<subprotocol>:`。
- **URL 与 HTTP**：
  - URL 用户信息（`scheme://user:pass@host`）；
  - 敏感 URL 查询参数，包括 `sig`、`signature`、`key`、`apikey`、`access_token`、`refresh_token`、`id_token`、`code` 和 `X-Amz-*`；
  - `Authorization`、`Proxy-Authorization`、`Cookie`、`Set-Cookie` 和 `X-Api-Key` 头的值；
  - 单独出现的 `Bearer` 和 `Basic` 凭据。
- **PEM 私钥**：支持真实换行和 JSON 转义换行；未闭合的块掩码到文本末尾。
- **令牌格式**：以固定前缀锚定，包括 JWT、AWS access key ID、Google API key、GitHub token 和 Slack token。

每个模式都以字面量锚定，且没有嵌套的无界量词。性能测试限制每个模式在对抗性的 1 MiB 输入上的执行时间。

明确不做的内容：通用高熵检测、百分号编码的秘密、SQL 字面量中的秘密、行数据、嵌入在 URL 路径中的秘密，以及 URL 之外的用户名或主机名。现有作业的 `errorMsg` 存储和 REST 行为不变。

**Worker 地址。** 第一版既不在运行中或已完成失败历史中存储 worker 的 `host:port` 归因字段，也不在 REST 响应中暴露该字段。捕获代码可以查询现有执行元数据，但不能将其中的地址复制到历史字段中。这并不保证删除已脱敏异常文本中提及的所有地址。现有执行元数据和 `/pending-jobs` 行为保持不变。

## Web UI 后续工作

Exception tab 可以在单独变更中接入 REST 端点。第一版 UI：

- 按 pipeline 和 attempt 分组展示失败，并利用 `attempts` 展示没有失败的 attempt 和被抑制的数量；
- 展示时间、pipeline、task group 或 pipeline source、task 名称、异常类型和消息；
- 将 `exceptionFingerprint` 相同的记录标记为跨 attempt 重复出现；
- 展示每个 attempt 失败时已运行的时长；
- 展示截断标记，堆栈默认折叠。

UI 仅以纯文本渲染 `message`、`stackTrace`、`taskName` 和 `exceptionType`，不解释 HTML，也不将异常文本中的 URL 转为链接。不得推断或暴露 API 已省略的 worker 地址。缺失的可选字段显示为不可用。当引擎只提供 task group 级失败时，UI 不应宣称具有 task 级精度。日志链接等待 #11662 和逻辑 worker 标识。

## 兼容性

对于有效的现有 job-detail 请求，该功能纯属新增。非法多段路径从数字解析的 `400` 改为上述 `404`：

- 现有作业不需要修改配置；
- 现有 REST 字段和最终 `errorMsg` 仍然可用；
- checkpoint 和 savepoint 负载、`JobInfo` 以及 `JobCleanupRecord` 保持不变；
- `job.retry.times`、恢复资格和恢复可用性保持不变；
- 对现有值的唯一改动见“Attempt key”：`/job-info` diagnostics 返回的 pipeline `CREATED` 时间戳在多次恢复之间严格递增；
- 旧的失败路径只填充其已知字段。

有两个 Java 序列化类跨越 worker 与 master 边界并新增字段，二者当前都未声明 `serialVersionUID`：

- `TaskExecutionState` 由 `NotifyTaskStatusOperation` 通过 `writeObject` 写出，生成值为 `-108652017022658969L`，源码自 2023 年 3 月以来未变；
- `TaskDeployState`（Lombok `@Data` 类）是 `DeployTaskOperation` 的响应，其生成值为 `2646079648150626562L`（基于 Lombok 生成后的类计算），源码自 2023 年 3 月以来未变。

两个值均在 JDK 8 和 JDK 11 上用 `serialver` 计算。实现先声明这两个确切的值再增加字段，保留现有字段的名称和类型，只增加可为空的字段：`Long` 类型的 `executionId` 和 `String` 类型的结构化失败字段。序列化形式中不出现新类，因此旧的读取方永远不需要它没有的类。

根据 Java 序列化的兼容变更规则，新的读取方将缺失字段设为 `null`，旧的读取方忽略未知字段，因此新旧 worker 与 master 可以互通；旧 worker 的上报只是没有 `executionId`。第一个实现切片在增加任何字段之前固定这两个值，并在 `org.apache.seatunnel.engine.server.serializable` 测试包中增加 `TaskStateSerializationTest`：

- fixture 是固定的 `TaskExecutionState` 和 `TaskDeployState` 值的 Java 序列化字节，由当前 `dev` 上未修改的类写出；JDK 8 和 JDK 11 写出的字节完全相同；
- 测试断言：
  - fixture 能被固定 UID 后的类逐字段反序列化；
  - 固定 UID 后的类写出与 fixture 完全相同的字节；
  - 每个声明的 UID 等于此前生成的值。

增加可选字段的切片保留这些 fixture，并增加以下测试：新类读取旧字节时新字段为 `null`；新类写出的字节能被隔离类加载器中的未修改类定义读取。

历史值及其 `EntryProcessor` 类是新类型。在没有这些类的成员上，历史操作会失败并按尽力而为原则丢弃。本设计不承诺在混合版本滚动升级期间提供失败历史。

## 规范规则

下列表格是上文规则的精确形式。正文与表格不一致时，以表格为准。

### R1. Attempt key 的来源和写入方

| 时机 | 复制的值 | 写入方 | 保存为 |
|---|---|---|---|
| 首次创建 pipeline 状态 | pipeline 状态不存在时，`SubPlan` 构造函数以 `System.currentTimeMillis()` 写入的 `CREATED` 槽位。保存 `initializationTimestamp` 的 `INITIALIZING` 槽位不是 key | active master 的 `SubPlan` 构造函数 | `SubPlan.currentKey`，由创建操作登记 |
| 新 active master 上的构造函数，状态已存在 | 已持久化的 `CREATED` 值，不做修改；该槽位为 null 时 key 未知 | 无，只读取 | `SubPlan.currentKey`，由接管操作登记 |
| 恢复 | `max(now, previousCreated + 1)`，`previousCreated` 在同一次写入执行中从已持久化的时间戳数组读取；该槽位为 null 时取 `now` | 仅 active master 的 `SubPlan`；`resetPipelineState()` 只在 `SubPlan` 的 `synchronized reset()` 中、持有 `restoreLock` 时运行 | `SubPlan.currentKey` 取自未抛出异常而返回的那次写入执行所写的值。`RetryUtils` 重试时会重新执行整个写入，因此每次重试都根据已持久化的值重新计算。随后提交尽力而为的 `registerAttempt` |
| 部署 task group | `getTaskGroupImmutableInformation()` 生成 `executionId` 时的 `SubPlan.currentKey` | active master 的 `PhysicalVertex` | vertex 的当前部署 `(executionId, key)` |
| `JobMaster.init` 以 `restart = true` 运行期间的 `PhysicalVertex.initStateFuture`，vertex 处于 `DEPLOYING`、`RUNNING`、`FAILING` 或 `CANCELING` | 存活检查之前的 `SubPlan.currentKey` | 新 active master 的 `PhysicalVertex` | 占位部署 `(unknown, key)` |
| 时间戳数组不存在 | 无 | 无 | key 未知：`attempt` 和 `attemptStartedAt` 为 null |

每个新 key 都由已持久化的上一个 key 推导而来，因此没有任何规则比较不同 master 的时钟。唯一的顺序比较是“大于 attempt 表中的所有 key”，而 key 按构造方式递增。

### R2. 上报解析

| 上报 | 解析出的 key | 结果 |
|---|---|---|
| `executionId` 等于 vertex 的当前部署 | 该部署的 key | 记录，受去重约束 |
| `executionId` 等于 vertex 的上一次部署 | 该部署的 key | 记录在上一个 attempt 下 |
| `executionId` 未知，且 vertex 的任一槽位中有占位部署 | 占位部署的 key | 记录。来自比占位部署所代表的执行更早的部署的上报无法区分，也归入占位部署 |
| `executionId` 未知，且没有占位部署 | 无 | 不记录：以原因 `STALE_EXECUTION` 经单作业限流器记录 WARN |
| 没有 `executionId`（节点丢失、master 恢复、旧 worker），且存在当前部署或占位部署 | 其 key | 记录 |
| 没有 `executionId`，且两者都不存在 | 无 | 不产生 task group 记录 |
| 在 `executionId` 产生之前失败的部署 | `SubPlan.currentKey` | `DEPLOY` 记录 |
| pipeline 失败（`CHECKPOINT`、`RESOURCE`、`ENGINE`） | 捕获时的 `SubPlan.currentKey` | `PIPELINE` 记录 |
| 与已保留记录相同的 `(pipelineId, key, taskGroupId)` 或 `(pipelineId, key, source)` | 相同的 key | 不产生效果，不消耗 sequence |

### R3. 被丢弃的操作

以下情况视为操作被丢弃：其 `offer` 被拒绝（队列已满、作业份额用尽或本节点已不是 active master）；原地重试三次后仍失败；或被隔离条件拒绝。隔离条件的拒绝包括：

- `NO_ENTRY`：不存在条目；
- `OWNER_MISMATCH`：条目属于其他所有者；
- `TERMINAL`：条目已进入终态，包括预期中的迟到写入。

每个被丢弃或被拒绝的操作都经单作业限流器以 WARN 记录，只包含标识和原因代码。历史不统计丢弃次数，也不声称完整。每种丢弃留下的结果如下：

| 被丢弃的操作 | 历史的表现 | REST 的表现 | `/job-info` diagnostics |
|---|---|---|---|
| 创建 | 没有条目；该作业之后的操作以 `NO_ENTRY` 被拒绝，或在其他所有者的条目仍存在时以 `OWNER_MISMATCH` 被拒绝 | 运行中的已知作业：`failures` 为空；结束后：其 `JobInfo` 存在期间为空列表，之后为上一个作业实例的快照（如仍存在，见“终态处理”） | 不变；`JobRuntimeDiagnostics` 从不读取失败历史 |
| 接管（failover 后） | 条目保留其记录，但当前 key 要等到其记录到达时才登记 | 已有记录；attempt 编号可能跳号 | 不变 |
| `registerAttempt` | 只有当该执行的某条记录在更大的 key 登记之前到达时，该执行才获得编号 | 没有失败的 attempt 可能不出现在 `attempts` 中 | 不变 |
| 追加 | 该失败不存在；其他失败（包括同一 task group 之后的失败）仍会被记录 | 缺少该失败 | 不变 |
| 携带被抑制总数的追加 | `suppressedCount` 停留在最后收到的总数 | `suppressedCount` 偏低 | 不变 |
| 终结 | 运行中条目保持存活，直到另一次终结提交或清扫生效；终态时间仍按“终态处理”中列出的来源确定 | 终态作业：终结生效后出现其快照，此前返回空列表 | 不变 |

恢复、task 失败处理和现有作业清理从不等待该队列，丢弃也从不改变它们的结果。

### R4. 历史 map 和内存状态

| 状态 | Key | 创建方 | 修改方 | 隔离条件 | 过期 | 清理负责方 | IMap 存储 |
|---|---|---|---|---|---|---|---|
| `engine_runningJobFailureHistory` | `jobId` | 消费线程执行的创建操作，由 `JobMaster.init` 在构建计划后立即提交 | 接管、追加、`registerAttempt`、`markTerminal`、`removeIfOwner` | 创建：先终结其他所有者尚未终结的条目再替换；接管：所有者匹配且未进入终态；追加和登记：条目存在、所有者匹配、未进入终态；`markTerminal`：所有者匹配；`removeIfOwner`：所有者匹配且已进入终态 | 存活期间无；`markTerminal` 之后为 `terminalTime + history-job-expire-minutes` | 消费线程的终结，由 `cleanJob`、`processPendingJobCleanup`、`cleanupTerminalZombieJob`、`cleanupPendingJobStateForRestore` 和清扫提交 | 由 `FileMapStore` 排除 |
| `engine_finishedJobFailureHistory` | `jobId` | 终结第 2 步 | 写入后不再修改 | 仅当不存在快照，或现有快照所有者的 `initializationTimestamp` 更小时写入 | 写入时设置为 `terminalTime + history-job-expire-minutes`；运维人员可增加淘汰策略 | 其 TTL | 由 `FileMapStore` 排除 |
| 操作队列、合并计数、单作业 WARN 限流器 | 每个 master；按作业 | `CoordinatorService` | 捕获、消费线程 | 消费线程在每次操作前检查自己仍是 active master | 作业的终结提交后删除其计数和限流器状态；清扫也会为没有 `JobInfo` 的作业删除它们 | `clearCoordinatorService()` 丢弃全部内容 | 仅在内存中 |
| 部署槽位和占位部署 | 每个 `PhysicalVertex` | 部署、`init` 以 `restart = true` 运行期间的 `initStateFuture` | 部署、`reset()` | 不需要；仅在本 master 内 | 随 `JobMaster` | 随 `JobMaster` 丢弃 | 仅在内存中 |

“终态处理”中定义的孤立条目被删除，不写入快照，例如 `init` 在提交创建操作后失败的提交所留下的条目。其他所有终结都会写入快照（即使快照为空），并且只在写入之后才删除运行中条目。

### R5. REST 解析

从上到下依次检查。先检查作业状态是否缺失，再调用 `isEndState()`。缺失的状态从不视为运行中。在当前 `dev` 上，它出现在两个窗口中：

- `submitJob` 在 `init` 写入作业状态之前先保存 `JobInfo`；
- `cleanupTerminalZombieJob` 和 `cleanupPendingJobStateForRestore` 先删除状态 key，再删除 `JobInfo`。

| `JobInfo` | `runningJobStateIMap` 中的作业状态 | 运行中条目 | 已完成快照 | 响应 |
|---|---|---|---|---|
| 存在 | 存在且不是终态 | 所有者与 `JobInfo` 匹配且未进入终态 | 任意 | 运行中记录 |
| 存在 | 存在且不是终态 | 不存在、属于其他所有者或已进入终态 | 任意 | 空列表 |
| 存在 | 终态或不存在 | 任意 | 所有者与 `JobInfo` 匹配 | 该快照 |
| 存在 | 终态或不存在 | 任意 | 不存在或属于其他所有者 | 空列表 |
| 不存在 | 任意 | 任意 | 存在 | 该快照 |
| 不存在 | 任意 | 任意 | 不存在，但存在已完成作业状态 | 空列表 |
| 不存在 | 任意 | 任意 | 不存在，且没有已完成作业状态 | `404` |

残留的运行中条目绝不会让未知或已过期的作业被误判为已知。

## 验收标准

1. 首个 attempt 中的 task group 失败创建一条 attempt 为 `0` 的 `TASK_GROUP` 记录。
2. 恢复后执行中的失败创建一条使用下一个 attempt、`attemptStartedAt` 更晚的记录；即使恢复后的执行在其尽力而为的登记生效之前失败、登记被丢弃，或 pipeline 曾经在没有任何失败记录的情况下恢复过一次，也成立。
3. 同一终态的重复投递在原记录保留期间只创建一条记录；记录被淘汰后，延迟重复可能再次被记录；并发重复只分配一个 sequence。
4. 同一 attempt 中不同 task group 的失败保持为单独记录。
5. 携带上一次部署 `executionId` 的迟到 `FAILED` 上报，记录在上一次 attempt key 下，而不是记录在恢复后的执行下。
6. active master 切换后，存活 task group 的上报（包括携带旧 master 分配的 `executionId` 的上报）通过占位部署记录在当前 key 下。
7. 节点丢失失败、master 恢复时丢失的 task group 和部署失败（包括在部署标识产生之前失败的部署），各自创建一条带有对应 `source` 的有界记录；部署记录的 `exceptionType` 标识原始原因，而不是 `TaskGroupDeployException`；`updateTaskState` 内部的状态更新错误不会被记录为 `DEPLOY`。
8. checkpoint coordinator 失败（包括取消期间 task 也上报 `FAILED` 的情况）、资源分配失败和引擎恢复错误，各自创建一条 `taskGroupId` 为 null 的 `PIPELINE` 记录。
9. 失败 task group 的捕获在其 pipeline 可能被重置之前提交；使用单 task group pipeline 测试，其结束回调与操作线程竞争。
10. 在崩溃窗口表每一行对应的时机发生 active master 切换时，attempt 编号符合表中描述；重试资格和恢复时机保持不变。
11. key 不在 attempt 表中且小于表中最大 key 的记录以 `attempt = null` 保存；attempt 表淘汰最小的 key，且从不改变已有编号或下一编号计数器。
12. `resetPipelineState()` 写入严格大于上一个值的 `CREATED` 时间戳，包括时钟回拨和写入重试的情况；内存中的 key 等于未抛出异常而返回的那次写入执行所写的值；`rest-api-v2` 文档相应说明该字段。
13. `attemptStartedAt` 始终等于在重置或部署时复制的 attempt key，在之后的恢复发生后绝不从 `runningJobStateTimestampsIMap` 读取。
14. 超过 100 条记录或保留文本超过 1 MiB 时，从最旧的记录开始淘汰，直到两个上限都满足；每个 attempt 的第一条记录最后淘汰。
15. 脱敏先于任何截断，包括 worker 端的上限；跨越截断边界的秘密不会被存储。超过 4 KiB 的消息和超过 64 KiB 的堆栈在有效 UTF-8 边界截断，并在运行中和已完成历史中提供截断标记以及脱敏后、截断前的字节长度。
16. 已完成快照记录其所有者，每条堆栈最多保留 16 KiB，文本总量最多 256 KiB，并保留标记和长度；其过期时间不晚于终态时间之后 `history-job-expire-minutes`。
17. 同一 pipeline attempt 中超过 20 个失败时，最多提交 20 次记录操作，其余数量通过 `attempts[].suppressedCount` 报告；重试的操作不会重复计数。
18. 队列已满，以及失败、重试和延迟的历史操作，不会阻塞捕获线程，不会重新进入失败或恢复处理，也不会改变失败、恢复或终态结果；捕获不执行正则处理，队列保持在数量、字节和单作业上限之内。
19. 失败历史产生的 WARN 或 ERROR 日志行不包含消息、堆栈或 task 名称文本；被丢弃记录中的标记秘密不会出现在任何日志行中。
20. 追加和 attempt 操作从不创建条目；在 `markTerminal` 或删除之后它们不产生任何效果，也从不移除或延长终态 TTL；在终结之前被队列接受的捕获会在条目进入终态之前被应用。
21. 终结之后不存在运行中条目，包括 active master 切换、terminal-zombie 恢复、复用 job ID 的 savepoint 启动以及清扫发现的孤立条目；终结失败由清扫重试，且不会使 `JobCleanupRecord` 保持待处理、延迟现有状态清理或导致 savepoint 启动提交失败。
22. 复用 job ID 的 savepoint 启动，在创建自己的条目之前，先终结上一个作业实例尚未终结的条目；上一个所有者的在途写入不会改变新条目。
23. 终态时间按规定锚定，恢复或清理重试从不延长截止时间；`markTerminal` 之后被提升的备份保留过期时间。
24. 为 `map.engine*` 配置 `map-store` 时，两个历史 map 都不会写入 IMap 存储。
25. REST：运行中作业返回运行中记录；终态作业只返回自己的已完成快照，包括在 `state-cleanup-delay-ms` 期间以及状态 key 删除之后；运行中的复用 job ID 的 savepoint 启动绝不返回上一个作业实例的历史，已结束的只在“终态处理”所述的两种情况下返回；没有记录的已知作业（包括快照写入失败或被淘汰后）返回空列表；未知或已过期作业即使存在残留条目也返回 `404`。
26. 只有符合规定语法的原始 URI 才提供失败历史；旧单 ID 路由保持现有行为，包括非法 ID 的 `400` 响应；别名失败历史请求、额外路径段、结尾斜杠、`%` 编码字符、`;` 参数、点路径段和空路径段返回受控的 `404`。
27. 该路由的每个非 200 响应都由处理器设置固定 JSON 响应体，不包含请求输入或堆栈；使用 URI 中的唯一标记以及消息携带标记的 map 读取失败进行测试；没有响应来自 `sendError` 或 Jetty 错误页。
28. 在新路由上，缺失、空、重复、零、负数、非数字、Unicode 数字、带符号和溢出的 limit，以及溢出的 job ID，返回规定的响应。
29. 该端点使用与现有 job-detail 端点相同的已配置 REST 认证边界；任何历史记录或响应都不包含 worker 地址归因字段。
30. “安全”中列出的每个脱敏项都有正向和近似反例测试，包括跨截断边界的秘密、PEM 块、转义引号以及带和不带前缀的 JAAS 文本；每个模式在对抗性的 1 MiB 输入上在固定时间预算内完成，且脱敏是幂等的。
31. `TaskExecutionState` 和 `TaskDeployState` 的 UID 保护测试和字节 fixture 测试通过：第一个切片断言字节完全相同，增加字段的切片再增加新旧两个方向的测试；现有 `errorMsg` 客户端不受影响。
32. 现有 `job.retry.times`、恢复资格和恢复调度从不读取或等待失败历史状态，包括 active master 切换之后。
33. 英文和中文文档描述相同的契约。
34. R2 的每一行都有测试，包括有占位部署和没有占位部署时的未知 `executionId`，以及不消耗 sequence 的重复投递。
35. R3 的每一行都有测试，强制触发丢弃或拒绝（队列已满、单作业份额用尽、重试耗尽、`NO_ENTRY`、`OWNER_MISMATCH`、`TERMINAL`），断言历史和 REST 结果符合表格、WARN 原因代码正确、`/job-info` diagnostics 不变，恢复和清理时机不受影响。
36. 已完成快照从不替换所有者 `initializationTimestamp` 更大的快照；只有“终态处理”中定义的孤立条目在删除时不写入快照；没有失败而结束的 savepoint 启动会得到自己的空快照；运行中条目只在其快照写入之后才被删除。
37. R4 和 R5 的每一行都有测试，包括 `JobInfo` 存在而作业状态缺失的情况，以及创建操作尚未执行的 savepoint 启动。

## 交付计划

1. 在 STIP-33 中就本契约达成一致。
2. 结构化失败传输：固定两个序列化 UID，增加可为空的 `executionId` 和失败字段，按 vertex 跟踪部署标识（包括 failover 后的占位部署），使 `CREATED` 时间戳严格递增并更新其 `rest-api-v2` 说明，同时增加兼容性测试。
3. 历史存储：两个 map、`FileMapStore` 排除、`EntryProcessor`、所有权、上限、终态隔离与终结以及清扫，并增加 failover 和清理测试。
4. 捕获：捕获点、消费队列、合并、文本处理和共享脱敏工具，并增加捕获顺序、崩溃窗口、脱敏和日志测试。
5. REST：路由和端点，包括认证边界、URI 语法、错误响应体、校验、兼容性以及运行中/已完成作业测试，并补充 `rest-api-v2` 和引擎安全文档的中英文内容。
6. 在单独的 pull request 中实现 Web UI 历史视图。
