# 任务失败历史设计

本文档提出 [GH-11667](https://github.com/apache/seatunnel/issues/11667) 的第一版后端契约。目前仅为设计，不代表 API 已经实现。

## 问题

当前 Job Detail 页面只展示一条异常文本。当 pipeline 多次恢复，或者同一个作业中的不同 task group 发生失败时，这些信息不足以定位问题。运维人员需要在不搜索每个 worker 日志的情况下，看到哪个 pipeline attempt 和 task group 失败以及失败时间。第一版按 pipeline 和 task group 归因，不按 worker 地址归因。

当前引擎状态有三个相关限制：

- `PhysicalPlan` 只保留 sub-plan 上报的第一条错误；
- `TaskExecutionState` 携带格式化后的异常文本，但没有持久化的失败标识；
- finished job 历史只保存最终错误文本，不保存到达终态前的失败序列。

## 范围

第一阶段实现应提供有界的、作业级别的失败历史，并通过同一个 REST 契约查询运行中和已完成作业。

它应当：

- 按 pipeline 执行 attempt 对失败进行分组；
- 标识上报失败的 pipeline 和 task group；
- 在信息可用时保留 task 元数据，但不将 worker 地址存储为失败历史归因字段；
- 以结构化方式保留时间、消息、堆栈和异常类型，不解析展示文本；
- 在 master 切换和 pipeline 恢复后继续保留；
- 随现有 finished job 历史策略过期；
- 为兼容性保留现有单一 `errorMsg` 字段。

第一阶段不增加日志聚合、分布式追踪或无界异常归档。Web UI 应在后端契约确定后单独实现。

## Attempt 模型

Attempt 属于 pipeline，而不是单个 task。

- pipeline 初始执行为 attempt `0`；
- 在调度恢复前递增 attempt；
- 同一次执行中捕获的所有失败使用相同的 attempt；
- 用于诊断的 attempt 标识必须写入 HA 状态，避免新的 active master 从 `0` 重新编号。

`SubPlan.pipelineRestoreNum` 当前参与 `job.retry.times` 判断。如果将它持久化并直接用于历史记录，也会使重试预算在 active master 切换后继续生效，这属于另一项行为变更。因此第一阶段实现必须保持现有重试计数器和重试上限语义不变。

失败历史在作业级 HA 状态中保存独立的、持久化的诊断 attempt 标识。初次执行创建 attempt `0`。每次恢复开始新的执行前，在历史状态中原子递增该 pipeline 的诊断 attempt 并记录开始时间。新的 active master 在记录下一次失败前读取该标识。这个计数器只用于失败关联和 REST 输出，不能参与恢复资格或重试上限判断。

attempt 递增与失败记录更新使用同一个作业级 `EntryProcessor` 串行化边界，因此不会覆盖同一作业中其他 pipeline 并发写入的记录。attempt 递增不同于尽力而为的失败记录写入，它属于恢复调度流程的一部分，必须在恢复后的执行启动前完成。如果写入失败，恢复调度需要重试或失败，不能使用旧的 attempt 标识启动新执行。

这样既保持与现有 pipeline 恢复边界一致，也不会改变现有重试语义。

## 失败记录

建议的 REST 表示如下：

```json
{
  "sequence": 7,
  "timestamp": 1753574400000,
  "jobId": "123456789",
  "pipelineId": 1,
  "attempt": 2,
  "attemptStartedAt": 1753574380000,
  "taskGroupId": 4,
  "taskId": null,
  "taskName": "mysql-source -> transform",
  "exceptionType": "java.sql.SQLException",
  "message": "Connection reset",
  "messageTruncated": false,
  "stackTrace": "java.sql.SQLException: Connection reset\n...",
  "stackTraceTruncated": false
}
```

字段规则：

- `sequence` 在单个作业内单调递增，在时间戳相同时提供确定顺序；
- `timestamp`、`jobId`、`pipelineId`、`attempt` 和 `taskGroupId` 为必填；
- `attemptStartedAt` 来自持久化的诊断 attempt 元数据。attempt `0` 在 pipeline 执行创建并准备部署时初始化；恢复 attempt 在调度恢复前递增诊断标识时初始化。对于无法解析该元数据的旧路径或合成路径，该字段可以为空。相同 `pipelineId` 和 `attempt` 的所有记录使用同一个值。它不同于 `timestamp`，后者表示单条失败被捕获的时间；
- `taskId`、`taskName`、`exceptionType`、`message` 和 `stackTrace` 为可选，因为旧路径或合成失败路径可能无法提供。第一版的运行中历史、已完成历史和 REST 响应均不包含 worker 地址归因字段；
- `messageTruncated` 和 `stackTraceTruncated` 为必填布尔值，用于说明对应内容是否在存储前被截断；
- `exceptionType` 必须来自结构化失败传输，不能通过解析格式化堆栈推断；
- `stackTrace` 保留诊断细节，`message` 用于简短展示。
- 存储的 UTF-8 内容中，`message` 上限为 4 KiB，`stackTrace` 上限为 64 KiB。截断必须保持 UTF-8 有效。截断消息保留开头，截断堆栈同时保留开头和结尾，以便保留异常信息和最深层 cause。

## 捕获与去重

`TaskExecutionState` 仍然是 worker 到 master 的结构化失败传输，但它不是 task group 失败的唯一路径。对于 worker 上报的终态失败，统一捕获点是 `PhysicalVertex` 在 `updateStateByExecutionService` 接受 `FAILED` 状态后的状态转换。该路径同时覆盖正常 worker 上报和直接路由到 physical vertex 的节点丢失状态更新。

部署失败没有 `TaskExecutionState`，而是通过 `makeTaskGroupFailing` 进入状态机。该路径必须使用部署异常以及已知的 pipeline 和 task group 元数据创建失败记录，不将 slot 或 worker 地址复制到历史记录中。`TaskDeployState.failed(Throwable)` 必须在原始 `Throwable` 仍然可用时，将失败类名、消息和有界堆栈提取为字符串。对于 `deployOnRemote`，这些字符串必须在 worker 端、响应跨越 Hazelcast RPC 边界之前完成提取；不能携带原始 `Throwable`，因为 master 端可能无法加载 connector 特有的异常类。失败记录直接读取这些结构化字段，因此 `exceptionType` 表示原始原因，而不是 `TaskGroupDeployException`。同一个去重键可以防止该 attempt 后续的终态上报生成重复记录。没有失败原因的取消不记录为异常。

异常内容必须在这些捕获边界完成脱敏和长度限制，再写入 HA 或 finished-job 历史。实现应将 `DryRunConnectFailureMessageSanitizer` 中的脱敏规则抽取为共享工具，不能持久化未经处理的 connector 消息或堆栈。失败历史仍使用自己的 4 KiB 消息上限、64 KiB 堆栈上限和截断标记，不能继承 dry-run 工具的 2 KiB 展示上限。

当原始记录仍在保留范围内时，重复收到同一个 task group 的终态不应生成重复记录。第一版使用 `(pipelineId, attempt, taskGroupId)` 去重，因为一个 task group 在一次 pipeline attempt 中只有一个终态失败。以作业级 HA 条目为目标的 Hazelcast `EntryProcessor` 在一次原子操作中完成去重检查、sequence 分配、追加记录和最旧记录删除。它必须从 task 状态操作路径异步提交。完成回调可以记录存储失败，但不能等待 Hazelcast operation thread，也不能重新进入该线程。第一次终态上报创建记录并分配 sequence，相同 key 的后续上报直接忽略且不消耗新的 sequence。同一 attempt 中不同 task group 的失败分别保留，恢复后的失败使用新的 attempt，因此仍然可见。

去重使用当前保留记录作为有界 key 集合。当一条记录因 100 条上限或 1 MiB 聚合文本上限被删除后，该 key 的延迟重复上报可能再次生成记录。第一版不会额外保存作业生命周期内所有历史 key 的无界集合。

历史记录属于尽力而为的诊断能力。存储失败需要记录日志，但不能阻塞原始失败处理或恢复决策。

## 存储与保留

失败历史应使用以 `jobId` 为 key 的独立 HA 引擎状态条目。第一版可以使用独立的 Hazelcast `IMap`，并采用与引擎现有作业状态 map 相同的默认 Hazelcast `MapConfig`。它不增加额外备份、持久化或外部历史后端。REST 表示不依赖具体的存储选择。

第一版使用以下边界：

- 每个作业最多保留 100 条失败记录；
- 每个作业保留的 UTF-8 `message` 和 `stackTrace` 内容总量最多为 1 MiB；
- 删除最旧记录，直到记录数量和文本总量两个限制都满足；
- 作业运行期间不设置 TTL；
- 作业进入终态后，为独立的 finished-history 条目设置自己的 `history-job-expire-minutes` TTL，并为仍然存在的运行中历史条目应用相同保留周期，作为清理兜底。

初始上限应使用常量，不新增用户配置。如果实际运行数据证明 100 条不足，可以后续增加可配置项。

作业进入终态时，`JobHistoryService` 将保留的失败记录和权威 pipeline attempt 值写入独立的 finished history 条目。该条目使用与对应 finished job 记录相同的 `history-job-expire-minutes` TTL，因此过期不依赖 cleanup listener 回调。listener 可以在 finished job 记录删除时提前清理，但条目自身的 TTL 仍是兜底保证。该方案复用现有 finished job 生命周期，不引入可插拔的历史存储抽象。

终态快照写入采用尽力而为语义。写入或清理失败必须记录日志，但不能改变作业终态、恢复行为或现有 finished job 记录。运行中和已完成作业虽然存储生命周期不同，但读取时使用同一个响应模型。

尝试写入 finished-history 快照后，必须通过现有持久化待清理机制（`JobCleanupRecord` / `IMAP_PENDING_JOB_CLEANUP`）删除运行中历史条目，即使快照写入失败也不例外。实现必须将新的历史条目纳入终态清理和 active master 切换后的 terminal-zombie 恢复；删除其他作业状态 key 时不能丢失待处理的历史清理。只有运行中历史条目已不存在时，才能完成该清理。删除或 TTL 设置失败需要记录日志并通过该机制重试，不能改变作业终态结果。

进入终态时，仍然存在的运行中历史条目必须设置基于作业终态时间和 `history-job-expire-minutes` 的兜底过期时间。恢复和清理重试必须保留该截止时间，不能延长保留期。终态隔离必须阻止排队中或延迟到达的失败记录及 attempt 更新重新创建已清理的运行中条目、移除其 TTL 或延长其过期时间。作业进入终态后，REST 只读取 finished-history 快照，不读取待清理条目。尽力而为的快照写入失败可能导致已知的已完成作业没有失败记录；不能通过无限期保留诊断数据来兜底。

master 切换会保留已经由 HA 历史存储确认的记录，并从持久化状态继续 sequence 和 attempt 编号。由于历史提交是异步且尽力而为的，active master 失效时仍在处理中的提交可能丢失。该诊断路径不会为了等待历史写入确认而延迟 task 失败或恢复决策。

## REST 契约

建议端点：

```text
GET /job-info/{jobId}/failures?limit=100
```

行为：

- 按 `sequence` 降序返回；
- `limit` 默认值为 100，非正数应被拒绝；
- 请求值不能超过保留上限；
- 已知但没有失败记录的作业返回空列表；
- 未知或已过期作业返回受控的 `404` 响应；
- 无论记录来自哪个独立状态条目，运行中和已完成作业都返回同一个响应模型。

`JobInfoServlet` 当前将全部路径信息作为一个数字 job ID 解析，同时处理已弃用的 `/running-job/*` 别名。实现必须区分 servlet 映射和精确路径段，或使用等效的独立处理器。只有 `/job-info/{jobId}/failures` 提供失败历史。单 ID 路由 `/job-info/{jobId}` 和 `/running-job/{jobId}` 保持现有行为，包括非法 ID 的现有 `400` 响应。这些映射下的其他路径形状，包括 `/running-job/{jobId}/failures` 和 `/failures` 后的额外路径段，返回受控的 `404`，不回显输入或暴露堆栈。禁止前缀或子字符串匹配。这明确将非法多段路径从当前数字解析的 `400` 改为 `404`，并不声称这种 not-found 行为已存在。

当前 `/job-info/{jobId}` 的行为及其 `errorMsg` 字段保持不变，包括未知作业的现有响应。新的失败历史端点定义明确的 `404` 响应，使调用方能够区分未知作业和没有失败记录的已知作业。

## 安全与输入校验

该端点沿用现有引擎 REST API 的 `BasicAuthFilter` 边界，不新增端点专用的认证机制。这是面向运维人员的端点：现有边界不提供按作业或租户的授权，已认证的 REST 用户可以读取作业诊断信息。需要更细粒度访问控制的部署必须在现有网关或网络边界实施限制。未启用 REST 认证时，能够访问该端点的调用方可以读取诊断信息；运维人员不能假定新路由会提供额外授权。即使凭据已脱敏，异常文本仍可能包含敏感的运行信息。

脱敏和保持有效 UTF-8 的截断必须在捕获时完成，早于每一次 HA IMap 或 finished-history 写入，而不能只在 REST 响应序列化时处理。该要求覆盖 worker 上报、部署失败、节点丢失和终态快照路径。这样 Hazelcast 状态、独立的 finished-history 条目和 API 响应都会保存同一份有界内容，也不能通过其他存储路径读取未经脱敏的数据。

新的失败历史路由处理器负责校验 `jobId` 和 `limit`；这些规则不改变旧单 ID 路由的校验：

- 非法或超出有符号 64 位范围的 job 标识，以及非数字、超出有符号 32 位范围或非正数的 limit 返回受控的 `400` 响应；
- 未提供 limit 时默认为 100；有效的正整数 limit 超过 100 时限制为 100；
- 校验失败不能包含堆栈，也不能通过共享异常处理器回显不可信输入。

第一版既不在运行中或已完成失败历史中持久化 worker 的 `host:port` 归因字段，也不在 REST 响应中暴露该字段。捕获代码可以查询现有执行元数据，但不能将其中的地址复制到历史字段中。这样可以避免为本版本没有消费者的拓扑数据增加保留；这并不保证删除已脱敏异常文本中提及的所有地址。现有执行元数据和 `/pending-jobs` 行为保持不变。仅限运维人员的显式开关或逻辑 worker 标识需要单独商定契约；本设计不新增角色体系或地址暴露选项。

## Web UI 后续工作

Exception tab 可以在单独变更中接入 REST 端点。第一版 UI 应按 attempt 分组，并展示时间、pipeline、task group、task 名称、异常类型和消息。不得推断或暴露 API 已省略的 worker 地址。堆栈默认折叠。

缺失的可选字段应显示为不可用。当引擎只提供 task group 级失败时，UI 不应宣称具有 task 级精度。

## 兼容性

对于有效的现有 job-detail 请求，该功能为增量功能。非法多段路径从数字解析的 `400` 改为上述受控 `404`：

- 现有作业不需要修改配置；
- 现有 REST 字段和最终错误消息继续保留；
- 不修改 checkpoint 或 savepoint 内容；
- 现有 `job.retry.times` 和 active master 切换后的重试行为保持不变；
- 旧失败路径只需要填写它能够提供的字段。

`TaskExecutionState` 在 worker 和 master 之间使用 Java 序列化。增加新的可选失败字段之前，实现必须记录当前类自动生成的 serial UID，并显式声明该值。保留这个 UID，同时将新字段视为可选字段，可以继续反序列化原有 wire 数据，避免无意改变兼容性。

## 验收标准

1. 首次执行的 task group 失败生成一条 attempt `0` 的记录；
2. pipeline 恢复后失败生成一条 attempt 递增的新记录；
3. 当原始记录仍在有界历史中时，重复发送同一个终态不会生成重复记录；记录被删除后，延迟重复上报可以再次生成记录；
4. 同一个 attempt 中不同 task group 的失败分别保留；
5. master 切换后，HA 历史存储已确认的记录和下一个 attempt 编号保持不变；切换时仍在处理中的异步提交可能丢失；
6. 成功写入的 finished-history 快照在配置的历史过期时间内提供保留记录；尽力而为的快照写入失败可能导致已知的已完成作业没有失败记录；
7. 超过 100 条记录，或者保留的 UTF-8 消息和堆栈内容超过 1 MiB 后，按确定顺序删除最旧记录，直到两个限制都满足；
8. 超过 4 KiB 的消息和超过 64 KiB 的堆栈在有效 UTF-8 边界截断，并提供对应的截断标记；
9. 作业进入终态时写入一个有界失败历史条目，该条目随对应的 finished job 记录过期；
10. 写入或清理失败历史时发生错误，不改变作业失败、恢复或终态流程；
11. 现有 job detail 客户端继续收到当前 `errorMsg` 字段。
12. 并发重复上报只生成一条记录，并通过作业条目的原子更新只分配一个 sequence；
13. 消息和堆栈中的敏感信息在写入 HA 和 finished history 前完成脱敏；
14. 在新的失败历史路由上，非法 `jobId` 或 `limit` 返回受控的 `400` 响应，不暴露堆栈或回显非法值；
15. 该端点使用与现有 job-detail 端点相同的 REST 认证边界。
16. 失败历史更新异步提交，不阻塞 Hazelcast operation thread；
17. 失败历史状态使用与现有作业状态 map 相同的默认 Hazelcast 配置，不增加备份或持久化；
18. 只有精确的 `/job-info/{jobId}/failures` 路由提供失败历史。旧单 ID 请求 `/job-info/{jobId}` 和 `/running-job/{jobId}` 保持现有行为，包括非法 ID 的 `400` 响应。其他路径形状，包括通过别名请求失败历史和额外路径段，返回受控 `404`，不回显输入或暴露堆栈。
19. 增加可选的结构化失败字段后，已有序列化 `TaskExecutionState` 仍可读取。
20. 通过 `makeTaskGroupFailing` 进入的部署失败即使没有 `TaskExecutionState`，也会创建一条有界记录；当原始 cause 可用时，`exceptionType` 表示原始原因而不是 `TaskGroupDeployException`。
21. 持久化诊断 attempt 标识不会改变 `job.retry.times`、恢复资格或 active master 切换后的重试行为。
22. 任何包含 `attemptStartedAt` 的记录都从该 pipeline attempt 创建时的持久化元数据读取该值。
23. 恢复后的执行只有在共享作业级条目中原子提交 attempt 递增后才能上报失败；递增写入失败时，不能使用上一个 attempt 标识启动执行。
24. 运行中历史条目、已完成历史快照和 REST 响应均不包含 worker 地址归因字段。启用或禁用 REST 认证不会额外提供按作业授权，也不会暴露地址开关字段；现有执行元数据保持不变。
25. 测试覆盖未提供、零、负数、非数字、溢出和超过上限的 limit，以及溢出的 job ID 和额外路径段。
26. 完成终态清理后不再存在运行中历史条目，包括 active master 切换或 terminal-zombie 恢复之后。快照写入、删除或 TTL 设置失败不能丢失持久化待清理记录，也不能改变终态结果。
27. 即使立即删除失败，残留的终态运行中历史条目也按原始终态时间起算的 `history-job-expire-minutes` 过期；清理重试和 master 恢复不能延长截止时间。
28. 排队中或延迟的历史及 attempt 更新不能重新创建已清理条目、移除终态 TTL 或延长过期时间。终态 REST 读取不能回退到待清理的运行中条目。

## 交付计划

1. 确认记录、attempt、存储、保留和 REST 契约；
2. 增加独立的 HA 运行中和已完成作业历史条目、原子 `EntryProcessor` 和结构化 task 失败传输，并补充单元测试；
3. 增加捕获时脱敏、去重、保留和恢复测试；
4. 增加 REST 路由和端点、认证边界、输入校验、向后兼容以及运行中和已完成作业的 API 测试；
5. 在单独的 pull request 中增加 Web UI 历史视图。
