# 任务失败历史设计

本文档提出 [GH-11667](https://github.com/apache/seatunnel/issues/11667) 的第一版后端契约。目前仅为设计，不代表 API 已经实现。

## 问题

当前 Job Detail 页面只展示一条异常文本。当 pipeline 多次恢复，或者同一个作业中的不同 task group 发生失败时，这些信息不足以定位问题。运维人员需要在不搜索每个 worker 日志的情况下，看到失败发生在哪次执行、哪个位置以及什么时间。

当前引擎状态有三个相关限制：

- `PhysicalPlan` 只保留 sub-plan 上报的第一条错误；
- `TaskExecutionState` 携带格式化后的异常文本，但没有持久化的失败标识；
- finished job 历史只保存最终错误文本，不保存到达终态前的失败序列。

## 范围

第一阶段实现应提供有界的、作业级别的失败历史，并通过同一个 REST 契约查询运行中和已完成作业。

它应当：

- 按 pipeline 执行 attempt 对失败进行分组；
- 标识上报失败的 pipeline 和 task group；
- 在信息可用时保留 worker 地址和 task 元数据；
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
- 当前 attempt 必须写入 HA 状态，避免新的 active master 从 `0` 重新编号。

`SubPlan.pipelineRestoreNum` 是当前对应这一边界的内存计数器。实现必须将该计数器改为 HA 持久化，并将其作为唯一权威的 attempt 值，不能再引入第二个计数器。新的 active master 必须在调度或记录下一次 attempt 前恢复该值，恢复次数限制、失败记录和 REST 响应都读取同一个值。

该模型与现有 pipeline 恢复边界一致，也不会引入引擎当前没有提供的 task 级重试语义。

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
  "worker": "10.0.0.12:5801",
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
- `attemptStartedAt` 为可选，因为失败可能发生在 pipeline 进入 `RUNNING` 之前。该字段存在时，表示该 pipeline attempt 的持久化开始时间，并且相同 `pipelineId` 和 `attempt` 的所有记录使用同一个值。它不同于 `timestamp`，后者表示单条失败被捕获的时间；
- `taskId`、`taskName`、`worker`、`exceptionType`、`message` 和 `stackTrace` 为可选，因为旧路径或合成失败路径可能无法提供；
- `messageTruncated` 和 `stackTraceTruncated` 为必填布尔值，用于说明对应内容是否在存储前被截断；
- `exceptionType` 必须来自结构化失败传输，不能通过解析格式化堆栈推断；
- `stackTrace` 保留诊断细节，`message` 用于简短展示。
- 存储的 UTF-8 内容中，`message` 上限为 4 KiB，`stackTrace` 上限为 64 KiB。截断必须保持 UTF-8 有效。截断消息保留开头，截断堆栈同时保留开头和结尾，以便保留异常信息和最深层 cause。

## 捕获与去重

`TaskExecutionState` 是 task 到 master 的自然传输边界。实现应扩展这条传输路径，增加结构化失败字段，并在释放 task 资源之前由 JobMaster 捕获记录。异常内容必须在该捕获边界完成脱敏和长度限制，再写入 HA 或 finished-job 历史。实现应将现有 `DryRunConnectFailureMessageSanitizer` 的规则抽取为共享工具，不能持久化未经处理的 connector 消息或堆栈。

重复收到同一个 task group 的终态不应生成重复记录。第一版使用 `(pipelineId, attempt, taskGroupId)` 去重，因为一个 task group 在一次 pipeline attempt 中只有一个终态失败。以作业级 HA 条目为目标的 Hazelcast `EntryProcessor` 在一次原子操作中完成去重检查、sequence 分配、追加记录和最旧记录删除。它必须从 task 状态操作路径异步提交。完成回调可以记录存储失败，但不能等待 Hazelcast operation thread，也不能重新进入该线程。第一次终态上报创建记录并分配 sequence，相同 key 的后续上报直接忽略且不消耗新的 sequence。同一 attempt 中不同 task group 的失败分别保留，恢复后的失败使用新的 attempt，因此仍然可见。

历史记录属于尽力而为的诊断能力。存储失败需要记录日志，但不能阻塞原始失败处理或恢复决策。

## 存储与保留

失败历史应使用以 `jobId` 为 key 的独立 HA 引擎状态条目。第一版可以使用独立的 Hazelcast `IMap`，与引擎现有的运行中作业状态保持一致。它的备份数量和持久化配置不能提供比现有运行中和已完成作业状态 map 更大的持久化范围。REST 表示不依赖具体的存储选择。

第一版使用以下边界：

- 每个作业最多保留 100 条失败记录；
- 超过限制时删除最旧记录；
- 作业运行期间不设置 TTL；
- 作业进入终态后，使用 `JobHistoryService` 现有的 `history-job-expire-minutes` 策略。

初始上限应使用常量，不新增用户配置。如果实际运行数据证明 100 条不足，可以后续增加可配置项。

作业进入终态时，`JobHistoryService` 将保留的失败记录和权威 pipeline attempt 值写入独立的 finished history 条目。该方案复用现有 finished job 的生命周期和 `history-job-expire-minutes` 语义，不假设已经存在独立或可插拔的历史存储抽象。对应的 finished job 记录过期时，失败历史条目也会被删除。

终态快照写入采用尽力而为语义。写入或清理失败必须记录日志，但不能改变作业终态、恢复行为或现有 finished job 记录。运行中和已完成作业虽然存储生命周期不同，但读取时使用同一个响应模型。

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
- 未知或已过期作业沿用现有 job-not-found 行为；
- 无论记录来自哪个独立状态条目，运行中和已完成作业都返回同一个响应模型。

`JobInfoServlet` 当前将 `/job-info/` 之后的全部路径信息作为一个数字 job ID 解析。REST 实现必须扩展该路由，或增加等效的独立处理器，确保 `/job-info/{jobId}` 保持现有行为，同时将 `/job-info/{jobId}/failures` 路由到失败历史。路由只能匹配这两种精确路径。额外路径段、前缀或子字符串匹配必须沿用现有 not-found 行为。

现有 job detail 响应和 `errorMsg` 字段保持不变，在 Web UI 单独接入历史端点前继续兼容现有客户端。

## 安全与输入校验

该端点沿用现有引擎 REST API 的 `BasicAuthFilter` 边界，不新增端点专用的认证机制。如果部署未启用 REST 认证，该诊断数据与其他 job-detail 端点遵循相同的暴露策略。文档必须说明异常文本和 worker 地址可能包含敏感的运行信息。

脱敏必须在写入 HA 前完成，而不能只在 REST 响应序列化时处理。这样 Hazelcast 状态、终态快照、外部历史后端和 API 响应都会保存同一份有界内容，也不能通过其他存储路径读取未经脱敏的数据。

路由处理器负责校验 `jobId` 和 `limit`：

- 非法 job 标识、非数字或非正数的 limit 返回受控的 `400` 响应；
- 超过保留上限的值限制为该上限；
- 校验失败不能包含堆栈，也不能通过共享异常处理器回显不可信输入。

Worker 地址保持可选，并与失败记录的其他字段使用同一认证边界。后续 API 版本可以改用逻辑 worker 标识，但第一版不能暴露比现有 job-detail API 更多的 worker 元数据。

## Web UI 后续工作

Exception tab 可以在单独变更中接入 REST 端点。第一版 UI 应按 attempt 分组，并展示时间、pipeline、task group、task 名称、worker、异常类型和消息。堆栈默认折叠。

缺失的可选字段应显示为不可用。当引擎只提供 task group 级失败时，UI 不应宣称具有 task 级精度。

## 兼容性

该功能为增量功能：

- 现有作业不需要修改配置；
- 现有 REST 字段和最终错误消息继续保留；
- 不修改 checkpoint 或 savepoint 内容；
- 旧失败路径只需要填写它能够提供的字段。

## 验收标准

1. 首次执行的 task group 失败生成一条 attempt `0` 的记录；
2. pipeline 恢复后失败生成一条 attempt 递增的新记录；
3. 重复发送同一个终态不会生成重复记录；
4. 同一个 attempt 中不同 task group 的失败分别保留；
5. master 切换后记录和下一个 attempt 编号保持不变；
6. 已完成作业在配置的历史过期时间内可以查询相同记录；
7. 超过 100 条记录后按确定顺序删除最旧记录；
8. 超过 4 KiB 的消息和超过 64 KiB 的堆栈在有效 UTF-8 边界截断，并提供对应的截断标记；
9. 作业进入终态时写入一个有界失败历史条目，该条目随对应的 finished job 记录过期；
10. 写入或清理失败历史时发生错误，不改变作业失败、恢复或终态流程；
11. 现有 job detail 客户端继续收到当前 `errorMsg` 字段。
12. 并发重复上报只生成一条记录，并通过作业条目的原子更新只分配一个 sequence；
13. 消息和堆栈中的敏感信息在写入 HA 和 finished history 前完成脱敏；
14. 非法 `jobId` 或 `limit` 返回受控的 `400` 响应，不暴露堆栈或回显非法值；
15. 该端点使用与现有 job-detail 端点相同的 REST 认证边界。
16. 失败历史更新异步提交，不阻塞 Hazelcast operation thread；
17. 失败历史状态使用的备份和持久化范围不超过现有作业状态 map；
18. 只接受精确的 `/job-info/{jobId}` 和 `/job-info/{jobId}/failures` 路径；额外路径段沿用现有 not-found 行为。

## 交付计划

1. 确认记录、attempt、存储、保留和 REST 契约；
2. 增加独立的 HA 运行中和已完成作业历史条目、原子 `EntryProcessor` 和结构化 task 失败传输，并补充单元测试；
3. 增加捕获时脱敏、去重、保留和恢复测试；
4. 增加 REST 路由和端点、认证边界、输入校验、向后兼容以及运行中和已完成作业的 API 测试；
5. 在单独的 pull request 中增加 Web UI 历史视图。
