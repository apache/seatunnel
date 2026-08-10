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
- 为兼容性保留现有单一 `errorMessage` 字段。

第一阶段不增加日志聚合、分布式追踪或无界异常归档。Web UI 应在后端契约确定后单独实现。

## Attempt 模型

Attempt 属于 pipeline，而不是单个 task。

- pipeline 初始执行为 attempt `0`；
- 在调度恢复前递增 attempt；
- 同一次执行中捕获的所有失败使用相同的 attempt；
- 当前 attempt 必须写入 HA 状态，避免新的 active master 从 `0` 重新编号。

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
  "stackTrace": "java.sql.SQLException: Connection reset\n..."
}
```

字段规则：

- `sequence` 在单个作业内单调递增，在时间戳相同时提供确定顺序；
- `timestamp`、`jobId`、`pipelineId`、`attempt` 和 `taskGroupId` 为必填；
- `taskId`、`taskName`、`worker`、`exceptionType`、`message` 和 `stackTrace` 为可选，因为旧路径或合成失败路径可能无法提供；
- `exceptionType` 必须来自结构化失败传输，不能通过解析格式化堆栈推断；
- `stackTrace` 保留诊断细节，`message` 用于简短展示。

## 捕获与去重

`TaskExecutionState` 是 task 到 master 的自然传输边界。实现应扩展这条传输路径，增加结构化失败字段，并在释放 task 资源之前由 JobMaster 捕获记录。

重复收到同一个 task group 的终态不应生成重复记录。第一版可以使用 `(pipelineId, attempt, taskGroupId)` 去重，因为一个 task group 在一次 pipeline attempt 中只上报一个终态失败。后续重试使用不同 attempt，因此仍然可见。

历史记录属于尽力而为的诊断能力。存储失败需要记录日志，但不能阻塞原始失败处理或恢复决策。

## 存储与保留

失败历史应使用以 `jobId` 为 key 的独立分布式 map。这样运行中和已完成作业可以使用同一个数据源，并在 active master 切换后保留数据。

第一版使用以下边界：

- 每个作业最多保留 100 条失败记录；
- 超过限制时删除最旧记录；
- 作业运行期间不设置 TTL；
- 作业进入终态后，使用 `JobHistoryService` 现有的 `history-job-expire-minutes` 策略。

初始上限应使用常量，不新增用户配置。如果实际运行数据证明 100 条不足，可以后续增加可配置项。

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
- 运行中和已完成作业读取同一个存储。

现有 job detail 响应和 `errorMessage` 字段保持不变，在 Web UI 单独接入历史端点前继续兼容现有客户端。

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
8. 写入失败历史时发生错误，不改变作业失败或恢复流程；
9. 现有 job detail 客户端继续收到当前 `errorMessage` 字段。

## 交付计划

1. 确认记录、attempt、存储、保留和 REST 契约；
2. 增加 HA 存储模型和结构化 task 失败传输，并补充单元测试；
3. 增加捕获、去重、保留和恢复测试；
4. 增加 REST 端点以及运行中和已完成作业的 API 测试；
5. 在单独的 pull request 中增加 Web UI 历史视图。
