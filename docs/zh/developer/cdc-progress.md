# 实验性 CDC 进度契约

SeaTunnel CDC Source 可以通过 `org.apache.seatunnel.api.cdc` 中的实验性类型提供最新运行进度。
该契约用于引擎可观测性，不会改变 checkpoint 或恢复行为，目前也不是稳定的 Connector API。

## 所有权

Reader 和 Enumerator 报告不同类型的事实：

- Reader 报告自身生命周期、当前 split、已消费位置、位置变化时间、源事件时间、已完成
  checkpoint 位置以及恢复位置。
- Enumerator 报告快照发现和分配状态、split 数量、剩余工作以及有界的活动 split 明细。

Enumerator 不应推断 Reader 的生命周期。尤其是，split 分配完成并不能证明 Reader 已进入追赶或
增量读取阶段。

## Provider 契约

`CdcProgressProvider#getCdcProgress()` 返回 Connector 已经维护的不可变快照。实现必须线程安全且
非阻塞，不能执行数据源访问、网络访问、checkpoint 或其他阻塞 I/O。在报告尚不可用时可以返回
`null`。

每个 `CdcProgressValue` 独立描述一个事实：

| 精度 | 含义 |
| --- | --- |
| `EXACT` | 当前 Connector 状态可以准确证明该值。 |
| `BEST_EFFORT` | 该值可用于诊断，但不能保证完全精确。 |
| `UNSUPPORTED` | Connector 或当前生命周期接线无法提供该值。 |
| `UNAVAILABLE` | 支持该值，但本次观测时暂不可用。 |

受支持的值必须包含非空 payload，`UNSUPPORTED` 和 `UNAVAILABLE` 不包含 payload。Connector 原生
位置会保留明确的位置类型和 schema 版本，消费者不能假设不同 Connector 使用相同字段。位置
payload 只能包含 binlog position、GTID、LSN 或时间戳等 offset 坐标，禁止包含凭证、连接 URL 或
其他认证信息。

## 运行时采集

Reader 报告在执行节点上定期采样、批量发送到活动 Coordinator。Enumerator 报告使用独立的
Coordinator 所有采集路径。活动 Coordinator 根据运行中的作业计划和自身管理的 slot 分配，确定
Enumerator Task Group 所在的节点，并向这些节点请求报告，包括 Enumerator 运行在自身节点上的
情况。通过校验的报告会写入 Coordinator 侧的最新值存储。

Source 通过 `SupportCdcProgress` 声明能力。Coordinator 在读取 slot 分配前先过滤该能力，
因此不会轮询非 CDC Enumerator。采集沿用 `print-execution-info-interval` 的监控周期，
每个 Worker 最多存在一个未完成请求。单个 Provider 失败不会丢弃其他 Task 的报告，
单次采集失败也不会停止后续监控。Provider 发布不可变快照；轮询不会获取 chunk 生成锁或访问数据源。

Enumerator Task 可能运行在活动 Coordinator 之外的节点上。这一传输细节不会把所有权交给 Worker
采样器：由 Coordinator 选择要轮询的 Enumerator、发起采集并负责排序和存储。Master 故障转移后，
恢复的 JobMaster 和 slot 分配会重新构建采集集合。

每个被接受的报告都包含 Task 标识、Source Vertex 标识、执行 attempt、attempt 内单调递增的序列号
和观测时间。相对于相同 owner 和 Task 已存储的报告，更低的 attempt 或同一 attempt 内不递增的
序列号会被忽略。这不是与权威部署标识的校验：重新打开且为空的作用域可能先收到旧 Reader 的
观测，然后才收到首个更新报告。并行 Reader 的 Task 明细保持独立，不会被当作一个原子的分布式快照。

## 生命周期和清理

Reader 生命周期包括 `SNAPSHOT`、`CATCH_UP`、`INCREMENTAL` 和 `UNKNOWN`。Enumerator 快照分配
状态单独表示为 `NOT_APPLICABLE`、`DISCOVERING`、`ASSIGNING` 和 `COMPLETED`。

最新值存储不保留历史。所属 Pipeline 清理时（包括 savepoint 完成），对应报告也会被删除。
只有已注册的活动 Pipeline 接受报告，延迟报告不能重新创建已删除的作用域。Master 清理同时使
Coordinator generation 失效并删除本地作用域；旧 generation 捕获的注册操作和 Enumerator 回调
不能向新 generation 发布报告。初始化失败只回滚该 JobMaster 拥有的作用域，并在 Pipeline 部署前
重建作用域。已完成 checkpoint 位置和恢复位置在对应生命周期回调证明之前保持 `UNSUPPORTED`。
当前消费位置不能当作已完成 checkpoint 位置，普通 split 分配也不能证明恢复来源。

Reader 进度在成功发送记录后复制独立的 offset 坐标。轮询不会读取 Connector 的可变 offset 对象，
因此后续原地更新或发送失败不会推进已经发布的消费位置。这会为成功的增量发送增加坐标复制工作，
Provider 不宣称逐记录零开销。

## 当前限制

- 该契约和报告类型仍为实验性。
- 当前基于 `connector-cdc-base` 的 CDC Source 会提供报告。MySQL 使用明确的 `MYSQL_BINLOG`
  位置类型；其他 base Connector 在定义更具体的位置类型前使用 Plugin 名称。未接入该 Provider 的
  CDC Source 不会返回报告。
- 仅快照、快照后增量和仅增量模式使用同一个 Provider 契约。配置或恢复的起始位置在成功发送记录前
  为 `BEST_EFFORT`；成功发送才证明消费进度。这不等同于恢复位置生命周期报告。
- Enumerator 报告最多保留 100 条活动 split 明细。`activeSplitsTruncated` 表示还有活动 split 被
  省略，聚合 split 数量仍描述完整状态。
- 内部诊断帧最多包含 100,000 个报告或 task-group 条目，每个原生位置最多包含 1,024 个字段。
  超限或格式错误的帧会被拒绝；这些限制不改变 Source、checkpoint 或 savepoint 状态。
- 当前实现不会通过 REST、CLI 或 metrics 暴露进度。
- 已完成 checkpoint 位置和恢复位置在对应引擎生命周期回调接入前保持 `UNSUPPORTED`。
- 位置不变化本身不能证明源端延迟、反压或数据源停滞。
