# 实验性 CDC 进度契约

SeaTunnel CDC Source 可以通过 `org.apache.seatunnel.api.cdc` 中的实验性类型提供最新运行进度。
该契约只描述 Connector 报告什么；引擎如何采集、存储或暴露这些报告不在本文范围内。它不会改变
checkpoint 或恢复行为，目前也不是稳定的 Connector API。

## 所有权

Reader 和 Enumerator 报告不同类型的事实，且互不推断对方的状态：

- Reader 报告自身生命周期、当前 split、已消费位置、该位置最近一次变化的时间以及最新源事件时间。
- Enumerator 报告快照发现和分配状态、split 数量、剩余工作以及有界的活动 split 明细。

split 分配完成并不能证明 Reader 已进入追赶或增量读取阶段。

## Provider 保证

Source 通过 `SupportCdcProgress` 标记声明能力，其 Reader 或 Enumerator 实现
`CdcProgressProvider#getCdcProgress()`，并满足以下要求：

- **非阻塞。** 返回 Connector 已经维护的快照，不执行数据源访问、网络访问、checkpoint 或其他
  阻塞 I/O，也不获取记录处理或 split 分配持有的锁。
- **线程安全。** 采集可以与任何 Reader 或 Enumerator 回调并发执行。
- **不可变。** 报告及其中的值都是不可变的。报告不引用 Connector 的可变 offset 对象，坐标在构造
  报告时复制。
- **真实。** 只有当前运行时状态能够证明时才使用 `EXACT`。缺失、过期、不支持或近似的值如实标记，
  不用零或猜测代替。
- **首个报告前可为空。** 返回 `null` 表示报告尚不可用。

每个 `CdcProgressValue` 独立携带精度：

| 精度 | 含义 |
| --- | --- |
| `EXACT` | 当前 Connector 状态可以准确证明该值。 |
| `BEST_EFFORT` | 该值可用于诊断，但不能保证完全精确。 |
| `UNSUPPORTED` | Connector 无法提供该值。 |
| `UNAVAILABLE` | 支持该值，但本次观测时暂不可用。 |

`EXACT` 和 `BEST_EFFORT` 总是包含非空 payload；`UNSUPPORTED` 和 `UNAVAILABLE` 不包含 payload。

## 位置

`CdcProgressPosition` 在明确的位置类型（例如 `MYSQL_BINLOG`）和 schema 版本下保留 Connector
原生坐标。消费者不能假设不同类型使用相同字段，也要允许同一类型下 `BEST_EFFORT` 与 `EXACT`
值的字段不同：配置的时间戳起始位置只包含时间戳，直到首条消费记录将其替换为完整的原生坐标。

位置 payload 只能包含 binlog position、GTID、LSN 或时间戳等 offset 坐标，禁止包含凭证、连接 URL
或其他认证信息。

Reader 的已消费位置是其最近观测到的位置，不是已完成 checkpoint 的位置；位置不变化本身不能证明
源端延迟、反压或数据源停滞。

## Reader 生命周期

`CdcProgressLifecycle` 由 Reader 正在处理的 split 推导，因此各启动模式下含义一致：

| 生命周期 | 含义 |
| --- | --- |
| `UNKNOWN` | 尚未分配 split。 |
| `SNAPSHOT` | Reader 正在读取快照 split。已消费位置为 `UNAVAILABLE`，split 水位由 Enumerator 报告。 |
| `CATCH_UP` | Reader 正在消费变更流，但尚未越过必须遵守的快照高水位。 |
| `INCREMENTAL` | Reader 只消费变更流。 |
| `COMPLETED` | Reader 已完成所有将收到的 split，并已发出不再有元素的信号。有界作业的每个 Reader 都会到达该阶段；无界作业中，未持有变更流 split 的 Reader 在 Enumerator 无更多 split 可分配时到达该阶段。 |

### 各启动模式的预期报告

| 启动模式 | Reader 生命周期顺序 | Enumerator 快照状态 | 首条记录前 → 后的已消费位置 |
| --- | --- | --- | --- |
| `initial` | 收到变更流 split 的 Reader：`UNKNOWN` → `SNAPSHOT` → `CATCH_UP` → `INCREMENTAL`；当 Reader 并行度大于增量并行度时，其余 Reader：`UNKNOWN` → `SNAPSHOT` → `COMPLETED` | `DISCOVERING` → `ASSIGNING` → `COMPLETED`；增量阶段保留计数 | 快照期间 `UNAVAILABLE` → `BEST_EFFORT`（最小快照水位）→ `EXACT` |
| `snapshot-only` | `UNKNOWN` → `SNAPSHOT` → `COMPLETED` | `DISCOVERING` → `ASSIGNING` → `COMPLETED` | 始终 `UNAVAILABLE` |
| `earliest`、`latest`、`specific`、`timestamp`、`committed-offset` | `UNKNOWN` → `INCREMENTAL` | `NOT_APPLICABLE`；计数为 `UNSUPPORTED` | `BEST_EFFORT`（配置或解析出的起始 offset）→ `EXACT` |

`CATCH_UP` 只出现在快照阶段之后。当变更流起始 offset 已等于最大快照高水位时，Reader 跳过该阶段，
直接报告 `INCREMENTAL`。

恢复后的生命周期同样由恢复的 split 推导：未完成的快照 split 报告 `SNAPSHOT`，变更流 split 在仍需
越过快照水位时报告 `CATCH_UP`，否则报告 `INCREMENTAL`。恢复的起始位置在首条消费记录前为
`BEST_EFFORT`。契约没有单独的恢复中状态，也不报告恢复来源。

## Enumerator 快照分配

`CdcSnapshotAssignmentStatus` 由 Enumerator 拥有：

| 状态 | 含义 |
| --- | --- |
| `NOT_APPLICABLE` | 该启动模式没有快照阶段。 |
| `DISCOVERING` | 仍在发现表或将表切分为 split。 |
| `ASSIGNING` | 已创建的 split 正在等待 Reader。 |
| `COMPLETED` | 所有快照 split 已分配。运行中计数表示仍在读取的 split 数量。 |

已分配、已完成、运行中、已准备待分配以及尚未分块的表计数分别报告，避免延迟分块被一个含糊的
剩余计数掩盖。存在的计数不为负；仅当三者均为 `EXACT` 时才校验 `assigned == completed + running`。
活动 split 明细最多保留 `CdcEnumeratorProgressReport.MAX_ACTIVE_SPLITS` 条；`activeSplitsTruncated`
表示是否有明细被省略，聚合计数仍描述完整状态。

## 当前限制

- 该契约和报告类型仍为实验性，Connector 验证期间可能变化。
- Reader 报告不包含 checkpoint 位置和恢复位置。
- 没有统一的数值型延迟；消费者按位置类型比较原生位置。
- 本文不描述引擎如何采集或暴露报告。
