# Worker 节点资源视图

## 状态

本文是 [issue #11665](https://github.com/apache/seatunnel/issues/11665) 的设计契约草案。第一阶段交付应当把已经收集到的节点数据变成 Workers/Master 页面上可用的集群资源视图，而不引入新的调度状态。

该设计刻意控制边界：

- 复用现有 `/system-monitoring-information` 返回的数据作为节点级 JVM/主机指标来源
- 复用资源管理器已有的实时 `WorkerProfile` 状态作为按 worker 统计 slot 的数据来源
- 只新增一个轻量、只读的 REST 投影；不新增任何可变状态，也不改动任何调度决策逻辑
- 把"按 worker 展示当前运行任务"（需要关联 `/trace/task-mapping`）留给后续版本，因为该接口是按作业维度设计的，而 Workers 页面本身没有作业上下文

## 问题

Workers/Master 页面（`seatunnel-engine-ui/src/views/managers/index.tsx`）目前只渲染 4 列：Host、Port、Physical MEM Total、Heap MEM Used，而 `/system-monitoring-information`（对应模型见 `seatunnel-engine-ui/src/service/manager/types.ts`）实际已经返回约 35 个字段。源码中还留着一个被注释掉的 Action 列。

另外，目前完全没有按 worker 维度展示的 slot 视图。`OverviewInfo`（`GET /overview`）只暴露集群维度的 `totalSlot`/`unassignedSlot` 总数。使用者无法从 UI 上看出哪个 worker 还有空闲 slot、哪个已经打满，也看不出集群容量分布是否均衡。

## 现有基础

| 范围 | 现有契约 |
|---|---|
| 节点级 JVM/主机指标 | `GET /system-monitoring-information` 每个节点返回一条 `Monitor` 记录（CPU load、堆内存/物理内存、GC 次数与耗时、线程数、executor 队列大小等）。 |
| 集群维度 slot 总数 | `GET /overview` 返回的 `OverviewInfo.totalSlot`/`unassignedSlot` 只是集群维度的汇总值。 |
| 按 worker 的实时资源状态 | `ResourceManager.getRegisterWorker()` 返回实时的 `ConcurrentMap<Address, WorkerProfile>`。每个 `WorkerProfile` 已经带有 `assignedSlots`/`unassignedSlots`（`SlotProfile[]`）、`dynamicSlot`、`attributes`（worker 标签）以及调度器内部使用的 `systemLoadInfo`（`cpuPercentage`、`memPercentage`）。这是资源管理器自己用来判断是否接受分配请求的权威数据源，不是派生近似值。 |
| 按作业的任务-worker 映射 | `GET /trace/task-mapping/:jobId` 已经能算出单个作业的任务/主机分配关系，但没有 UI 消费方，也没有跨作业的集群级形态。 |
| 跨节点读取模式 | `GetOverviewOperation` 已经展示了 REST 请求如何触达 master 持有的实时状态的既有模式：`OverviewService` 先判断本节点是否为 active master（`getSeaTunnelServer(true)`），不是的话通过 `NodeEngineUtil.sendOperationToMasterNode(...)` 转发。 |

## 目标

1. 把已经拉取到的 `/system-monitoring-information` 字段中真正有运维价值的一部分渲染成表格列，而不是目前的 4 列。
2. 保留访问全量原始字段的入口，同时不把表格变成约 35 列、无法阅读的宽表。
3. 新增按 worker 维度的 slot 视图（总数/已用），数据来自资源管理器已有的实时状态，不新增任何记账逻辑。
4. 集群空闲（没有作业运行）时该视图依然有意义。
5. 刷新成本保持可控，和现有轮询模型一致。

## 非目标

- 通过 `/trace/task-mapping` 实现"按 worker 展示当前运行任务"的下钻。该接口是按作业维度设计的，要做成跨作业的集群级视图需要单独的扇出方案，留给本视图契约稳定后的后续版本。
- 节点资源指标的历史或长期留存。
- 集群容量规划或自动扩缩容建议。
- 按节点查看日志（现有的 worker 日志查看器已经覆盖）。

## 资源模型

### 节点监控字段（复用，不改动后端）

这部分不需要任何新的后端工作：字段在 `Monitor` 里已经存在。V1 表格应该展示精选子集，而不是全部约 35 个字段：

| 列 | 对应的现有 `Monitor` 字段 |
|---|---|
| Host / Port | `host`、`port` |
| 角色 | 由 `isMaster` 派生（Master/Worker 页面已经用它做区分） |
| CPU Load | `load.systemAverage` |
| Heap Used / Max | `heap.memory.used`、`heap.memory.max` |
| Physical MEM Total | `physical.memory.total` |
| GC（minor/major） | `minor.gc.count`、`major.gc.count` |
| 线程数 | `thread.count` |

其余字段不会被丢弃：一个"查看详情"操作（对应 `managers/index.tsx` 中已经存在但被注释掉的列）会打开一个抽屉，展示该行完整的原始 `Monitor` 字段，以及新增的按 worker 统计的 slot/标签信息。这样既避免了宽表难以阅读的问题，又保证所有既有字段仍然可以访问到。

### 按 Worker 的 Slot 状态（新增只读投影，不新增状态）

新增一个接口，对资源管理器已有的实时状态做投影，每个已注册 worker 一行：

| 字段 | 数据来源 |
|---|---|
| `address` | `WorkerProfile.address` |
| `totalSlot` | `assignedSlots.length + unassignedSlots.length` |
| `usedSlot` | `assignedSlots.length` |
| `dynamicSlot` | `WorkerProfile.dynamicSlot` |
| `cpuPercentage` / `memPercentage` | `WorkerProfile.systemLoadInfo`（可能为空，只有调度器已经采集到时才有值） |
| `attributes` | `WorkerProfile.attributes`（worker 标签） |

这是一次纯读取/投影操作：调用 `getRegisterWorker()` 并映射每个 `WorkerProfile`，完全复用 `GetOverviewOperation` 已经建立的跨节点访问模式。它不会给 `WorkerProfile` 本身新增任何字段，也不改动任何分配/调度代码路径。

Workers/Master 页面在客户端按 `address`（host + port）把这份数据和现有的 `Monitor` 行做关联，方式和页面现在用 `isMaster` 区分 Master/Worker 行完全一致。

## 稳定契约与 Best-Effort 信号

稳定标识：

- `host`、`port`、`isMaster`/角色
- `totalSlot`、`usedSlot`（派生自实时 slot 数组，因为读取的是同一份状态，天然和调度器自身视图保持一致）

Best-effort 诊断信号：

- CPU load、内存使用、GC 计数（今天通过 `/system-monitoring-information` 拿到的本来就是 best-effort 数据）
- 来自 `systemLoadInfo` 的 `cpuPercentage`/`memPercentage`——按调度器自己的节奏采集填充，不保证严格实时

## 刷新、留存与成本

- 复用 Workers/Master 页面现有的"进入页面即请求"模型，不在 `/system-monitoring-information` 之外新增持续轮询。
- 新接口的开销是 O(已注册 worker 数)，和已经遍历同一份资源管理器状态的 `GET /overview` 成本量级一致。
- 不会把任何新数据写入磁盘或做超出资源管理器现有内存态 `WorkerProfile` map 之外的留存。

## 大集群降级

对于 worker 数量较多的集群，表格仍然是扁平、可排序/可筛选的列表，而不是图，因此不存在类似 DAG 那样的渲染成本问题——行数随 worker 数量线性增长，且现有 `NDataTable` 组件已经支持分页。

## V1 交付计划

1. 新增 `WorkerOverviewInfo` DTO 和 `GetWorkerOverviewOperation`，复用 `GetOverviewOperation` 的跨节点模式，数据来自 `ResourceManager.getRegisterWorker()`。
2. 参照现有 `OverviewService`/`OverviewServlet` 的结构，新增对应的 REST 接口和 servlet。
3. 扩展 `seatunnel-engine-ui` 的 manager service/types，拉取新接口并按 address 与现有 `/system-monitoring-information` 数据做关联。
4. 按上文的精选列扩展 Workers/Master 表格，并重新启用现有（当前被注释）的 Action 列，作为展示完整原始数据的"查看详情"抽屉。
5. 更新中英文 Web UI 与 REST API 文档。

## 验收要求

- 后端单元测试覆盖从 `WorkerProfile` 到 `WorkerOverviewInfo` 的映射逻辑，包括零已注册 worker、worker 的 slot 数为零这两种情况。
- 前端测试覆盖按 address 做客户端关联的逻辑，以及精选列的渲染。
- 文档需要明确写出 slot 与 load 字段反映的是资源管理器的实时内存态，而不是持久化历史。
- V1 不引入任何新的持久化表、文件或写入路径。

## 相关文档

- [Web UI](./web-ui.md)
- [RESTful API V2](./rest-api-v2.md)
- [实时可观测性](./realtime-observability.md)
