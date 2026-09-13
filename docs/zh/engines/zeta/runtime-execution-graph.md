# 运行时执行图

## 状态

本文是 [issue #11351](https://github.com/apache/seatunnel/issues/11351) 的设计契约草案。第一阶段交付应当把现有 Job Detail DAG 和实时可观测指标组合成一个聚焦诊断的运行时执行图，而不是新增另一条指标链路。

该设计刻意控制边界：

- 复用 Zeta 作业 DAG 作为唯一图拓扑
- 复用 Active Master 内存中的实时指标窗口展示节点和边的健康状态
- 复用现有指标 API、checkpoint REST 接口和 UI 详情面板做 drill-down
- 第一版不引入持久化指标快照，也不提供长期回放

## 问题

SeaTunnel 已经暴露了作业拓扑、vertex 指标、edge 指标、checkpoint 历史、异常和日志。运行中的作业出现变慢或阻塞时，使用者仍然需要一条统一路径回答这些问题：

- 哪个 vertex 现在忙、空闲或等待
- 哪条带队列的 edge 正在阻塞或逐渐填满
- 当前瓶颈更接近 Source 读取、Transform 处理、Sink 写入、checkpoint 或 commit，还是外部系统
- 在进入表格和日志前，先判断 slowdown 大概率从哪里开始

运行时执行图应当作为诊断入口，而不是变成第二套监控系统。

## 现有基础

V1 运行时执行图应组合现有 Zeta 契约：

| 范围 | 现有契约 |
|---|---|
| 拓扑 | Job Detail 已经渲染 `JobDAGInfo` 拓扑。 |
| Vertex 指标 | `/metrics/realtime/jobs/{jobId}/vertices` 返回最近窗口内 Source、Transform、Sink 的 bucket。 |
| Edge 指标 | `/metrics/realtime/jobs/{jobId}/edges` 返回带 `queueId` 与 `targetVertexId` 的队列 edge bucket。 |
| Checkpoint 状态 | `/jobs/checkpoints/:jobId` 和 `/jobs/checkpoints/history/:jobId` 暴露 checkpoint 概览与历史。 |
| 异常与日志 | Job Detail、Exception 和 Log API 已经提供失败上下文。 |

Active Master 只保留短窗口内存数据。当前 collector 每 5 秒拉取一次 worker 指标，REST 查询窗口默认 3 分钟，最大 10 分钟。

## 目标

1. 在 DAG 上直接展示节点健康状态。
2. 在 DAG 上直接展示队列 edge 健康状态。
3. 不切换页面也能看出最热 vertex 和最阻塞 edge。
4. 点击图上的节点或边，可以进入现有指标表格或详情抽屉。
5. 在图附近展示 checkpoint 与任务错误上下文，但不把它们变成 graph-only 契约。
6. 对大 DAG 提供可预期、成本更低的降级展示。

## 非目标

- 分布式 tracing
- 任意时间范围的历史回放
- 持久化运行时指标快照
- connector 专属图组件
- 与 realtime observability 并行的新后端指标模型
- 替代人工判断的自动根因结论

## 运行时图数据模型

### 拓扑

图拓扑来自当前作业 DAG：

- `jobId`
- `vertexId`
- vertex 类型，例如 Source、Transform 或 Sink
- vertex 之间的有向边
- 可与 `targetVertexId` 关联的 edge 元数据

运行时执行图不能发明另一套拓扑。如果未来执行 DAG 发生变化，图应跟随引擎 DAG 契约，而不是维护自己的形状。

### 节点运行状态

每个图节点通过 `vertexId` 合并最近一个 realtime vertex point。

| Vertex 类型 | 主要视觉信号 | 辅助字段 |
|---|---|---|
| Source | `sourceReadRatio` 和 `sourceIdleRatio` | `sourceReadNs`、`sourceIdleNs`、`subtaskCount` |
| Transform | `transformBusyRatio` | `transformProcessNsPerRecord`、`transformRecordsIn`、`transformRecordsOut`、`subtaskCount` |
| Sink | `sinkBusyRatio` | `sinkWriteNsPerRecord`、`sinkRecordsIn`、`sinkPrepareCommitNs`、`sinkCommitNs`、`sinkAbortNs`、`subtaskCount` |

节点主颜色应代表最符合该 vertex 类型的指标。例如 Source 使用读取或空闲比例，Transform 使用 transform busy ratio，Sink 使用 sink busy ratio。

### 边运行状态

V1 只有带队列的 edge 才能暴露背压指标。每条图 edge 优先通过 REST 返回的 `targetVertexId` 合并最近一个 realtime edge point，必要时再从 `queueId` 解码。

| 字段 | 含义 |
|---|---|
| `queueId` | realtime 聚合使用的稳定队列指标标识。 |
| `targetVertexId` | 用来把队列指标映射回 DAG edge 的下游 vertex。 |
| `bpRatio` | 当前 bucket 内生产端等待队列容量的时间占比。 |
| `queueFillRatio` | 最近一次采样到的队列填充比例。 |
| `queueSize` | 最近一次采样到的队列大小。 |
| `queueCapacity` | 最近一次采样到的队列容量。 |

edge 颜色应代表 `bpRatio`，edge 宽度应代表 `queueFillRatio`。详情抽屉应展示原始字段和最近 bucket 序列。

## 稳定契约与 Best-Effort 信号

运行时图需要区分稳定标识字段和有诊断价值但来自采样的信号。

稳定契约字段：

- `jobId`
- `vertexId`
- `queueId`
- `targetVertexId`
- `bucketMs`
- `fromMs`
- `toMs`
- point 时间戳 `ts`
- `subtaskCount`

Best-effort 诊断字段：

- busy ratio
- idle ratio
- 单条记录耗时估算
- queue size
- queue fill ratio
- producer wait ratio
- checkpoint 与错误摘要 badge

Best-effort 字段在恢复、rescale、counter reset 或采样延迟附近可能波动。UI 应把它们表达为实时诊断信号，而不是审计口径的统计值。

## 刷新、保留与成本

V1 应保持现有刷新和保留模型：

- worker counter 由 Active Master 收集
- 收集结果只保留短窗口内存数据
- REST 查询窗口默认 3 分钟
- REST 查询窗口最大 10 分钟
- UI 可以比 collector 更频繁刷新，但必须能接受连续刷新拿到相同 bucket
- 默认不把 runtime graph 数据写入磁盘

这样可以让运行时图的成本跟随现有 realtime observability，而不是新增一条轮询或持久化链路。

## Checkpoint 与错误上下文

Checkpoint 和错误信号应显示在图附近，但 V1 中仍保持独立契约：

- checkpoint 概览与历史继续来自 checkpoint REST 接口
- 作业异常与日志继续来自现有作业详情 API
- 图上可以展示小型状态 badge 或入口链接，但 drill-down 应打开现有详情面板

这可以避免在生命周期与保留规则尚未明确前，把 checkpoint 数据混入 realtime metrics endpoint。

## 大 DAG 降级策略

大 DAG 可能难以阅读，也会带来较高重绘成本。V1 应当降级，而不是强行在每个元素上渲染全部信号。

推荐行为：

- 保留拓扑渲染能力
- 展示最热 vertices 与最阻塞 edges 的摘要健康表
- DAG 较大时限制自动 fit 和动画
- 保持选中节点或边后的 drill-down 能力
- 不为了 UI 复杂度提高 master 采集频率

具体实现 PR 应记录选择的大 DAG 阈值，并将其保持为 UI 渲染规则，而不是后端采样规则。

## V1 交付计划

1. 保持 Active Master realtime REST endpoint 作为 vertex 和 edge 运行状态来源。
2. 根据匹配 `vertexId` 的最新 vertex point 渲染节点颜色。
3. 根据匹配 `targetVertexId` 的最新 edge point 渲染 edge 颜色和宽度。
4. 在现有详情抽屉中展示最近 point 序列和原始字段。
5. 将 checkpoint 与错误信息作为邻近上下文入口，而不是向 realtime metrics 中嵌入新字段。
6. 对过大的 DAG 提供降级视图，列出最热 vertices 与最阻塞 edges。
7. 同步更新 REST、Web UI、运维文档的英文与中文说明。

## 验收要求

实现 PR 至少应覆盖这些检查：

- 后端 realtime edge 和 vertex 响应测试覆盖当前字段与 `targetVertexId` 映射
- UI 测试覆盖节点染色、edge 染色、edge 宽度和指标未开启时的行为
- 大 DAG 降级使用确定性的合成 DAG 做测试
- 文档明确 realtime window 仅为内存、best effort
- V1 不引入新的持久化指标表、文件或写入路径

## 相关文档

- [实时可观测性](./realtime-observability.md)
- [忙碌度与背压](./busyness-and-backpressure.md)
- [Web UI](./web-ui.md)
- [RESTful API V2](./rest-api-v2.md)
