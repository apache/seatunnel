# 作业血缘 REST API 设计

## 状态

本文档是设计提案，并不表示该 API 已在已发布的 SeaTunnel 版本中可用。在 STIP 范围和公开契约
获得认可之前，它必须保持在已发布文档导航之外。

## 背景

Zeta REST API 已经在 `/job-info/{jobId}` 中返回供内置 Web UI 使用的 DAG，但该结构主要服务于
UI，并未被定义为稳定的血缘契约。因此，外部元数据目录、治理平台和运维工具目前没有受支持的
方式获取作业级的 Source、Transform、Sink 流向。

第一版应同时支持批处理和流处理作业的作业级执行血缘，但不能暗示列级精度，也不能依赖抽样的
行记录链路。

## 范围

### 目标

- 暴露一个 Zeta 作业的 Source、Transform、Sink 图；
- 作业处于等待、运行以及保留在完成历史中时返回同一种模型；
- 节点 ID 在单个作业内保持稳定；
- 暴露已知的 Source 和 Sink 表路径，但不推断表到表的映射；
- 复用作业详情已有状态，在 Active Master 切换后仍可查询；
- 保持现有 `/job-info/{jobId}` 响应不变；
- 在实现前明确排序、错误、限制和兼容性规则。

### 非目标

第一版不提供：

- 列级血缘；
- 字段表达式或 Transform 语义分析；
- 记录级来源追踪；
- 跨多个作业的血缘；
- 全局数据集图或外部元数据目录集成；
- Flink 或 Spark 引擎作业的血缘；
- 已完成作业超过保留时间后的历史图版本；
- 动态 Source 发现新表后的实时更新；
- Web UI 血缘页面。

## 术语

- **作业血缘**：作业范围内的 Source、Transform、Sink 执行图。
- **数据集元数据**：构建作业 DAG 信息时，Source 或 Sink 节点已经知道的表路径集合。
- **StainTrace**：抽样的记录链路，用于延迟和记录流分析，不是权威拓扑来源。
- **节点 ID**：只在一个作业血缘快照内稳定，不是连接器、数据集或跨作业标识。

## 现有架构

`JobMaster` 通过 `DAGUtils.getJobDAGInfo()` 延迟构建 `JobDAGInfo`。该对象包含：

- 作业 ID；
- 按 pipeline 分组的边；
- 包含节点 ID、插件类型、连接器名称和已知表路径的节点映射；
- 当前作业详情使用的执行位置信息。

运行中作业由 Coordinator 从 Active `JobMaster` 返回该对象。请求到达 Follower 时，现有 Master
Operation 路径会从 Active Master 获取。作业完成后，`JobHistoryService` 使用配置的
`history-job-expire-minutes` 保留同一个 `JobDAGInfo`。

血缘端点应是该现有对象的确定性投影，不应反序列化连接器配置、扫描 trace 文件或增加新的
Hazelcast Map。

## 权威数据源

第一版只使用 `JobDAGInfo` 作为权威数据源，因为它表示 Zeta 对运行中和已完成作业已暴露的执行图。

该选择带来以下约束：

- 血缘描述 Zeta 执行的图，而不是原始 HOCON block 布局；
- 优化器生成或合并的 Transform 按 `JobDAGInfo` 中已有节点展示，不重新构造内部组件；
- 节点 ID 复用 `VertexInfo.vertexId`，在该作业生命周期内稳定；
- pipeline ID 复用 `JobDAGInfo.pipelineEdges`；
- 表路径只是辅助元数据，不产生表级血缘边；
- 第一版不追加 `JobDAGInfo` 创建后动态发现的新表。

不得使用 StainTrace 补全图。StainTrace 是可选、抽样且独立存储的，只可能包含部分记录和阶段。

## REST 契约

建议端点：

```text
GET /job-lineage/{jobId}
```

独立路由可以避免修改 `/job-info/{jobId}` 当前的路径解析和响应行为。

响应示例：

```json
{
  "schemaVersion": 1,
  "jobId": "733584788375093248",
  "graphKind": "EXECUTION",
  "idScope": "JOB",
  "nodes": [
    {
      "id": "1",
      "kind": "SOURCE",
      "name": "Jdbc",
      "datasets": ["catalog.sales.orders"],
      "datasetMetadata": "REPORTED"
    },
    {
      "id": "2",
      "kind": "TRANSFORM",
      "name": "Sql",
      "datasets": [],
      "datasetMetadata": "NOT_APPLICABLE"
    },
    {
      "id": "3",
      "kind": "SINK",
      "name": "Kafka",
      "datasets": ["default.default.orders"],
      "datasetMetadata": "REPORTED"
    }
  ],
  "edges": [
    {
      "pipelineId": 1,
      "sourceNodeId": "1",
      "targetNodeId": "2"
    },
    {
      "pipelineId": 1,
      "sourceNodeId": "2",
      "targetNodeId": "3"
    }
  ],
  "warnings": []
}
```

### 字段语义

- `schemaVersion` 是响应契约版本，第一版为 `1`。
- `graphKind` 在本提案中固定为 `EXECUTION`。
- `idScope` 为 `JOB`；客户端保存节点时必须组合 `jobId` 和节点 `id`。
- `kind` 为 `SOURCE`、`TRANSFORM` 或 `SINK`。
- `name` 是 `VertexInfo.connectorType` 已有的连接器类型，只用于显示，不是标识符。
- `datasets` 包含 Source 和 Sink 节点已知的、排序且去重后的表路径字符串。
- `datasetMetadata` 为以下值之一：
  - `REPORTED`：响应包含 `JobDAGInfo` 中已有的非默认表路径，但不声明动态连接器已经发现全部表；
  - `UNAVAILABLE`：没有可靠的数据集元数据；
  - `NOT_APPLICABLE`：第一版中用于 Transform 节点。
- `warnings` 包含具有稳定 `code` 的对象；当 warning 只针对一个节点时，还包含
  `nodeId`。第一版定义 `DATASET_METADATA_UNAVAILABLE`，用于无法可靠表示数据集元数据的
  Source 或 Sink。客户端必须使用 `code` 而不是数组位置识别 warning。

节点按数字节点 ID 排序。边按 pipeline ID、源节点 ID 和目标节点 ID 排序。数据集名称排序，warning
按 `code` 和 `nodeId` 排序。因此，即使内部 Map 没有顺序保证，响应仍然确定。

## 数据集和 Transform 语义

多表 Source 和 Sink 仍为一个图节点，并带有多个 `datasets`。第一版不声明某个源表对应某个目标
表，因为 `JobDAGInfo` 中没有这种映射。

Transform 或 Transform chain 保留为一个执行节点。端点不解析 SQL、表达式、schema 或 Transform
实现类。过滤、拆分、Join 或重命名只有在 `JobDAGInfo` 已经包含对应连边时才会反映在图连接关系中。

对于动态表发现，响应是创建 `JobDAGInfo` 时可用元数据的快照。如果连接器当时无法枚举表，节点
使用 `UNAVAILABLE`，不得把 `TablePath.DEFAULT` 当作真实数据集暴露。

## 批处理、流处理、恢复和故障转移

批处理和流处理使用相同响应模型，因为二者都有 Zeta 作业 DAG。

Pipeline 重试保留同一个作业 ID 和图。旧的 savepoint resume 路径也可能复用同一个作业 ID，
因此保留同一个作业范围血缘标识。恢复提交为新作业时，包括引用源作业的提交，会获得
新的作业 ID 和独立血缘快照。第一版不创建跨作业恢复边，客户端不能跨不同作业比较节点 ID。

Active Master 切换不需要新的血缘存储。Active Master 可以通过现有 Coordinator 和
`JobImmutableInformation` 路径返回或重建 `JobDAGInfo`。已完成作业的血缘只在
`JobHistoryService` 保留对应 `JobDAGInfo` 期间可用。

## 可用性和错误语义

端点返回：

- 已知作业且 DAG 信息可用时返回 `200` 和完整图；
- 作业 ID 缺失、格式错误或非正数时返回 `400`，code 为 `INVALID_JOB_ID`；
- 作业未知或已完成历史过期时返回 `404`，code 为 `JOB_NOT_FOUND`；
- 作业已知但无法获取一致 DAG 快照时返回 `409`，code 为 `LINEAGE_UNAVAILABLE`；
- 序列化响应超过 8 MiB 时返回 `413`，code 为 `LINEAGE_GRAPH_TOO_LARGE`。

返回 `200` 前必须校验所有边引用。悬空边、重复节点 ID 或缺失必填节点字段会使快照不可用，端点
不得返回部分图。

错误响应使用一个 JSON 对象，并包含字符串字段 `code` 和 `message`，例如：

```json
{
  "code": "JOB_NOT_FOUND",
  "message": "Job lineage is not available for the requested job"
}
```

响应不能包含 Java stack trace、原始请求值、连接器配置或内部类名。客户端必须根据 `code` 分支；
`message` 是描述文本，可以在不修改 `schemaVersion` 的情况下调整。

血缘 Servlet 在请求边界负责异常转换。它必须捕获未预期的失败并返回相同的 `{code, message}`
契约，不能让异常进入共享的 `ExceptionHandlingFilter`。该过滤器现有的兜底响应会包含 stack trace，
并且使用不同的响应结构。本提案不修改其他端点使用的共享过滤器。

## 性能和负载限制

映射复杂度为 `O(nodes + edges + datasets)`，并复用已有缓存的 `JobDAGInfo`。端点不能在每次请求
时重建执行计划，也不能扫描 StainTrace 文件。

第一版提议使用 8 MiB 序列化响应限制。图是一个原子整体，超限时拒绝返回，而不是截断或分页。截断可能
产生没有节点的边，对治理工具不安全。错误中可以包含节点数和边数，但不能包含数据集名称。

实现应只序列化一次到有界缓冲区，检查字节数后写入响应，不能再创建第二份无界 JSON 副本。

## 安全

端点使用其他 Zeta REST 端点相同的 Jetty 和 `BasicAuthFilter` 边界，不增加端点级授权。

连接器名称和表路径可能泄露部署拓扑和业务数据集名称。运维人员不应在没有认证和网络控制的情况下
暴露 REST 服务。响应不能包含环境选项、连接器配置、凭证、插件 JAR URL、Master/Worker 地址或
StainTrace payload。

第一版继承现有集群级授权模型：通过 REST API 认证的调用方可以查询所有仍被保留的作业。按作业
授权不在本提案范围内。

## 兼容性和版本管理

本提案是增量能力：

- `/job-info/{jobId}` 保持不变；
- 不向 Java 序列化的 `JobDAGInfo` 或 `JobImmutableInformation` 增加字段；
- 不修改 checkpoint、savepoint 或连接器 API；
- 不增加 Hazelcast Map 或新的保留配置。

`schemaVersion` 保持 `1` 时可以增加新的可选响应字段。删除字段、改变字段语义或修改枚举值需要新
的 schema 版本。现有 REST 路径没有 URL 版本，因此响应中的版本必须明确保留。

## 实现切片

1. 增加不可变 REST 响应模型和从 `JobDAGInfo` 转换的纯 Mapper，包含确定性排序、校验、数据集状态
   处理和大小统计。
2. 增加 `JobLineageService` 和注册在 `/job-lineage` 的独立 Servlet，复用当前运行中/已完成 DAG
   查询路径，不增加存储。Servlet 在自身边界处理所有失败，并只返回上文定义的错误契约。
3. 契约获批后补充 REST API 文档以及安全和保留说明。
4. 在获得实际使用反馈后，再单独考虑 Web UI 或外部元数据目录集成。

## 测试计划

### Mapper 测试

- Source-Transform-Sink 图产生稳定的节点和边排序；
- 多个 pipeline 保留各自 pipeline ID；
- 多表 Source 和 Sink 返回排序、去重的数据集；
- Transform 和 transform chain 不声明数据集血缘；
- 不可用的数据集元数据产生明确状态和 warning；
- 悬空边或重复节点 ID 按失败关闭；
- 字节限制拒绝整个图而不是截断。

### Service 和 REST 测试

- 等待、运行和已完成作业返回相同模型；
- 支持有代表性的批处理和流处理作业；
- Follower 请求通过 Active Master 路径解析；
- Active Master 切换后血缘仍可用；
- 已完成作业血缘随现有作业 DAG 历史过期；
- 格式错误和未知作业 ID 返回受控错误；
- 启用 Basic Auth 时端点受到保护；
- 现有 fixture 的 `/job-info/{jobId}` 输出逐字节保持不变。

### 兼容性测试

- 复用作业 ID 的旧 savepoint resume 保留同一个作业范围标识；
- 使用新作业 ID 提交的恢复获得独立作业范围图；
- 现有 `JobDAGInfo` 序列化保持不变；
- 不要求配置 StainTrace；
- 不要求修改连接器实现。

## 验收标准

1. 批处理或流处理 Zeta 作业暴露一个确定的 Source-Transform-Sink 图。
2. 运行中和保留的已完成作业使用同一响应 schema。
3. 节点 ID 在作业内稳定，并明确不能跨作业使用。
4. 多表元数据不暗示不受支持的表到表或列级血缘。
5. 动态或不可用数据集元数据会被标记，而不是猜测。
6. Pipeline 重试和 Active Master 切换不改变图。
7. 单独提交的恢复作业拥有自己的图和作业范围 ID。
8. 未知、过期、不一致和超大图返回受控错误。
9. 端点不暴露配置、凭证、地址或 trace payload。
10. `/job-info/{jobId}`、checkpoint/savepoint 格式和 Java 序列化保持不变。
11. 端点不增加新的 HA 状态或保留配置。
12. 列级血缘和 Web UI 血缘视图作为独立后续工作。

## 需要社区确认的问题

1. `/job-lineage/{jobId}` 是否优于 `/job-info/{jobId}/lineage`？
2. 现有偏执行视角的 `JobDAGInfo` 是否适合作为第一版数据源，还是应该单独保留逻辑计划快照？
3. 8 MiB 是否适合作为第一版响应上限？
4. 后续是否应增加连接器能力来区分完整和部分动态发现，而第一版只把已有表路径描述为
   `REPORTED`？
