## Context

HugeGraph connector 当前仅实现 Sink（写入），缺少 Source（读取）。现有代码结构：

- `client/HugeGraphClient` 绑定 `HugeGraphSinkConfig`，无法被 Source 复用
- `config/HugeGraphOptions` 定义了共享连接选项（HOST/PORT/GRAPH_NAME 等），可复用
- Sink 使用 `AbstractSimpleSink` 模式，Source 应使用 `AbstractSingleSplitSource` 单分片模式（与 Neo4j Source 一致）
- HugeGraph Client 1.5.0 的 `GraphManager` 提供 `listVertices()`/`listEdges()` 分页查询 API

参考同类图数据库连接器 Neo4j 的 Source 实现：`Neo4jSource` -> `Neo4jSourceFactory` -> `Neo4jSourceReader`，采用单分片有界读取模式。

## Goals / Non-Goals

**Goals:**
- 实现 HugeGraph Source 连接器，支持按 label 读取顶点和边数据
- 支持属性过滤和属性选择
- 复用现有 `HugeGraphOptions` 连接配置
- 重构 `HugeGraphClient` 使其可被 Source 和 Sink 共享
- 遵循 SeaTunnel Source 连接器标准模式（单分片有界读取）

**Non-Goals:**
- 不实现并行分片读取（HugeGraph REST API 的分页机制不适合多分片拆分）
- 不实现增量读取/CDC（HugeGraph 不支持变更日志）
- 不实现 Gremlin 自定义查询（首期仅支持按 label 的标准查询）
- 不修改 Sink 的已有功能和行为

## Decisions

### 1. 使用单分片模式（AbstractSingleSplitSource）

**选择**: 继承 `AbstractSingleSplitSource<SeaTunnelRow>`，创建 `HugeGraphSourceReader` 继承 `AbstractSingleSplitReader<SeaTunnelRow>`

**理由**: HugeGraph REST API 通过 `GraphManager.listVertices()`/`listEdges()` 分页查询，所有数据属于同一个逻辑查询，无法按范围拆分为独立分片。单分片模式实现简单，与 Neo4j/Redis 等同类连接器一致。

**备选方案**: 实现并行分片（按 label 拆分），但增加了复杂度且 HugeGraph 单个 label 的数据量通常不大，收益不明显。

### 2. HugeGraphClient 重构为接受通用连接参数

**选择**: 新增基于连接参数（host, port, graphName, username, password, maxRetries, retryBackoffMs）的构造函数，保留原有 `HugeGraphSinkConfig` 构造函数标记 `@Deprecated`。

**理由**: Source 和 Sink 共享连接、重试、重连逻辑，但配置来源不同（`HugeGraphSourceConfig` vs `HugeGraphSinkConfig`）。提取通用参数避免客户端类绑定特定配置类。

**备选方案**: 将连接参数提取为独立的 `HugeGraphConnectionConfig` 类，被 SourceConfig 和 SinkConfig 组合使用。但改动范围更大，当前阶段不必要。

### 3. Source 配置项设计

**选择**: 新增 `HugeGraphSourceOptions` 定义 Source 特有选项：
- `label` (required): 要读取的顶点或边 label 名称
- `type` (required): 读取类型，VERTEX 或 EDGE
- `properties` (optional): 要读取的属性列表，为空则读取全部属性
- `page_size` (optional, default 500): 分页查询每页大小
- `limit` (optional): 读取记录数上限

复用 `HugeGraphOptions` 中的 HOST、PORT、GRAPH_NAME、GRAPH_SPACE、USERNAME、PASSWORD。

**理由**: 与 Sink 的配置风格一致（共享连接选项 + 特有业务选项）。`label` + `type` 唯一确定一个读取目标。

### 4. 数据读取方式

**选择**: 使用 `GraphManager.listVertices(String label, int limit)` / `GraphManager.listEdges(String label, int limit)` 进行分页查询，通过 offset+limit 模式逐页读取。

**理由**: HugeGraph Client 1.5.0 的 `GraphManager` 提供基于 label 的列表查询 API，支持 limit 参数。分页读取避免一次性加载大量数据导致 OOM。

### 5. CatalogTable 和 SeaTunnelRowType 的构建

**选择**: 在 `HugeGraphSourceFactory.createSource()` 中，根据用户配置的 schema 构建 CatalogTable；若用户未配置 schema，则从 HugeGraph 服务端获取 label 对应的 PropertyKey 定义自动推断 rowType。

**理由**: 与 Neo4j Source 的模式一致（用户可配置 schema 或自动推断）。自动推断提升易用性，用户配置 schema 提供精确控制。

## Risks / Trade-offs

- **[HugeGraph 分页 API 限制]** HugeGraph REST API 的分页可能不支持 offset 模式，仅支持基于 page token 的游标分页 -> 使用 `GraphManager` 提供的迭代器接口，若不支持 offset 则使用 page token 方式

- **[HugeGraphClient 重构影响 Sink]** 修改 HugeGraphClient 构造函数可能影响现有 Sink -> 保留旧构造函数标记 `@Deprecated`，确保 Sink 功能不受影响

- **[大数量读取性能]** 单分片读取无法利用并行加速 -> 首期接受此限制，后续可扩展为多分片模式

- **[自动 schema 推断的网络开销]** 需要额外请求 HugeGraph 服务端获取 PropertyKey 定义 -> 仅在用户未配置 schema 时触发，属于一次性开销
