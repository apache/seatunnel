## Why

HugeGraph connector 目前仅支持 Sink（写入数据到 HugeGraph），不支持 Source（从 HugeGraph 读取数据）。用户无法通过 SeaTunnel 将 HugeGraph 中的顶点和边数据抽取到其他存储系统中，限制了数据迁移和同步场景。作为图数据库连接器，Source 和 Sink 的完整支持是基本要求，且同类型的 Neo4j 连接器已同时提供 Source 和 Sink。

## What Changes

- 新增 HugeGraph Source 连接器，支持从 HugeGraph 读取顶点（Vertex）和边（Edge）数据
- 新增 `HugeGraphSource`、`HugeGraphSourceFactory`、`HugeGraphSourceReader` 类
- 新增 `HugeGraphSourceOptions` 定义 Source 特有配置项（如 label、查询类型、属性过滤等）
- 新增 `HugeGraphSourceConfig` 封装 Source 配置
- 重构 `HugeGraphClient` 使其不再绑定 `HugeGraphSinkConfig`，支持 Source 和 Sink 共享客户端逻辑
- 在 `plugin-mapping.properties` 中注册 Source 插件
- 新增 Source 相关错误码（如 `READ_FAILED`）
- 新增 Source 单元测试和集成测试

## Capabilities

### New Capabilities
- `graph-data-reading`: 从 HugeGraph 读取顶点和边数据的能力，支持按 label 过滤、按属性选择、分页查询

### Modified Capabilities
- `graph-client`: HugeGraphClient 需要解耦对 HugeGraphSinkConfig 的依赖，改为接受通用连接参数，以支持 Source 和 Sink 共享客户端

## Impact

- **代码结构**: `client/HugeGraphClient.java` 需要重构，构造函数签名变更，影响 `sink/HugeGraphSinkWriter.java` 和 `utils/SchemaValidator.java`
- **配置**: 新增 Source 配置选项，共享选项复用 `HugeGraphOptions`
- **SPI 注册**: `plugin-mapping.properties` 需新增 `seatunnel.source.HugeGraph` 条目
- **错误码**: `HugeGraphConnectorErrorCode` 需新增读取相关错误码
- **依赖**: 无新外部依赖，复用已有的 `hugegraph-client` 1.5.0
