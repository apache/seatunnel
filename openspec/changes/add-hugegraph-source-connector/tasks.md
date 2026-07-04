## 1. HugeGraphClient 重构

- [ ] 1.1 为 HugeGraphClient 新增基于通用连接参数的构造函数（host, port, graphName, graphSpace, username, password, maxRetries, retryBackoffMs）
- [ ] 1.2 将现有 HugeGraphSinkConfig 构造函数标记为 @Deprecated，内部委托到新构造函数
- [ ] 1.3 在 HugeGraphClient 中新增 listVertices(label, offset, limit) 分页读取方法
- [ ] 1.4 在 HugeGraphClient 中新增 listEdges(label, offset, limit) 分页读取方法
- [ ] 1.5 在 HugeGraphClient 中新增 getVertexLabelPropertyKeys(label) 方法，返回属性名和类型列表
- [ ] 1.6 在 HugeGraphClient 中新增 getEdgeLabelPropertyKeys(label) 方法，返回属性名和类型列表
- [ ] 1.7 验证 Sink 功能不受重构影响，现有测试通过

## 2. Source 配置类

- [ ] 2.1 创建 HugeGraphSourceOptions 类，定义 LABEL、TYPE、PROPERTIES、PAGE_SIZE、LIMIT 配置项
- [ ] 2.2 创建 HugeGraphSourceConfig 类，包含连接参数和 Source 特有配置，提供 of(ReadonlyConfig) 静态工厂方法

## 3. Source 连接器核心实现

- [ ] 3.1 创建 HugeGraphSource 类，继承 AbstractSingleSplitSource<SeaTunnelRow>，实现 getPluginName/getBoundedness/getProducedCatalogTables/createReader
- [ ] 3.2 创建 HugeGraphSourceReader 类，继承 AbstractSingleSplitReader<SeaTunnelRow>，实现 open/internalPollNext/close 方法
- [ ] 3.3 在 HugeGraphSourceReader.internalPollNext 中实现分页循环读取逻辑，将 Vertex/Edge 映射为 SeaTunnelRow
- [ ] 3.4 实现顶点字段映射：id、label、各属性字段
- [ ] 3.5 实现边字段映射：id、label、source_id、target_id、各属性字段
- [ ] 3.6 实现 properties 过滤逻辑：仅输出用户指定的属性子集
- [ ] 3.7 实现 limit 限制：达到 limit 后停止读取并 signalNoMoreElement

## 4. Source Factory 和 SPI 注册

- [ ] 4.1 创建 HugeGraphSourceFactory 类，实现 TableSourceFactory，添加 @AutoService(Factory.class)
- [ ] 4.2 在 HugeGraphSourceFactory 中实现 optionRule()，定义必填和可选配置项
- [ ] 4.3 在 HugeGraphSourceFactory 中实现 createSource()，支持用户配置 schema 和自动推断 schema
- [ ] 4.4 在 plugin-mapping.properties 中添加 seatunnel.source.HugeGraph = connector-hugegraph

## 5. Schema 自动推断

- [ ] 5.1 实现 schema 自动推断逻辑：从 HugeGraph 服务端获取 PropertyKey 定义，转换为 SeaTunnelRowType
- [ ] 5.2 实现 HugeGraph 数据类型到 SeaTunnel 数据类型的映射

## 6. 错误码和异常处理

- [ ] 6.1 在 HugeGraphConnectorErrorCode 中新增 READ_FAILED 错误码
- [ ] 6.2 在 Source 读取流程中添加异常处理：label 不存在、连接失败、分页读取失败等场景

## 7. 测试

- [ ] 7.1 编写 HugeGraphSourceConfig 单元测试
- [ ] 7.2 编写 HugeGraphSourceReader 顶点读取单元测试
- [ ] 7.3 编写 HugeGraphSourceReader 边读取单元测试
- [ ] 7.4 编写 schema 自动推断单元测试
- [ ] 7.5 在 HugeGraphIT 中添加 Source 集成测试：读取顶点、读取边、属性过滤、limit 限制
