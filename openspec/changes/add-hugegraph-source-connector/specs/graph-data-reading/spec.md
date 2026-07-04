## ADDED Requirements

### Requirement: Source connector reads vertex data by label
HugeGraph Source 连接器 SHALL 支持按指定的 vertex label 从 HugeGraph 读取顶点数据，并将每条顶点记录映射为 `SeaTunnelRow` 输出。

#### Scenario: Read vertices with valid label
- **WHEN** 用户配置 type=VERTEX 且 label 指定一个已存在的 vertex label
- **THEN** 连接器 SHALL 读取该 label 下所有顶点数据并输出为 SeaTunnelRow

#### Scenario: Read vertices with non-existent label
- **WHEN** 用户配置 type=VERTEX 且 label 指定一个不存在的 vertex label
- **THEN** 连接器 SHALL 抛出 `HugeGraphConnectorException`，错误码为 `INVALID_GRAPH_SCHEMA`

### Requirement: Source connector reads edge data by label
HugeGraph Source 连接器 SHALL 支持按指定的 edge label 从 HugeGraph 读取边数据，并将每条边记录映射为 `SeaTunnelRow` 输出。

#### Scenario: Read edges with valid label
- **WHEN** 用户配置 type=EDGE 且 label 指定一个已存在的 edge label
- **THEN** 连接器 SHALL 读取该 label 下所有边数据并输出为 SeaTunnelRow

#### Scenario: Read edges with non-existent label
- **WHEN** 用户配置 type=EDGE 且 label 指定一个不存在的 edge label
- **THEN** 连接器 SHALL 抛出 `HugeGraphConnectorException`，错误码为 `INVALID_GRAPH_SCHEMA`

### Requirement: Source connector supports property selection
HugeGraph Source 连接器 SHALL 支持通过 `properties` 配置项指定要读取的属性子集。若未配置，则读取该 label 下的全部属性。

#### Scenario: Read with specified properties
- **WHEN** 用户配置 properties=["name","age"]
- **THEN** 输出的 SeaTunnelRow 仅包含 name 和 age 两个字段

#### Scenario: Read without properties configuration
- **WHEN** 用户未配置 properties
- **THEN** 输出的 SeaTunnelRow 包含该 label 下所有属性字段

### Requirement: Source connector supports page-based reading
HugeGraph Source 连接器 SHALL 使用分页方式读取数据，通过 `page_size` 配置项控制每页大小，避免一次性加载全部数据。

#### Scenario: Read with custom page size
- **WHEN** 用户配置 page_size=200
- **THEN** 连接器 SHALL 每次请求最多 200 条记录

#### Scenario: Read with default page size
- **WHEN** 用户未配置 page_size
- **THEN** 连接器 SHALL 使用默认值 500

### Requirement: Source connector supports read limit
HugeGraph Source 连接器 SHALL 支持通过 `limit` 配置项限制读取的最大记录数。

#### Scenario: Read with limit
- **WHEN** 用户配置 limit=1000 且该 label 下有 5000 条记录
- **THEN** 连接器 SHALL 仅读取前 1000 条记录后停止

#### Scenario: Read without limit
- **WHEN** 用户未配置 limit
- **THEN** 连接器 SHALL 读取该 label 下全部记录

### Requirement: Source connector produces bounded stream
HugeGraph Source 连接器 SHALL 返回 `Boundedness.BOUNDED`，表示读取为有界批处理模式。

#### Scenario: Boundedness check
- **WHEN** 调用 `HugeGraphSource.getBoundedness()`
- **THEN** SHALL 返回 `Boundedness.BOUNDED`

### Requirement: Source connector registers via SPI
HugeGraph Source 连接器 SHALL 通过 `@AutoService(Factory.class)` 注册 `HugeGraphSourceFactory`，并在 `plugin-mapping.properties` 中添加 `seatunnel.source.HugeGraph = connector-hugegraph` 条目。

#### Scenario: Plugin discovery
- **WHEN** SeaTunnel 引擎启动插件扫描
- **THEN** SHALL 能发现并加载 HugeGraph Source 连接器

### Requirement: Source connector supports schema auto-inference
当用户未显式配置 schema 时，HugeGraph Source 连接器 SHALL 从 HugeGraph 服务端获取指定 label 的 PropertyKey 定义，自动推断 SeaTunnelRowType。

#### Scenario: Auto-infer schema for vertex
- **WHEN** 用户配置 type=VERTEX、label="person" 且未配置 schema
- **THEN** 连接器 SHALL 从 HugeGraph 获取 "person" vertex label 的属性定义，构建对应的 SeaTunnelRowType

#### Scenario: User provides explicit schema
- **WHEN** 用户通过 schema 配置项显式定义字段
- **THEN** 连接器 SHALL 使用用户定义的 schema，不从服务端获取

### Requirement: Source connector maps vertex properties to SeaTunnelRow
对于顶点数据，连接器 SHALL 将顶点 ID 映射为 SeaTunnelRow 的 `id` 字段，将顶点 label 映射为 `label` 字段，将所有属性映射为对应名称的字段。

#### Scenario: Vertex field mapping
- **WHEN** 读取一个 id="v1"、label="person"、properties={name:"Alice",age:30} 的顶点
- **THEN** 输出的 SeaTunnelRow 包含字段 id="v1", label="person", name="Alice", age=30

### Requirement: Source connector maps edge properties to SeaTunnelRow
对于边数据，连接器 SHALL 将边 ID 映射为 `id` 字段，将 label 映射为 `label` 字段，将 source vertex ID 映射为 `source_id` 字段，将 target vertex ID 映射为 `target_id` 字段，将所有属性映射为对应名称的字段。

#### Scenario: Edge field mapping
- **WHEN** 读取一条 id="e1"、label="knows"、sourceId="v1"、targetId="v2"、properties={weight:0.8} 的边
- **THEN** 输出的 SeaTunnelRow 包含字段 id="e1", label="knows", source_id="v1", target_id="v2", weight=0.8
