## MODIFIED Requirements

### Requirement: HugeGraphClient accepts generic connection parameters
HugeGraphClient SHALL 支持通过通用连接参数（host, port, graphName, graphSpace, username, password, maxRetries, retryBackoffMs）构造实例，不再绑定 `HugeGraphSinkConfig`。

#### Scenario: Construct client with connection parameters
- **WHEN** 使用连接参数 host="localhost", port=8080, graphName="hugegraph" 构造 HugeGraphClient
- **THEN** SHALL 成功创建客户端并连接到 HugeGraph 服务

#### Scenario: Construct client with HugeGraphSinkConfig (backward compatible)
- **WHEN** 使用 `HugeGraphSinkConfig` 构造 HugeGraphClient
- **THEN** SHALL 从 config 中提取连接参数并正常工作，该方法标记为 `@Deprecated`

### Requirement: HugeGraphClient provides read operations
HugeGraphClient SHALL 提供从 HugeGraph 读取顶点和边数据的方法，支持分页查询。

#### Scenario: List vertices by label with pagination
- **WHEN** 调用 `listVertices(label, offset, limit)`
- **THEN** SHALL 返回指定 label 下从 offset 开始的最多 limit 条顶点记录

#### Scenario: List edges by label with pagination
- **WHEN** 调用 `listEdges(label, offset, limit)`
- **THEN** SHALL 返回指定 label 下从 offset 开始的最多 limit 条边记录

#### Scenario: Get vertex label property keys
- **WHEN** 调用 `getVertexLabelPropertyKeys(label)`
- **THEN** SHALL 返回该 vertex label 下所有 PropertyKey 的名称和类型信息

#### Scenario: Get edge label property keys
- **WHEN** 调用 `getEdgeLabelPropertyKeys(label)`
- **THEN** SHALL 返回该 edge label 下所有 PropertyKey 的名称和类型信息
