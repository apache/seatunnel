---
title: 元数据 SPI
weight: 6
---

# 元数据 SPI

## 概述

元数据 SPI（Service Provider Interface）是 SeaTunnel 引入的扩展机制，用于集中管理数据源连接配置和表结构元数据。它允许外部元数据系统管理数据源元数据，而 SeaTunnel 作业通过简单的 `metadata_datasource_id` 引用这些配置。

### 优势

- **简化配置**：数据源连接信息（URL、用户名、密码等）在外部管理，无需在多个作业配置中重复
- **增强安全性**：敏感凭据不再存储在作业配置文件中
- **集中管理**：对数据源配置的修改只需在外部系统中进行一次
- **结构发现**：自动从元数据系统检索表结构
- **可扩展**：通过实现 `MetadataProvider` 接口可以集成自定义元数据系统

### 引擎支持

> **重要提示**：元数据 SPI 目前仅在 **SeaTunnel Zeta 引擎**上受支持，尚未兼容 Flink 或 Spark 引擎。

## 使用 metadata_datasource_id

`metadata_datasource_id` 是所有 SeaTunnel 连接器都可用的通用参数。当指定此参数时，连接器将从外部元数据服务获取连接配置，而不是使用直接配置。

### 使用示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    metadata_datasource_id = "mysql-source-01"
    database = "test_db"
    table = "users"
    query = "select * from users where status = 'active'"
  }
}

sink {
  Jdbc {
    metadata_datasource_id = "mysql-sink-01"
    database = "reporting_db"
    table = "user_summary"
  }
}
```

当指定 `metadata_datasource_id` 时，连接器将：
1. 使用 `metadata_datasource_id` 从外部元数据服务获取连接详细信息
2. 将获取的配置与作业配置中的其他参数合并
3. 作业级别的参数优先于获取的配置

## 使用 metadata_table_id

`metadata_table_id` 是可用于支持 schema 定义的连接器的 `schema` 配置中的参数。当指定此参数时，连接器将从外部元数据服务获取表结构，而不是手动定义 `columns`。

### 使用示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  LocalFile {
    path = "/tmp/data"
    file_format_type = "json"
    schema {
      table = "db.users"
      metadata_table_id = "mysql-catalog.test_db.users"
    }
  }
}
```

当在 schema 配置中指定 `metadata_table_id` 时，连接器将：
1. 使用 `metadata_table_id` 从外部元数据服务获取表结构
2. 获取的结构包括列定义、数据类型和约束
3. 无需手动定义 `columns`

详见 [Schema 特性](./schema-feature.md) 了解更多关于 schema 配置的信息。

## 元数据 SPI 规范

本节定义所有元数据提供者必须实现的标准 SPI 接口。

### MetadataProvider 接口

`MetadataProvider` 接口是将外部元数据系统与 SeaTunnel 集成的契约。实现通过使用 `@AutoService` 注解的 Java SPI 机制被发现。

**位置**：`seatunnel-api/src/main/java/org/apache/seatunnel/api/metadata/MetadataProvider.java`

```java
public interface MetadataProvider extends AutoCloseable {

    /**
     * 返回此提供者的唯一标识符。
     * 必须与 seatunnel.yaml 配置中的 "kind" 值匹配。
     * 示例："gravitino"、"datahub"、"atlas"、"custom"
     */
    String kind();

    /**
     * 使用来自 seatunnel.yaml 的配置初始化提供者。
     * 在 SeaTunnel 启动期间调用一次。
     *
     * @param config 提供者特定的配置
     */
    void init(Config config);

    /**
     * 将 metadata_datasource_id 映射到连接器配置。
     *
     * @param connectorIdentifier 连接器标识符（例如："Jdbc"、"Kafka"）
     * @param metaDataDatasourceId 外部系统中的数据源 ID
     * @return 连接器的配置映射，如果映射失败则返回 null
     */
    Map<String, Object> datasourceMap(String connectorIdentifier, String metaDataDatasourceId);

    /**
     * 获取给定元数据表 ID 的表结构。
     *
     * <p>此方法从外部元数据系统获取表元数据，包括列定义、数据类型和约束。
     *
     * @param metaDataTableId 外部元数据系统中的表 ID
     * @return 如果找到则返回表结构，否则返回空
     */
    Optional<TableSchema> tableSchema(String metaDataTableId);

    /**
     * 关闭此提供者持有的资源。
     * 在 SeaTunnel 关闭期间调用。
     */
    @Override
    void close();
}
```

### 生命周期

1. **发现**：提供者实例通过 `@AutoService(MetadataProvider.class)` 被发现并缓存
2. **初始化**：使用来自 `seatunnel.yaml` 的配置调用 `init(Config)`
3. **使用**：调用 `datasourceMap(String, String)` 来解析每个连接器的 `metadata_datasource_id`
4. **结构检索**：调用 `tableSchema(String)` 来获取表结构
5. **清理**：关闭期间调用 `close()`

### 资源管理

提供者负责管理数据源映射所需的所有资源：
- 用于 REST API 调用的 HTTP 客户端
- 用于数据库访问的连接池
- 任何其他共享资源

这些资源应在 `init()` 中创建，在多次 `datasourceMap()` 和 `tableSchema()` 调用中重用，并在 `close()` 中清理。

## 配置

以下配置示例以 **Gravitino 作为默认提供者**为例。如需使用其他提供者，请相应调整 `kind` 和提供者特定的选项。

### seatunnel.yaml 配置

要启用元数据中心，请在 `seatunnel.yaml` 中添加以下配置：

```yaml
seatunnel:
  engine:
     metadata:
      enabled: true
      kind: gravitino
      gravitino:
        uri: http://127.0.0.1:8090
        metalake: test_metalake
        sensitive_keys:
          - password
          - secret_key
```

`sensitive_keys` 为可选配置，用于定义动态元数据数据源查询响应中需要脱敏的
datasource property 字段名。未配置时，SeaTunnel 默认不脱敏。

### 配置选项

| 选项                   | 类型      | 默认值         | 描述                                          |
|----------------------|---------|-------------|---------------------------------------------|
| `enabled`            | Boolean | `false`     | 是否启用元数据中心                                   |
| `kind`               | String  | `gravitino` | 要使用的元数据提供者类型                                |
| `gravitino.uri`      | String  | -           | Gravitino 服务器 URI（当 kind=gravitino 时必填）     |
| `gravitino.metalake` | String  | -           | Gravitino metalake 名称（当 kind=gravitino 时必填） |

## 内置实现：DynamicMetadataProvider

`DynamicMetadataProvider` 是 Zeta 引擎内置的 Metadata SPI 实现。它使用 Hazelcast 分布式 `IMap` 在 SeaTunnel 集群中保存数据源连接配置，并提供 REST API 用于在运行时创建、查询、更新和删除数据源定义。

当你希望通过 API 动态管理数据源连接信息，而不是在每个作业配置文件中维护连接参数时，可以使用该提供者。

### 支持能力

- 通过 REST API 注册数据源连接属性
- 在 source 和 sink 连接器中通过 `metadata_datasource_id` 引用已注册的数据源
- 不修改作业配置模板即可更新数据源属性
- 将数据源定义保存在 Zeta 集群运行时状态中

> **注意**：`DynamicMetadataProvider` 目前只支持数据源配置查询，不支持通过 `metadata_table_id` 获取表结构。

### 启用 DynamicMetadataProvider

在 `seatunnel.yaml` 中配置如下内容：

```yaml
seatunnel:
  engine:
    http:
      enable-http: true
      port: 8080
    metadata:
      enabled: true
      kind: dynamic
```

REST API 使用 Zeta 引擎的 HTTP 服务，因此必须启用 `seatunnel.engine.http.enable-http`。

### 数据源 REST API

#### 创建数据源

```bash
curl -X POST http://localhost:8080/metadata/datasource \
  -H 'Content-Type: application/json' \
  -d '{
    "metadataDatasourceId": "mysql_source",
    "connectorType": "Jdbc",
    "properties": {
      "url": "jdbc:mysql://mysql:3306/seatunnel",
      "driver": "com.mysql.cj.jdbc.Driver",
      "username": "root",
      "password": "secret"
    }
  }'
```

响应：

```json
{
  "status": "success",
  "metadataDatasourceId": "mysql_source",
  "message": "Datasource created successfully"
}
```

#### 查询单个数据源

```bash
curl http://localhost:8080/metadata/datasource/mysql_source
```

#### 查询数据源列表

```bash
curl http://localhost:8080/metadata/datasources
```

#### 更新数据源

```bash
curl -X PUT http://localhost:8080/metadata/datasource/mysql_source \
  -H 'Content-Type: application/json' \
  -d '{
    "properties": {
      "url": "jdbc:mysql://mysql:3306/seatunnel",
      "driver": "com.mysql.cj.jdbc.Driver",
      "username": "root",
      "password": "new-secret"
    }
  }'
```

#### 删除数据源

```bash
curl -X DELETE http://localhost:8080/metadata/datasource/mysql_source
```

### 在作业中使用已注册的数据源

注册数据源后，在连接器配置中通过 `metadata_datasource_id` 引用。提供者会将已注册的 `properties` 合并到连接器配置中；作业级别配置优先于提供者返回的配置。

```hocon
env {
  execution.parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    metadata_datasource_id = "mysql_source"
    query = "select * from source"
  }
}

sink {
  Jdbc {
    metadata_datasource_id = "mysql_source"
    database = "seatunnel"
    generate_sink_sql = true
    table = "sink"
  }
}
```

### 请求字段

| 字段                     | 是否必填 | 描述                                                       |
|------------------------|------|----------------------------------------------------------|
| `metadataDatasourceId` | 是    | 数据源唯一 ID，在作业配置中通过 `metadata_datasource_id` 引用            |
| `connectorType`        | 是    | 连接器标识，例如 `Jdbc`；必须与作业中使用的连接器匹配                       |
| `properties`           | 是    | 连接器连接属性，例如 `url`、`driver`、`username`、`password` |

### 限制

- 仅支持 Zeta 引擎。
- 数据源定义保存在 Zeta 集群运行时状态中。
- 查询响应会对密码、token 等敏感字段做脱敏处理。
- `connectorType` 会与作业中的连接器标识进行校验。例如，使用 `connectorType: "Jdbc"` 创建的数据源只能被 `Jdbc` 连接器使用。
- `DynamicMetadataProvider` 不支持通过 `metadata_table_id` 获取表结构。

### 配置动态数据源持久化，避免集群重启后丢失

`DynamicMetadataProvider` 将数据源定义保存在名为 `engine_metadataDatasource` 的 Hazelcast IMap 中。默认情况下，Hazelcast IMap 数据只保存在内存中。如果所有 Zeta 引擎节点都停止，已注册的数据源定义会丢失，除非配置 IMap 持久化。

如需在集群完全重启后保留动态元数据数据源定义，请在 `hazelcast.yaml` 中为 `engine_metadataDatasource` 配置 MapStore 持久化。

使用 HDFS 的示例：

```yaml
hazelcast:
  map:
    engine_metadataDatasource:
      map-store:
        enabled: true
        initial-mode: EAGER
        write-delay-seconds: 0
        factory-class-name: org.apache.seatunnel.engine.server.persistence.FileMapStoreFactory
        properties:
          type: hdfs
          namespace: /tmp/seatunnel/imap
          clusterName: seatunnel-cluster
          storage.type: hdfs
          fs.defaultFS: hdfs://localhost:9000
```

单节点集群或共享挂载目录可以使用本地文件路径：

```yaml
hazelcast:
  map:
    engine_metadataDatasource:
      map-store:
        enabled: true
        initial-mode: EAGER
        write-delay-seconds: 0
        factory-class-name: org.apache.seatunnel.engine.server.persistence.FileMapStoreFactory
        properties:
          type: hdfs
          namespace: /tmp/seatunnel/imap
          clusterName: seatunnel-cluster
          storage.type: hdfs
          fs.defaultFS: file:///
```

配置说明：

- `engine_metadataDatasource` 是 `DynamicMetadataProvider` 使用的固定 IMap 名称。
- `initial-mode: EAGER` 表示集群启动时加载已持久化的数据源定义。
- `write-delay-seconds: 0` 表示同步写入，降低关闭前最后一次 REST API 修改丢失的概率。
- 多节点集群应使用 HDFS、OSS、S3 或共享挂载文件系统等共享外部存储。节点本地路径只适合单节点部署。
- 集群重启后需要保持相同的 `namespace` 和 `clusterName`，否则 SeaTunnel 会读取到不同的持久化位置。

更多 IMap 持久化选项请参考 [IMap 持久化配置](../../engines/zeta/hybrid-cluster-deployment.md#53-imap持久化配置)。

## 默认实现：Gravitino

Apache Gravitino 是元数据 SPI 的默认实现。要使用 Gravitino 作为元数据中心，必须将 `metadata.enabled` 设置为 `true`，并明确指定 `kind` 为 `gravitino`，同时配置所需的参数。

### metadata_datasource_id 配置

当使用 Gravitino 作为元数据中心时，`metadata_datasource_id` 的值应配置为 Gravitino 中 **catalog 的名称**。

例如，如果 Gravitino 中有一个名为 `mysql-catalog` 的 catalog，则直接将其作为 `metadata_datasource_id` 使用：

```hocon
source {
  Jdbc {
    metadata_datasource_id = "mysql-catalog"
    database = "test_db"
    table = "users"
  }
}
```

### 表结构检索

`tableSchema(String metaDataTableId)` 方法允许 SeaTunnel 自动从 Gravitino 获取表结构。使用此功能时，包括列定义、数据类型和约束的表结构将在无需手动配置的情况下获取。

对于 Gravitino，`metaDataTableId` 应格式化为 `{catalog}.{schema}.{table}`。例如，`mysql-catalog.test_db.users`。

有关 Gravitino 类型如何映射到 SeaTunnel 类型的详细信息，请参阅 [Gravitino 类型映射](./gravitino-type-mapping.md)。

### 属性映射

Gravitino 提供者执行**有限的属性名映射**，从 Gravitino 目录属性映射到 SeaTunnel 连接器配置。**仅支持以下四种属性映射**：

| Gravitino 属性    | SeaTunnel 属性 |
|-----------------|--------------|
| `jdbc-url`      | `url`        |
| `jdbc-user`     | `username`   |
| `jdbc-password` | `password`   |
| `jdbc-driver`   | `driver`     |

> **注意**：Gravitino 目录中的任何其他属性不会传递。如果您需要额外的属性映射，请考虑实现自定义的 `MetadataProvider`。

### 连接器支持

Gravitino 提供者目前支持：
- **Jdbc** 连接器（完全支持）

### 示例

#### Gravitino 目录响应

```json
{
  "code": 0,
  "catalog": {
    "name": "mysql-catalog",
    "type": "relational",
    "provider": "jdbc-mysql",
    "properties": {
      "jdbc-url": "jdbc:mysql://localhost:3306/",
      "jdbc-user": "root",
      "jdbc-password": "secret",
      "jdbc-driver": "com.mysql.cj.jdbc.Driver"
    }
  }
}
```

#### 映射后的 SeaTunnel 配置

```hocon
{
  url = "jdbc:mysql://localhost:3306/"
  username = "root"
  password = "secret"
  driver = "com.mysql.cj.jdbc.Driver"
}
```

## 实现自定义提供者

要将自定义元数据系统与 SeaTunnel 集成，请实现 `MetadataProvider` 接口。

### 步骤 1：添加依赖

将 `seatunnel-api` 依赖添加到项目的 `pom.xml` 中：

```xml
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>seatunnel-api</artifactId>
    <version>${seatunnel.version}</version>
    <scope>provided</scope>
</dependency>
```

> **注意**：使用 `<scope>provided</scope>`，因为 SeaTunnel 在运行时已包含此依赖。

### 步骤 2：创建提供者类

```java
@AutoService(MetadataProvider.class)
public class MyMetadataProvider implements MetadataProvider {

    private HttpClient httpClient;

    @Override
    public String kind() {
        return "my-provider";
    }

    @Override
    public void init(Config config) {
        // 初始化客户端、连接池等
        this.httpClient = HttpClient.newBuilder().build();
    }

    @Override
    public Map<String, Object> datasourceMap(String connectorIdentifier, String metaDataDatasourceId) {
        // 根据连接器类型从元数据服务获取
        // 返回 SeaTunnel 兼容的配置
        switch (connectorIdentifier.toLowerCase()) {
            case "jdbc":
                return fetchJdbcConfig(metaDataDatasourceId);
            case "kafka":
                return fetchKafkaConfig(metaDataDatasourceId);
            default:
                return Collections.emptyMap();
        }
    }

    @Override
    public Optional<TableSchema> tableSchema(String metaDataTableId) {
        // 从元数据服务获取表结构
        // 解析表 ID 并返回结构
        return Optional.ofNullable(fetchTableSchema(metaDataTableId));
    }

    @Override
    public void close() {
        // 清理资源
        if (httpClient != null) {
            // 清理 HTTP 客户端
        }
    }
}
```

### 步骤 3：配置 seatunnel.yaml

```yaml
seatunnel:
  engine:
     metadata:
      enabled: true
      kind: my-provider
      my-provider:
        endpoint: https://my-metadata-service.com
        api-key: your-api-key
```

### 步骤 4：打包和部署

- 将实现包含在 SeaTunnel 的类路径中
- `@AutoService` 注解将通过 Java SPI 自动注册

## 运行时流程

1. **SeaTunnel 启动**
   - 根据 `seatunnel.yaml` 加载配置的 `MetadataProvider`
   - 使用提供者特定的配置调用 `init()`

2. **作业提交**
   - 解析作业配置
   - 检测连接器配置中是否存在 `metadata_datasource_id`

3. **配置获取**
   - 调用 `provider.datasourceMap(connectorIdentifier, metaDataDatasourceId)` 从外部系统检索配置
   - 提供者查询元数据服务并返回连接器配置

4. **配置合并**
   - 将获取的配置与作业级别的参数合并
   - 作业级别的参数优先

5. **结构检索**（如适用）
   - 调用 `provider.tableSchema(metaDataTableId)` 获取表结构
   - 提供者返回包含列定义和类型的表结构
