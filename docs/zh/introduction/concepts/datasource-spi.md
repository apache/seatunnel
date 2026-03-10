---
title: 数据源 SPI
weight: 6
---

# 数据源 SPI

## 概述

数据源 SPI（Service Provider Interface）是 SeaTunnel 引入的扩展机制，用于集中管理数据源连接配置。它允许外部元数据系统（如 Apache Gravitino、DataHub、Atlas或者Custom）管理数据源元数据，而 SeaTunnel 作业通过简单的 `datasource_id` 引用这些配置。

### 优势

- **简化配置**：数据源连接信息（URL、用户名、密码等）在外部管理，无需在多个作业配置中重复
- **增强安全性**：敏感凭据不再存储在作业配置文件中
- **集中管理**：对数据源配置的修改只需在外部系统中进行一次
- **向后兼容**：不使用 `datasource_id` 的现有作业可以继续正常工作

## datasource_id 参数

`datasource_id` 是所有 SeaTunnel 连接器都可用的通用参数。当指定此参数时，连接器将从外部元数据服务获取连接配置，而不是使用直接配置。

### 使用示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    datasource_id = "mysql-source-01"
    database = "test_db"
    table = "users"
    query = "select * from users where status = 'active'"
  }
}

sink {
  Jdbc {
    datasource_id = "mysql-sink-01"
    database = "reporting_db"
    table = "user_summary"
  }
}
```

当指定 `datasource_id` 时，连接器将：
1. 使用 `datasource_id` 从外部元数据服务获取连接详细信息
2. 将获取的配置与作业配置中的其他参数合并
3. 作业级别的参数优先于获取的配置

## 数据源 SPI 接口

### DataSourceProvider 接口

`DataSourceProvider` 接口是将外部元数据系统与 SeaTunnel 集成的入口点。它通过使用 `@AutoService` 注解的 Java SPI 机制被发现。

**位置**：`seatunnel-api/src/main/java/org/apache/seatunnel/api/datasource/DataSourceProvider.java`

```java
public interface DataSourceProvider extends AutoCloseable {

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
     * 返回此提供者支持的数据源映射器集合。
     * 每个映射器处理特定的连接器类型（Jdbc、Kafka 等）
     */
    Collection<DataSourceMapper> dataSourceMappers();

    /**
     * 关闭此提供者持有的资源。
     * 在 SeaTunnel 关闭期间调用。
     */
    @Override
    void close();
}
```

#### 生命周期

1. **发现**：提供者实例通过 `@AutoService(DataSourceProvider.class)` 被发现并缓存
2. **初始化**：使用来自 `seatunnel.yaml` 的配置调用 `init(Config)`
3. **使用**：调用 `dataSourceMappers()` 获取用于解析 `datasource_id` 的映射器
4. **清理**：关闭期间调用 `close()`

#### 资源管理

提供者负责管理其映射器所需的所有资源：
- 用于 REST API 调用的 HTTP 客户端
- 用于数据库访问的连接池
- 任何其他共享资源

映射器应通过构造函数从提供者接收资源，而不应直接持有资源。

### DataSourceMapper 接口

`DataSourceMapper` 接口将外部元数据转换为 SeaTunnel 连接器配置。

**位置**：`seatunnel-api/src/main/java/org/apache/seatunnel/api/datasource/DataSourceMapper.java`

```java
public interface DataSourceMapper {

    /**
     * 返回此映射器支持的连接器标识符。
     * 必须与 SeaTunnel 连接器的插件标识符匹配。
     * 示例："Jdbc"、"Kafka"、"MySQL-CDC"
     */
    String connectorIdentifier();

    /**
     * 将 datasource_id 映射到连接器配置。
     *
     * @param datasourceId 外部系统中的数据源 ID
     * @return 连接器的配置映射，如果映射失败则返回 null
     */
    Map<String, Object> map(String datasourceId);
}
```

#### 实现指南

- 映射器应该是轻量级且无状态的
- 通过构造函数从 `DataSourceProvider` 接收资源
- 必须是线程安全的，因为可能被并发调用
- 优雅地处理错误并返回有意义的错误消息

## 数据源配置

### seatunnel.yaml 配置

要启用数据源中心，请在 `seatunnel.yaml` 中添加以下配置：

```yaml
seatunnel:
  engine:
    datasource:
      enabled: true
      kind: gravitino
      gravitino:
        uri: http://127.0.0.1:8090
        metalake: test_metalake
```

### 配置选项

| 选项                   | 类型      | 默认值         | 描述                                          |
|----------------------|---------|-------------|---------------------------------------------|
| `enabled`            | Boolean | `false`     | 是否启用数据源中心                                   |
| `kind`               | String  | `gravitino` | 要使用的数据源提供者类型                                |
| `gravitino.uri`      | String  | -           | Gravitino 服务器 URI（当 kind=gravitino 时必填）     |
| `gravitino.metalake` | String  | -           | Gravitino metalake 名称（当 kind=gravitino 时必填） |

## Gravitino 实现

Apache Gravitino 是数据源 SPI 的默认（参考）实现。

### 概述

Gravitino 是一个面向数据和 AI 的统一元数据目录。SeaTunnel Gravitino 集成提供：
- 集中式 JDBC 数据源管理
- 安全的凭据存储
- Gravitino 和 SeaTunnel 之间的类型映射

### GravitinoDataSourceProvider

**位置**：`seatunnel-api/src/main/java/org/apache/seatunnel/api/datasource/gravitino/GravitinoDataSourceProvider.java`

Gravitino 提供者实现了 `DataSourceProvider` 接口：

```java
@AutoService(DataSourceProvider.class)
public class GravitinoDataSourceProvider implements DataSourceProvider {

    @Override
    public String kind() {
        return "gravitino";
    }

    @Override
    public void init(Config config) {
        // 验证并存储 URI 和 metalake 配置
        // 初始化用于 Gravitino API 调用的 HTTP 客户端
    }

    @Override
    public Collection<DataSourceMapper> dataSourceMappers() {
        // 返回支持的映射器列表
        // 当前仅支持 JDBC 连接器
        return Collections.singletonList(
            new GravitinoJdbcDataSourceMapper(buildMetalakeUrl(), client));
    }
}
```

### GravitinoJdbcDataSourceMapper

**位置**：`seatunnel-api/src/main/java/org/apache/seatunnel/api/datasource/gravitino/GravitinoJdbcDataSourceMapper.java`

JDBC 映射器将 Gravitino 目录属性转换为 SeaTunnel JDBC 连接器配置。

#### 属性映射

| Gravitino 属性    | SeaTunnel 属性 |
|-----------------|--------------|
| `jdbc-url`      | `url`        |
| `jdbc-user`     | `username`   |
| `jdbc-password` | `password`   |
| `jdbc-driver`   | `driver`     |

#### Gravitino 响应示例

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

## 运行时流程

1. **SeaTunnel 启动**
   - 根据 `seatunnel.yaml` 加载配置的 `DataSourceProvider`
   - 使用提供者特定的配置调用 `init()`

2. **作业提交**
   - 解析作业配置
   - 检测连接器配置中是否存在 `datasource_id`

3. **映射器解析**
   - 根据连接器标识符（如 "Jdbc"）查找匹配的 `DataSourceMapper`
   - 每种连接器类型都有自己的映射器

4. **配置获取**
   - 调用 `mapper.map(datasourceId)` 从外部系统检索配置
   - 映射器查询元数据服务并返回连接器配置

5. **配置合并**
   - 将获取的配置与作业级别的参数合并
   - 作业级别的参数优先

## 实现自定义提供者

要实现自定义数据源提供者：

1. **创建提供者类**
   ```java
   @AutoService(DataSourceProvider.class)
   public class MyDataSourceProvider implements DataSourceProvider {
       @Override
       public String kind() {
           return "my-provider";
       }

       @Override
       public void init(Config config) {
           // 初始化客户端、连接池等
       }

       @Override
       public Collection<DataSourceMapper> dataSourceMappers() {
           return Arrays.asList(new MyJdbcMapper(), new MyKafkaMapper());
       }

       @Override
       public void close() {
           // 清理资源
       }
   }
   ```

2. **创建映射器类**
   ```java
   public class MyJdbcMapper implements DataSourceMapper {
       @Override
       public String connectorIdentifier() {
           return "Jdbc";
       }

       @Override
       public Map<String, Object> map(String datasourceId) {
           // 从元数据服务获取
           // 返回 SeaTunnel 兼容的配置
       }
   }
   ```

3. **配置 seatunnel.yaml**
   ```yaml
   seatunnel:
     engine:
       datasource:
         enabled: true
         kind: my-provider
         my-provider:
           # 提供者特定的选项
   ```

4. **打包和部署**
   - 将实现包含在 SeaTunnel 的类路径中
   - `@AutoService` 注解将自动注册它
