import ChangeLog from '../changelog/connector-jdbc.md';

# SingleStore

> JDBC SingleStore 源连接器

## 描述

通过 JDBC 从 SingleStore（原 MemSQL）读取数据。SingleStore 是兼容 MySQL 的高性能实时分析数据库。本连接器使用 JDBC 源配合 SingleStore 方言。

## 支持的 SingleStore 版本

- **SingleStore v7.1+**（已在 7.1 及更高版本上测试）。该版本范围是连接器使用的 JDBC 驱动与 MySQL 兼容 SQL（如 `SHOW TABLE STATUS`、`CRC32`、`ON DUPLICATE KEY UPDATE`）所要求的。更早版本未正式支持，可能存在兼容性差异。

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 使用依赖

### 对于 Spark/Flink 引擎

> 1. 您需要确保 [SingleStore JDBC 驱动](https://mvnrepository.com/artifact/com.singlestore/singlestore-jdbc-client) 已放置在目录 `${SEATUNNEL_HOME}/plugins/` 中。连接器使用 `singlestore-jdbc-client` 1.2.8 构建与测试；其他版本可能可用，但需自行验证兼容性与安全性。

### 对于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [SingleStore JDBC 驱动](https://mvnrepository.com/artifact/com.singlestore/singlestore-jdbc-client) 已放置在目录 `${SEATUNNEL_HOME}/lib/` 中。

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [ ] [流](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行性](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户自定义 split](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表读取](../../introduction/concepts/connector-v2-features.md)

## 支持的数据源信息

| 数据源    | 驱动                        | URL                                   | Maven                                                                 |
|----------|-----------------------------|---------------------------------------|-----------------------------------------------------------------------|
| SingleStore | com.singlestore.jdbc.Driver | jdbc:singlestore://host:3306/database | [下载](https://mvnrepository.com/artifact/com.singlestore/singlestore-jdbc-client) |

### 连接 URL 格式

SingleStore JDBC URL 格式如下：

```
jdbc:singlestore:[loadbalance:|sequential:]//<host>[:port]/[database][?<key1>=<value1>[&<key2>=<value2>]]
```

- 默认端口为 `3306`。
- 示例：`jdbc:singlestore://localhost:3306/test?user=root&password=myPassword`
- 负载均衡：`jdbc:singlestore:loadbalance://host1,host2/db`
- 顺序故障转移：`jdbc:singlestore:sequential://host1,host2/db`

## 数据类型映射

SingleStore 兼容 MySQL。数据类型映射与 [MySQL JDBC 源](Mysql.md#数据类型映射) 相同（TINYINT、INT、BIGINT、VARCHAR、TEXT、DATETIME 等）。

## 常见问题与排查

| 现象 | 可能原因 | 建议 |
|------|----------|------|
| 连接被拒绝或超时 | 主机/端口错误、防火墙或 SingleStore 未启动 | 检查 URL 格式 `jdbc:singlestore://host:port/database`，默认端口 3306，确保数据库可达。 |
| "No suitable driver" 或 ClassNotFoundException | JDBC 驱动未在 classpath | 将 `singlestore-jdbc-client` JAR 放入 `${SEATUNNEL_HOME}/plugins/`（Spark/Flink）或 `${SEATUNNEL_HOME}/lib/`（Zeta）。 |
| 分片或采样报错 | SingleStore 版本或 SQL 差异 | 使用 SingleStore 7.1+。若对 `SHOW TABLE STATUS` 或 `CRC32` 报错，请提供 SingleStore 版本信息。 |
| Upsert 或批量写入失败 | 语法或驱动行为 | 在 URL 或 properties 中设置 `rewriteBatchedStatements=true`，并确认主键列与表结构。 |
| Schema 变更（ALTER TABLE）异常 | DDL 继承自 MySQL 方言 | 在您的 SingleStore 版本上测试 ADD/MODIFY/DROP COLUMN，并记录差异。 |

## 手动集成测试

本项目中未提供 SingleStore 的 Testcontainers 镜像。若需在真实 SingleStore 实例上验证连接器：

1. 启动 SingleStore 7.1+（如 Docker 或云环境）。
2. 创建数据库与表，使用上文示例配置运行一次小规模 JDBC 源（及可选 sink）任务。
3. 验证分片（并行）与 sink 的 upsert、批量写入行为。
4. 若使用 Schema Evolution，执行 ADD/MODIFY/DROP COLUMN 并确认任务仍能正确运行。

## 源选项

所有 [JDBC 源](Jdbc.md) 连接器选项均适用。SingleStore 关键选项如下：

| 参数名     | 类型   | 必须 | 默认值 | 描述                                                                 |
|------------|--------|------|--------|----------------------------------------------------------------------|
| url        | String | 是   | -      | JDBC 连接 URL。示例：`jdbc:singlestore://localhost:3306/test`        |
| driver     | String | 是   | -      | JDBC 驱动类：`com.singlestore.jdbc.Driver`                           |
| username   | String | 否   | -      | 数据库用户名                                                         |
| password   | String | 否   | -      | 数据库密码                                                           |
| query      | String | 否   | -      | 查询语句。可使用 `query` 或 `table_path` / `table_list`               |
| table_path | String | 否   | -      | 完整表路径，如 `mydb.mytable`                                        |
| dialect    | String | 否   | -      | 可选。当 URL 不以 `jdbc:singlestore:` 开头时，设置为 `SingleStore`   |

## 示例

### 按表路径读取

```hocon
source {
  Jdbc {
    url = "jdbc:singlestore://localhost:3306/test"
    driver = "com.singlestore.jdbc.Driver"
    username = "root"
    password = "myPassword"
    table_path = "test.my_table"
  }
}
```

### 按查询读取

```hocon
source {
  Jdbc {
    url = "jdbc:singlestore://localhost:3306/test"
    driver = "com.singlestore.jdbc.Driver"
    username = "root"
    password = "myPassword"
    query = "SELECT * FROM my_table WHERE id > 100"
  }
}
```

### 带连接参数

```hocon
source {
  Jdbc {
    url = "jdbc:singlestore://localhost:3306/test?rewriteBatchedStatements=true"
    driver = "com.singlestore.jdbc.Driver"
    username = "root"
    password = "myPassword"
    table_path = "test.my_table"
    properties {
      defaultFetchSize = 1000
    }
  }
}
```

<ChangeLog />
