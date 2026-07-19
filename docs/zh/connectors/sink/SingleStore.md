import ChangeLog from '../changelog/connector-jdbc.md';

# SingleStore

> JDBC SingleStore 目标连接器

## 描述

通过 JDBC 将数据写入 SingleStore（原 MemSQL）。SingleStore 是兼容 MySQL 的高性能实时分析数据库。本连接器使用 JDBC 目标配合 SingleStore 方言，支持通过 `ON DUPLICATE KEY UPDATE` 实现 upsert。

## 支持的 SingleStore 版本

- **SingleStore v7.1+**（已在 7.1 及更高版本上测试）。连接器所需的 JDBC 驱动与 MySQL 兼容 SQL 详见 [SingleStore 源](../source/SingleStore.md)。

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 使用依赖

### 对于 Spark/Flink 引擎

> 1. 您需要确保 [SingleStore JDBC 驱动](https://mvnrepository.com/artifact/com.singlestore/singlestore-jdbc-client) 已放置在目录 `${SEATUNNEL_HOME}/plugins/` 中。

### 对于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [SingleStore JDBC 驱动](https://mvnrepository.com/artifact/com.singlestore/singlestore-jdbc-client) 已放置在目录 `${SEATUNNEL_HOME}/lib/` 中。

## 关键特性

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [x] 通过主键实现 Upsert（ON DUPLICATE KEY UPDATE）

## 支持的数据源信息

| 数据源    | 驱动                        | URL                                   | Maven                                                                 |
|----------|-----------------------------|---------------------------------------|-----------------------------------------------------------------------|
| SingleStore | com.singlestore.jdbc.Driver | jdbc:singlestore://host:3306/database | [下载](https://mvnrepository.com/artifact/com.singlestore/singlestore-jdbc-client) |

### 连接 URL 格式

与 [SingleStore 源](../source/SingleStore.md#连接-url-格式) 相同：`jdbc:singlestore://host:port/database[?params]`

## 常见问题与排查

连接、驱动与 URL 问题请参见 [SingleStore 源常见问题](../source/SingleStore.md#常见问题与排查)。针对 sink 的 upsert、批量写入或 schema 变更问题，建议先用小表验证主键与 `rewriteBatchedStatements=true`。

## 目标选项

所有 [JDBC 目标](Jdbc.md) 连接器选项均适用。SingleStore 关键选项如下：

| 参数名         | 类型    | 必须 | 默认值 | 描述                                                                 |
|----------------|--------|------|--------|----------------------------------------------------------------------|
| url            | String | 是   | -      | JDBC 连接 URL。示例：`jdbc:singlestore://localhost:3306/test`        |
| driver         | String | 是   | -      | JDBC 驱动类：`com.singlestore.jdbc.Driver`                           |
| username       | String | 否   | -      | 数据库用户名                                                         |
| password       | String | 否   | -      | 数据库密码                                                           |
| database       | String | 否   | -      | 目标数据库（与 `table` 配合使用）                                    |
| table          | String | 否   | -      | 目标表名（与 `database` 配合使用）                                   |
| primary_keys   | Array  | 否   | -      | 主键列，用于 upsert                                                  |
| dialect        | String | 否   | -      | 可选。当 URL 不以 `jdbc:singlestore:` 开头时，设置为 `SingleStore`   |
| enable_upsert   | Boolean | 否 | true   | 设置 primary_keys 时启用 upsert（ON DUPLICATE KEY UPDATE）           |

## 示例

### 写入表

```hocon
sink {
  Jdbc {
    url = "jdbc:singlestore://localhost:3306/test"
    driver = "com.singlestore.jdbc.Driver"
    user = "root"
    password = "myPassword"
    database = "test"
    table = "my_table"
    primary_keys = ["id"]
  }
}
```

### 带批量和属性

```hocon
sink {
  Jdbc {
    url = "jdbc:singlestore://localhost:3306/test?rewriteBatchedStatements=true"
    driver = "com.singlestore.jdbc.Driver"
    user = "root"
    password = "myPassword"
    database = "test"
    table = "my_table"
    primary_keys = ["id"]
    batch_size = 1000
    properties {
      rewriteBatchedStatements = true
    }
  }
}
```

<ChangeLog />
