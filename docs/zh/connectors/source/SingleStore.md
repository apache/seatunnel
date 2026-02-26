import ChangeLog from '../changelog/connector-jdbc.md';

# SingleStore

> JDBC SingleStore 源连接器

## 描述

通过 JDBC 从 SingleStore（原 MemSQL）读取数据。SingleStore 是兼容 MySQL 的高性能实时分析数据库。本连接器使用 JDBC 源配合 SingleStore 方言。

## 支持的 SingleStore 版本

- SingleStore v7.1+

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
    user = "root"
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
    user = "root"
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
    user = "root"
    password = "myPassword"
    table_path = "test.my_table"
    properties {
      defaultFetchSize = 1000
    }
  }
}
```
