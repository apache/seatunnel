import ChangeLog from '../changelog/connector-databend.md';

# Databend

> Databend 源连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要功能

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持多表读](../../introduction/concepts/connector-v2-features.md)

## 描述

用于从 Databend 读取数据的源连接器。

## 依赖

### 对于 Spark/Flink

> 1. 你需要下载 [Databend JDBC driver jar package](https://github.com/databendlabs/databend-jdbc/) 并添加到目录 `${SEATUNNEL_HOME}/plugins/`.

### 对于 SeaTunnel Zeta

> 1. 你需要下载 [Databend JDBC driver jar package](https://github.com/databendlabs/databend-jdbc/) 并添加到目录 `${SEATUNNEL_HOME}/lib/`.

## 支持的数据源信息

| 数据源 | 支持版本 | 驱动 | Url | Maven |
|--------|----------|------|-----|-------|
| Databend | 1.2.x 及以上版本 | - | - | - |

## 数据类型映射

| Databend 数据类型 | SeaTunnel 数据类型 |
|-----------------|------------------|
| BOOLEAN | BOOLEAN |
| TINYINT | TINYINT |
| SMALLINT | SMALLINT |
| INT | INT |
| BIGINT | BIGINT |
| FLOAT | FLOAT |
| DOUBLE | DOUBLE |
| DECIMAL | DECIMAL |
| STRING | STRING |
| VARCHAR | STRING |
| CHAR | STRING |
| TIMESTAMP | TIMESTAMP |
| DATE | DATE |
| TIME | TIME |
| BINARY | BYTES |

## 源选项

基础配置:

| 名称 | 类型 | 是否必须 | 默认值 | 描述 |
|------|------|----------|--------|------|
| url | String | 是 | - | Databend JDBC 连接 URL |
| username | String | 是 | - | Databend 数据库用户名 |
| password | String | 是 | - | Databend 数据库密码 |
| database | String | 否 | - | Databend 数据库名称，默认使用连接 URL 中指定的数据库名 |
| table | String | 否 | - | Databend 表名称 |
| query | String | 否 | - | Databend 查询语句。如果设置，会覆盖 database 和 table 的设置 |
| sql | String | 否 | - | 自定义 SQL 语句。若同时配置 `sql` 和 `query`，优先使用 `sql` |
| fetch_size | Integer | 否 | 1 | 每次从 Databend 拉取的记录数。读取大量数据时可以适当调大 |
| ssl | Boolean | 否 | false | 是否使用 SSL 连接 Databend |
| jdbc_config | Map | 否 | - | 额外的 JDBC 连接配置，如加载均衡策略等 |

必须配置 `sql`、`query`、或同时配置 `database` 和 `table`。当前连接器不支持
`table_list`，如果要读多张表，请为每张表分别配置一个 Databend source。

## 任务示例

### 单表读取

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  Databend {
    url = "jdbc:databend://localhost:8000"
    username = "root"
    password = ""
    database = "default"
    table = "users"
  }
}

sink {
  Console {}
}
```

### 使用自定义查询

```hocon
source {
  Databend {
    url = "jdbc:databend://localhost:8000"
    username = "root"
    password = ""
    query = "SELECT id, name, age FROM default.users WHERE age > 18"
  }
}
```

### 使用 SSL

```hocon
source {
  Databend {
    url = "jdbc:databend://databend.example.com:8000/default"
    username = "root"
    password = ""
    sql = "SELECT * FROM default.users"
    ssl = true
    fetch_size = 1000
  }
}
```

## 相关链接

- [Databend 官方网站](https://databend.rs/)
- [Databend JDBC 驱动](https://github.com/databendlabs/databend-jdbc/)

## Changelog

<ChangeLog />
