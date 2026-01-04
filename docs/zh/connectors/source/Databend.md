import ChangeLog from '../changelog/connector-databend.md';

# Databend

> Databend 源连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要功能

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../concept/connector-v2-features.md)
- [ ] [支持多表读](../../concept/connector-v2-features.md)

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
| query | String | 否 | - | Databend 查询语句，如果设置将覆盖 database 和 table 的设置 |
| fetch_size | Integer | 否 | 0 | 一次从数据库中获取的记录数，设置为0使用JDBC驱动默认值 |
| jdbc_config | Map | 否 | - | 额外的 JDBC 连接配置，如加载均衡策略等 |

表清单配置:

| 名称 | 类型 | 是否必须 | 默认值 | 描述 |
|------|------|----------|--------|------|
| database | String | 是 | - | 数据库名称 |
| table | String | 是 | - | 表名称 |
| query | String | 否 | - | 自定义查询语句 |
| fetch_size | Integer | 否 | 0 | 一次从数据库中获取的记录数 |

注意: 当此配置对应于单个表时，您可以将 table_list 中的配置项展平到外层。

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

## 相关链接

- [Databend 官方网站](https://databend.rs/)
- [Databend JDBC 驱动](https://github.com/databendlabs/databend-jdbc/)

## Changelog

<ChangeLog />