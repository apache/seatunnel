import ChangeLog from '../changelog/connector-jdbc.md';

# Greenplum

> Greenplum 源连接器

## 描述

通过 [Jdbc 连接器](Jdbc.md) 读取 Greenplum 数据。Greenplum 使用 PostgreSQL 协议，通常可以直接使用 PostgreSQL JDBC 驱动；如果使用 Greenplum 原生 JDBC 驱动，需要用户自行提供驱动 jar。

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 使用依赖

### Spark/Flink 引擎

> 1. 使用 `org.postgresql.Driver` 时，请确保 [PostgreSQL JDBC 驱动](https://mvnrepository.com/artifact/org.postgresql/postgresql) 已放到 `${SEATUNNEL_HOME}/plugins/`。
> 2. 使用 `com.pivotal.jdbc.GreenplumDriver` 时，请自行下载 Greenplum JDBC 驱动，并放到 `${SEATUNNEL_HOME}/plugins/`。

### SeaTunnel Zeta 引擎

> 1. 使用 `org.postgresql.Driver` 时，请确保 [PostgreSQL JDBC 驱动](https://mvnrepository.com/artifact/org.postgresql/postgresql) 已放到 `${SEATUNNEL_HOME}/lib/`。
> 2. 使用 `com.pivotal.jdbc.GreenplumDriver` 时，请自行下载 Greenplum JDBC 驱动，并放到 `${SEATUNNEL_HOME}/lib/`。

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户自定义拆分](../../introduction/concepts/connector-v2-features.md)

> 支持通过查询 SQL 选择需要的列，从而实现列投影。

## 支持的数据源信息

| 数据源 | 驱动 | URL | Maven |
|--------|------|-----|-------|
| 使用 PostgreSQL 驱动连接 Greenplum | `org.postgresql.Driver` | `jdbc:postgresql://localhost:5432/testdb` | [下载](https://mvnrepository.com/artifact/org.postgresql/postgresql) |
| 使用 Greenplum 原生驱动连接 Greenplum | `com.pivotal.jdbc.GreenplumDriver` | `jdbc:pivotal:greenplum://localhost:5432;DatabaseName=testdb` | 从 Greenplum 获取 |

:::tip

出于许可证原因，SeaTunnel 不内置 Greenplum 原生 JDBC 驱动。如果使用 `com.pivotal.jdbc.GreenplumDriver`，请在运行任务前将 `greenplum-xxx.jar` 复制到对应引擎的依赖目录。

:::

## 选项

这里只列出 Greenplum 常用配置。其他 JDBC 源端配置，例如 `fetch_size`、`connection_check_timeout_sec`、`properties`、`table_path` 和多表读取能力，继承自 [Jdbc Source](Jdbc.md)。

| 名称 | 类型 | 是否必填 | 默认值 | 描述 |
|------|------|----------|--------|------|
| url | String | 是 | - | JDBC 连接 URL。使用 PostgreSQL 驱动时格式为 `jdbc:postgresql://host:port/database`；使用 Greenplum 原生驱动时格式为 `jdbc:pivotal:greenplum://host:port;DatabaseName=database`。 |
| driver | String | 是 | - | JDBC 驱动类名，通常为 `org.postgresql.Driver` 或 `com.pivotal.jdbc.GreenplumDriver`。 |
| username | String | 否 | - | Greenplum 用户名。 |
| password | String | 否 | - | Greenplum 密码。 |
| query | String | 是 | - | 读取数据的 SQL，可以只选择需要的字段。 |
| partition_column | String | 否 | - | 用于并行拆分读取的字段，可以是数值或字符串类型。数值列按 `partition_num` 切分为数值范围；字符串列在 `split.string_split_mode = sample`（默认）下按哈希拆分，在 `charset_based` 下按字典序范围拆分。 |
| partition_lower_bound | String | 否 | - | `partition_column` 的下界；不配置时 SeaTunnel 会查询最小值。 |
| partition_upper_bound | String | 否 | - | `partition_column` 的上界；不配置时 SeaTunnel 会查询最大值。 |
| partition_num | Int | 否 | 10 | 拆分数量。数值列（以及 `charset_based` 字符串列）每个拆分使用范围查询；`sample` 字符串列每个拆分使用哈希取模谓词。未设置时默认为 `10`，可按需上调以匹配作业并行度。 |
| split.string_split_mode | String | 否 | sample | 字符串拆分算法。拆分字段是可打印 ASCII 字符串，并且希望使用确定性的范围类拆分时，可以配置为 `charset_based`。 |
| common-options | | 否 | - | 源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md)。 |

### 小贴士

> 不配置 `partition_column` 时，源端按单拆分读取；配置后，SeaTunnel 会按 `partition_num` 或作业并行度并行读取。

## 任务示例

### 读取 Greenplum 数据

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = "org.postgresql.Driver"
    url = "jdbc:postgresql://localhost:5432/testdb"
    username = "tester"
    password = "pivotal"
    query = "select age, name from source"
  }
}

sink {
  Console {}
}
```

### 按字符串字段并行读取

```hocon
source {
  Jdbc {
    driver = "org.postgresql.Driver"
    url = "jdbc:postgresql://localhost:5432/testdb"
    username = "tester"
    password = "pivotal"
    query = "select age, name from source"
    partition_column = "name"
    split.string_split_mode = charset_based
  }
}
```

### Greenplum 读写示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = "org.postgresql.Driver"
    url = "jdbc:postgresql://localhost:5432/testdb"
    username = "tester"
    password = "pivotal"
    query = "select age, name from source"
    partition_column = "name"
    split.string_split_mode = charset_based
  }
}

sink {
  Jdbc {
    driver = "org.postgresql.Driver"
    url = "jdbc:postgresql://localhost:5432/testdb"
    username = "tester"
    password = "pivotal"
    query = "insert into sink(age, name) values(?, ?)"
  }
}
```

## 变更日志

<ChangeLog />
