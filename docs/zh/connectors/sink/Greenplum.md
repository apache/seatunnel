import ChangeLog from '../changelog/connector-jdbc.md';

# Greenplum

> Greenplum Sink 连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

使用 [Jdbc 连接器](Jdbc.md) 将数据写入 Greenplum。Greenplum 使用 PostgreSQL 协议，通常可以直接使用 PostgreSQL JDBC 驱动；如果使用 Greenplum 原生 JDBC 驱动，需要用户自行提供驱动 jar。

## 使用依赖

### Spark/Flink 引擎

> 1. 使用 `org.postgresql.Driver` 时，请确保 [PostgreSQL JDBC 驱动](https://mvnrepository.com/artifact/org.postgresql/postgresql) 已放到 `${SEATUNNEL_HOME}/plugins/`。
> 2. 使用 `com.pivotal.jdbc.GreenplumDriver` 时，请自行下载 Greenplum JDBC 驱动，并放到 `${SEATUNNEL_HOME}/plugins/`。

### SeaTunnel Zeta 引擎

> 1. 使用 `org.postgresql.Driver` 时，请确保 [PostgreSQL JDBC 驱动](https://mvnrepository.com/artifact/org.postgresql/postgresql) 已放到 `${SEATUNNEL_HOME}/lib/`。
> 2. 使用 `com.pivotal.jdbc.GreenplumDriver` 时，请自行下载 Greenplum JDBC 驱动，并放到 `${SEATUNNEL_HOME}/lib/`。

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [变更数据捕获](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

:::tip

Greenplum Sink 不支持精确一次语义，因为 Greenplum 不支持 XA 事务。

:::

## 支持的数据源信息

| 数据源 | 驱动 | URL | Maven |
|--------|------|-----|-------|
| 使用 PostgreSQL 驱动连接 Greenplum | `org.postgresql.Driver` | `jdbc:postgresql://localhost:5432/testdb` | [下载](https://mvnrepository.com/artifact/org.postgresql/postgresql) |
| 使用 Greenplum 原生驱动连接 Greenplum | `com.pivotal.jdbc.GreenplumDriver` | `jdbc:pivotal:greenplum://localhost:5432;DatabaseName=testdb` | 从 Greenplum 获取 |

## 数据类型映射

Greenplum 沿用 PostgreSQL JDBC 驱动映射。下表列出常用类型对照：

| Greenplum 数据类型 | SeaTunnel 数据类型 |
|--------------------|--------------------|
| BOOLEAN | BOOLEAN |
| SMALLINT / INT2 | SMALLINT |
| INT / INT4 / SERIAL | INT |
| BIGINT / INT8 / BIGSERIAL | BIGINT |
| NUMERIC(p, s) / DECIMAL(p, s) / MONEY | DECIMAL(p, s) |
| REAL / FLOAT4 | FLOAT |
| DOUBLE PRECISION / FLOAT8 | DOUBLE |
| CHAR / VARCHAR / TEXT / JSON / JSONB | STRING |
| DATE | DATE |
| TIME | TIME |
| TIMESTAMP / TIMESTAMPTZ | TIMESTAMP |
| BYTEA | BYTES |

## 选项

这里只列出 Greenplum 常用配置。其他 JDBC Sink 配置，例如 `batch_size`、`max_retries`、`generate_sink_sql`、`database`、`table`、`primary_keys`、`connection_check_timeout_sec`、`max_commit_attempts` 等，继承自 [Jdbc Sink](Jdbc.md)。

| 名称 | 类型 | 是否必填 | 默认值 | 描述 |
|------|------|----------|--------|------|
| url | String | 是 | - | JDBC 连接 URL。使用 PostgreSQL 驱动时格式为 `jdbc:postgresql://host:port/database`；使用 Greenplum 原生驱动时格式为 `jdbc:pivotal:greenplum://host:port;DatabaseName=database`。 |
| driver | String | 是 | - | JDBC 驱动类名，通常为 `org.postgresql.Driver` 或 `com.pivotal.jdbc.GreenplumDriver`。 |
| username | String | 否 | - | Greenplum 用户名。 |
| password | String | 否 | - | Greenplum 密码。 |
| query | String | 否 | - | 写入上游数据的 SQL，例如 `insert into sink(age, name) values(?, ?)`。`query` 优先级高于自动生成的写入 SQL。 |
| batch_size | Int | 否 | 1000 | 写入 Greenplum 前最多缓存的记录数。 |
| max_retries | Int | 否 | 0 | `executeBatch` 失败后的重试次数。 |
| generate_sink_sql | Boolean | 否 | false | 是否根据 `database` 和 `table` 自动生成插入 SQL。 |
| database | String | 否 | - | `generate_sink_sql = true` 时使用的数据库名。 |
| table | String | 否 | - | `generate_sink_sql = true` 时使用的目标表名。 |
| primary_keys | Array | 否 | - | 自动生成 SQL 时用于 upsert 语义的主键字段列表。 |
| connection_check_timeout_sec | Int | 否 | 30 | 验证数据库连接操作的超时时间（秒）。 |
| max_commit_attempts | Int | 否 | 3 | 事务提交失败时的最大重试次数。 |
| transaction_timeout_sec | Int | 否 | -1 | 事务超时时间（秒）。`-1` 表示无限制。 |
| enable_upsert | Boolean | 否 | true | 是否启用基于主键的 upsert 写入。 |
| common-options | | 否 | - | Sink 插件通用参数，请参考 [Sink 通用选项](../common-options/sink-common-options.md)。 |

:::tip

出于许可证原因，SeaTunnel 不内置 Greenplum 原生 JDBC 驱动。如果使用 `com.pivotal.jdbc.GreenplumDriver`，请在运行任务前将 `greenplum-xxx.jar` 复制到对应引擎的依赖目录。

Greenplum 不支持 XA 事务，因此 Sink 端无法保证精确一次语义。如果你需要端到端一致，请结合外部存储（例如 Kafka、Hudi）使用幂等批写。

:::

## 任务示例

### 写入 Greenplum

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 16
    schema = {
      fields {
        age = "int"
        name = "string"
      }
    }
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

### 自动生成写入 SQL

```hocon
sink {
  Jdbc {
    driver = "org.postgresql.Driver"
    url = "jdbc:postgresql://localhost:5432/testdb"
    username = "tester"
    password = "pivotal"
    generate_sink_sql = true
    database = "testdb"
    table = "sink"
  }
}
```

### CDC 数据流写入

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  MySQL-CDC {
    username = "cdc_user"
    password = "cdc_pass"
    table-list = ["cdc_test.orders"]
    base-url = "jdbc:mysql://localhost:3306/cdc_test"
    startup.mode = "initial"
  }
}

sink {
  Jdbc {
    driver = "org.postgresql.Driver"
    url = "jdbc:postgresql://localhost:5432/testdb"
    username = "tester"
    password = "pivotal"
    generate_sink_sql = true
    database = "testdb"
    table = "orders"
    primary_keys = ["id"]
    enable_upsert = true
  }
}
```

## 变更日志

<ChangeLog />
