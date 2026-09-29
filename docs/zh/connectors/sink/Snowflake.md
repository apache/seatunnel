import ChangeLog from '../changelog/connector-jdbc.md';

# Snowflake

> JDBC Snowflake Sink 连接器

## 支持以下引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

通过 JDBC 向 Snowflake 写入数据。Sink 支持批处理和流式作业、并发写入，以及 CDC 事件。当缓冲行数达到 `batch_size`、定时刷新间隔 `batch_interval_ms` 到达或触发 checkpoint 时，批次会被刷新。

## 数据库依赖

> 请下载 "Maven" 对应的支持列表，并将其复制到 `$SEATUNNEL_HOME/plugins/jdbc/lib/` 工作目录下<br/>
> 例如 Snowflake 数据源：cp snowflake-connector-java-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)（通过主键 upsert / merge SQL）
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [x] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 支持的数据源信息

| 数据源 | 支持版本                                  | 驱动                                       | Url                                          | Maven                                                          |
|--------|-------------------------------------------|--------------------------------------------|----------------------------------------------|----------------------------------------------------------------|
| Snowflake | 不同的依赖版本有不同的驱动程序类。    | net.snowflake.client.jdbc.SnowflakeDriver | jdbc:snowflake://<account_name>.snowflakecomputing.com | [下载](https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc) |

## 数据类型映射

|                             Snowflake 数据类型                            | SeaTunnel 数据类型 |
|-----------------------------------------------------------------------------|--------------------|
| BOOLEAN                                                                     | BOOLEAN            |
| TINYINT<br/>SMALLINT<br/>BYTEINT                                            | SHORT              |
| INT<br/>INTEGER                                                             | INT                |
| BIGINT                                                                      | LONG               |
| DECIMAL<br/>NUMERIC<br/>NUMBER<br/>                                         | DECIMAL(p, s)      |
| DECIMAL(p, s)（p > 38 时）                                                  | DECIMAL(38, 18)    |
| REAL<br/>FLOAT4                                                             | FLOAT              |
| DOUBLE<br/>DOUBLE PRECISION<br/>FLOAT8<br/>FLOAT                            | DOUBLE             |
| CHAR<br/>CHARACTER<br/>VARCHAR<br/>STRING<br/>TEXT<br/>VARIANT<br/>OBJECT   | STRING             |
| DATE                                                                        | DATE               |
| TIME                                                                        | TIME               |
| DATETIME<br/>TIMESTAMP<br/>TIMESTAMP_LTZ<br/>TIMESTAMP_NTZ<br/>TIMESTAMP_TZ | TIMESTAMP          |
| BINARY<br/>VARBINARY<br/>GEOGRAPHY<br/>GEOMETRY                             | BYTES              |

## 选项

| 名称                          |  类型   | 是否必填 | 默认值 | 描述                                                                                                                                                                                |
|-------------------------------|---------|----------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                           | String  | 是       | -       | JDBC 连接 URL，例如 `jdbc:snowflake://<account_name>.snowflakecomputing.com`。                                                                                                       |
| driver                        | String  | 是       | -       | JDBC 驱动类名，Snowflake 使用 `net.snowflake.client.jdbc.SnowflakeDriver`。                                                                                                          |
| username                      | String  | 否       | -       | Snowflake 账户用户名。                                                                                                                                                              |
| password                      | String  | 否       | -       | Snowflake 账户密码。                                                                                                                                                                |
| query                         | String  | 否       | -       | 写入上游数据的 SQL。优先级高于 `database`/`table` 自动生成的 SQL；设置后会关闭基于目录的优化（无法生成 `MERGE` upsert）。                                                          |
| database                      | String  | 否       | -       | 数据库名。`generate_sink_sql = true` 时与 `table` 一起用于生成 `INSERT`/`MERGE` SQL；与 `query` 互斥，同时设置时 `query` 优先生。                          |
| table                         | String  | 否       | -       | 目标表名。与 `database` 一起配合 `generate_sink_sql` 生成写入语句。                                                                                                                  |
| primary_keys                  | Array   | 否       | -       | 主键列。`generate_sink_sql = true` 且 `enable_upsert = true` 时用于构建 `MERGE` upsert 语句。                                                                                       |
| connection_check_timeout_sec  | Int     | 否       | 30      | 连接校验超时时间（秒）。                                                                                                                                                            |
| max_retries                   | Int     | 否       | 0       | `executeBatch` 失败的重试次数。                                                                                                                                                    |
| batch_size                    | Int     | 否       | 1000    | 触发 flush 的缓冲行数；同时在 `checkpoint.interval` 时也会 flush。                                                                                                                  |
| batch_interval_ms             | Long    | 否       | 0       | 两次 flush 之间的最大时间间隔（毫秒）。`0` 关闭按时间间隔的 flush。                                                                                                                  |
| max_commit_attempts           | Int     | 否       | 3       | 事务提交失败的重试次数。                                                                                                                                                            |
| transaction_timeout_sec       | Int     | 否       | -1      | 事务超时时间（秒），`-1` 表示永不超时。                                                                                                                                              |
| auto_commit                   | Boolean | 否       | true    | 是否自动提交每个批次。                                                                                                                                                              |
| properties                    | Map     | 否       | -       | 额外的 JDBC 连接参数。`properties` 与 `url` 包含相同键时优先级由驱动决定。                                                                                                          |
| common-options                |         | 否       | -       | Sink 插件通用参数，详见 [Sink 通用选项](../common-options/sink-common-options.md)。                                                                                                |
| enable_upsert                 | Boolean | 否       | true    | 在 `primary_keys` 已配置且 `generate_sink_sql = true` 时，生成 `MERGE` upsert 语句；若输入无重复主键，可设为 `false` 使用更快的纯插入路径。                                          |

## 说明

- 想完全控制 `INSERT` 语句和参数顺序时使用 `query`。
- 让 SeaTunnel 自动生成 insert、update、delete SQL 时，使用 `database`、`table` 和 `primary_keys`（配合 `generate_sink_sql = true`）。
- Snowflake sink 使用普通 JDBC 批量写入，不提供精确一次保证。
- 不要把 Snowflake 凭据写在共享示例、日志或截图中。

## 任务示例

### 简单示例

此示例从 `FakeSource` 读取 16 行数据并插入到 Snowflake 的 `test_table` 表中。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    parallelism = 1
    plugin_output = "fake"
    row.num = 16
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  Jdbc {
    url = "jdbc:snowflake://<account_name>.snowflakecomputing.com"
    driver = "net.snowflake.client.jdbc.SnowflakeDriver"
    username = "USER"
    password = "PASSWORD"
    query = "insert into test_table(name, age) values(?, ?)"
  }
}
```

运行作业前，请先在 Snowflake 中创建目标数据库和表。

### CDC 事件

配置 `database`、`table` 和 `primary_keys`，SeaTunnel 就能为 CDC 事件生成对应的 `INSERT`/`UPDATE`/`DELETE` SQL。

```hocon
sink {
  Jdbc {
    url = "jdbc:snowflake://<account_name>.snowflakecomputing.com"
    driver = "net.snowflake.client.jdbc.SnowflakeDriver"
    username = "USER"
    password = "PASSWORD"
    generate_sink_sql = true
    database = "test"
    table = "sink_table"
    primary_keys = ["id", "name"]
  }
}
```

## 变更日志

<ChangeLog />
