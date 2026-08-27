import ChangeLog from '../changelog/connector-jdbc.md';

# DB2

> JDBC DB2 Sink 连接器

## 支持以下引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

通过 JDBC 向 DB2 写入数据。支持批处理和流式作业、并发写入，以及基于 XA 事务的精确一次语义。可通过将 `is_exactly_once` 设置为 `true` 并配上对应的 `xa_data_source_class_name` 启用精确一次。

## 使用依赖关系

### 适用于 Spark/Flink 引擎

> 1. 您需要确保 [JDBC 驱动 JAR 包](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc) 已放置在目录 `${SEATUNNEL_HOME}/plugins/`。

### 适用于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [JDBC 驱动 JAR 包](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc) 已放置在目录 `${SEATUNNEL_HOME}/lib/`。

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)（XA 事务）
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)（通过主键 upsert / merge SQL）
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [x] [定时刷新](../../introduction/concepts/connector-v2-features.md)

> 使用 XA 事务保证精确一次。需要同时启用 `is_exactly_once = true` 并配置数据库对应的 `xa_data_source_class_name`。

## 支持的数据源信息

| 数据库 | 支持版本                                  | 驱动                       | Url                            | Maven                                                          |
|--------|-------------------------------------------|----------------------------|--------------------------------|----------------------------------------------------------------|
| DB2    | 不同的依赖版本有不同的驱动程序类。       | com.ibm.db2.jcc.DB2Driver  | jdbc:db2://127.0.0.1:50000/dbname | [下载](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc) |

## 数据类型映射

|                                            DB2 数据类型                                            | SeaTunnel 数据类型 |
|------------------------------------------------------------------------------------------------------|--------------------|
| BOOLEAN                                                                                              | BOOLEAN            |
| SMALLINT                                                                                             | SHORT              |
| INT<br/>INTEGER                                                                                      | INTEGER            |
| BIGINT                                                                                               | LONG               |
| DECIMAL<br/>DEC<br/>NUMERIC<br/>NUM                                                                  | DECIMAL(38,18)     |
| REAL                                                                                                 | FLOAT              |
| FLOAT<br/>DOUBLE<br/>DOUBLE PRECISION<br/>DECFLOAT                                                   | DOUBLE             |
| CHAR<br/>VARCHAR<br/>LONG VARCHAR<br/>CLOB<br/>GRAPHIC<br/>VARGRAPHIC<br/>LONG VARGRAPHIC<br/>DBCLOB | STRING             |
| BLOB                                                                                                 | BYTES              |
| DATE                                                                                                 | DATE               |
| TIME                                                                                                 | TIME               |
| TIMESTAMP                                                                                            | TIMESTAMP          |
| ROWID<br/>XML                                                                                        | 暂不支持           |

## 选项

| 名称                          |  类型   | 是否必填 | 默认值 | 描述                                                                                                                                                                              |
|-------------------------------|---------|----------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                           | String  | 是       | -       | JDBC 连接 URL，例如 `jdbc:db2://127.0.0.1:50000/dbname`。                                                                                                                          |
| driver                        | String  | 是       | -       | JDBC 驱动类名，DB2 使用 `com.ibm.db2.jcc.DB2Driver`。                                                                                                                              |
| username                      | String  | 否       | -       | DB2 用户名。                                                                                                                                                                       |
| password                      | String  | 否       | -       | DB2 密码。                                                                                                                                                                         |
| query                         | String  | 否       | -       | 写入上游数据的 SQL。优先级高于 `database`/`table` 自动生成的 SQL；设置后会关闭基于目录的优化（无法生成 `MERGE` upsert）。                                                          |
| database                      | String  | 否       | -       | 数据库名。`generate_sink_sql = true` 时与 `table` 一起用于生成 `INSERT`/`MERGE` SQL；与 `query` 互斥，同时设置时 `query` 优先生。                          |
| table                         | String  | 否       | -       | 目标表名。与 `database` 一起配合 `generate_sink_sql` 生成写入语句。                                                                                                                  |
| primary_keys                  | Array   | 否       | -       | 主键列。`generate_sink_sql = true` 且 `enable_upsert = true` 时用于构建 `MERGE` upsert 语句。                                                                                       |
| connection_check_timeout_sec  | Int     | 否       | 30      | 连接校验超时时间（秒）。                                                                                                                                                            |
| max_retries                   | Int     | 否       | 0       | `executeBatch` 失败的重试次数。                                                                                                                                                    |
| batch_size                    | Int     | 否       | 1000    | 触发 flush 的缓冲行数；同时在 `checkpoint.interval` 时也会 flush。                                                                                                                  |
| batch_interval_ms             | Long    | 否       | 0       | 两次 flush 之间的最大时间间隔（毫秒）。`0` 关闭按时间间隔的 flush。                                                                                                                  |
| is_exactly_once               | Boolean | 否       | false   | 是否启用基于 XA 的精确一次；启用时必须设置 `xa_data_source_class_name`。                                                                                                            |
| generate_sink_sql             | Boolean | 否       | false   | 基于 `database`/`table`/`primary_keys` 自动生成 `INSERT` 或 `MERGE` SQL，而不是手动提供 `query`。                                                                                  |
| xa_data_source_class_name     | String  | 否       | -       | XA 数据源类名，DB2 使用 `com.ibm.db2.jcc.DB2XADataSource`。                                                                                                                          |
| max_commit_attempts           | Int     | 否       | 3       | 事务提交失败的重试次数。                                                                                                                                                            |
| transaction_timeout_sec       | Int     | 否       | -1      | 事务超时时间（秒），`-1` 表示永不超时；设置超时可能会影响精确一次。                                                                                                                  |
| auto_commit                   | Boolean | 否       | true    | 是否自动提交每个批次。                                                                                                                                                              |
| properties                    | Map     | 否       | -       | 额外的 JDBC 连接参数。`properties` 与 `url` 包含相同键时优先级由驱动决定。                                                                                                          |
| common-options                |         | 否       | -       | Sink 插件通用参数，详见 [Sink 通用选项](../common-options/sink-common-options.md)。                                                                                                |
| enable_upsert                 | Boolean | 否       | true    | 在 `primary_keys` 已配置且 `generate_sink_sql = true` 时，生成 `MERGE` upsert 语句；若输入无重复主键，可设为 `false` 使用更快的纯插入路径。                                          |

## 任务示例

### 简单示例

此示例从 `FakeSource` 读取 16 行数据并插入到 DB2 的 `test_table` 表中。

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
    url = "jdbc:db2://127.0.0.1:50000/dbname"
    driver = "com.ibm.db2.jcc.DB2Driver"
    username = "db2inst1"
    password = "123456"
    query = "insert into test_table(name, age) values(?, ?)"
  }
}
```

运行作业前，请先在 DB2 中创建目标数据库和表。

### 自动生成 Sink SQL

不写 `INSERT` 语句，让 SeaTunnel 根据 `database` 和 `table` 自动生成。

```hocon
sink {
  Jdbc {
    url = "jdbc:db2://127.0.0.1:50000/dbname"
    driver = "com.ibm.db2.jcc.DB2Driver"
    username = "db2inst1"
    password = "123456"
    generate_sink_sql = true
    database = test
    table = test_table
  }
}
```

### 精确一次

启用基于 XA 的精确一次：只有两阶段都成功时事务才会提交，`max_retries`/`max_commit_attempts` 提供额外容错。

```hocon
sink {
  Jdbc {
    url = "jdbc:db2://127.0.0.1:50000/dbname"
    driver = "com.ibm.db2.jcc.DB2Driver"
    username = "db2inst1"
    password = "123456"
    query = "insert into test_table(name, age) values(?, ?)"
    max_retries = 0
    is_exactly_once = true
    xa_data_source_class_name = "com.ibm.db2.jcc.DB2XADataSource"
  }
}
```

### 使用自动生成 SQL 的 Upsert

当 `generate_sink_sql = true` 且设置了 `primary_keys`，DB2 会通过生成的 `MERGE` 语句进行 upsert 写入。如果上游数据只包含新增数据，可设置 `enable_upsert = false` 使用更快的纯插入路径。

```hocon
sink {
  Jdbc {
    url = "jdbc:db2://127.0.0.1:50000/E2E"
    driver = "com.ibm.db2.jcc.DB2Driver"
    username = "db2inst1"
    password = "123456"
    database = "E2E"
    table = "SINK"
    generate_sink_sql = true
    enable_upsert = true
    primary_keys = ["C_INT"]
  }
}
```

## 变更日志

<ChangeLog />
