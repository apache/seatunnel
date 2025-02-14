# PostgreSQL CDC

> PostgreSQL CDC 源连接器

## 支持的引擎

> SeaTunnel Zeta<br/>
> Flink <br/>

## 主要特性

- [ ] [批处理](../../concept/connector-v2-features.md)
- [x] [流处理](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)
- [ ] [列投影](../../concept/connector-v2-features.md)
- [x] [并行性](../../concept/connector-v2-features.md)
- [x] [支持用户定义的拆分](../../concept/connector-v2-features.md)

## 描述

Postgre CDC 连接器允许从 Postgre 数据库读取快照数据和增量数据。本文件描述了如何设置 Postgre CDC 连接器，以便对 Postgre 数据库执行 SQL 查询。

## 支持的数据源信息

| 数据源      |                     支持的版本                      |        驱动        |                  Url                  |                                  Maven                                   |
|------------|-----------------------------------------------------|---------------------|---------------------------------------|--------------------------------------------------------------------------|
| PostgreSQL | 不同的依赖版本有不同的驱动类。                       | org.postgresql.Driver | jdbc:postgresql://localhost:5432/test | [下载](https://mvnrepository.com/artifact/org.postgresql/postgresql) |
| PostgreSQL | 如果您想在 PostgreSQL 中操作 GEOMETRY 类型。        | org.postgresql.Driver | jdbc:postgresql://localhost:5432/test | [下载](https://mvnrepository.com/artifact/net.postgis/postgis-jdbc)  |

## 使用依赖

### 安装 Jdbc 驱动

#### 对于 Spark/Flink 引擎

> 1. 您需要确保 [jdbc 驱动 jar 包](https://mvnrepository.com/artifact/org.postgresql/postgresql) 已放置在目录 `${SEATUNNEL_HOME}/plugins/` 中。

#### 对于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [jdbc 驱动 jar 包](https://mvnrepository.com/artifact/org.postgresql/postgresql) 已放置在目录 `${SEATUNNEL_HOME}/lib/` 中。

请下载并将 PostgreSQL 驱动放入 `${SEATUNNEL_HOME}/lib/` 目录。例如：cp postgresql-xxx.jar `$SEATUNNEL_HOME/lib/`

> 以下是启用 PostgreSQL 中的 CDC（变化数据捕获）的步骤：

1. 确保 wal_level 设置为 logical：通过在 postgresql.conf 配置文件中添加 "wal_level = logical" 来修改，重启 PostgreSQL 服务器以使更改生效。
   或者，您可以使用 SQL 命令直接修改配置：

```sql
ALTER SYSTEM SET wal_level TO 'logical';
SELECT pg_reload_conf();
```

2. 将指定表的 REPLICA 策略更改为 FULL

```sql
ALTER TABLE your_table_name REPLICA IDENTITY FULL;
```

## 数据类型映射

|                                  PostgreSQL 数据类型                                   |                                                              SeaTunnel 数据类型                                                               |
|-----------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| BOOL<br/>                                                                               | BOOLEAN                                                                                                                                        |
| _BOOL<br/>                                                                              | ARRAY&LT;BOOLEAN&GT;                                                                                                                           |
| BYTEA<br/>                                                                              | BYTES                                                                                                                                          |
| _BYTEA<br/>                                                                             | ARRAY&LT;TINYINT&GT;                                                                                                                           |
| INT2<br/>SMALLSERIAL<br/>INT4<br/>SERIAL<br/>                                           | INT                                                                                                                                            |
| _INT2<br/>_INT4<br/>                                                                    | ARRAY&LT;INT&GT;                                                                                                                               |
| INT8<br/>BIGSERIAL<br/>                                                                 | BIGINT                                                                                                                                         |
| _INT8<br/>                                                                              | ARRAY&LT;BIGINT&GT;                                                                                                                            |
| FLOAT4<br/>                                                                             | FLOAT                                                                                                                                          |
| _FLOAT4<br/>                                                                            | ARRAY&LT;FLOAT&GT;                                                                                                                             |
| FLOAT8<br/>                                                                             | DOUBLE                                                                                                                                         |
| _FLOAT8<br/>                                                                            | ARRAY&LT;DOUBLE&GT;                                                                                                                            |
| NUMERIC(指定列的列大小>0)                                                               | DECIMAL(指定列的列大小, 获取指定列小数点右侧的位数)                                                                                             |
| NUMERIC(指定列的列大小<0)                                                               | DECIMAL(38, 18)                                                                                                                                |
| BPCHAR<br/>CHARACTER<br/>VARCHAR<br/>TEXT<br/>GEOMETRY<br/>GEOGRAPHY<br/>JSON<br/>JSONB | STRING                                                                                                                                         |
| _BPCHAR<br/>_CHARACTER<br/>_VARCHAR<br/>_TEXT                                           | ARRAY&LT;STRING&GT;                                                                                                                            |
| TIMESTAMP<br/>                                                                          | TIMESTAMP                                                                                                                                      |
| TIME<br/>                                                                               | TIME                                                                                                                                           |
| DATE<br/>                                                                               | DATE                                                                                                                                           |
| 其他数据类型                                                                            | 尚不支持                                                                                                                                       |

## 源选项

|                      名称                      | 类型       | 必需 | 默认  | 描述                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|------------------------------------------------|----------|------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| base-url                                       | String   | 是   | -        | JDBC 连接的 URL。参考案例：`jdbc:postgresql://localhost:5432/postgres_cdc?loggerLevel=OFF`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| username                                       | String   | 是   | -        | 连接到数据库服务器时使用的数据库名称。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| password                                       | String   | 是   | -        | 连接到数据库服务器时使用的密码。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| database-names                                 | List     | 否   | -        | 需要监控的数据库名称。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| table-names                                    | List     | 是   | -        | 需要监控的数据库表名称。表名称需要包含数据库名称，例如：`database_name.table_name`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| table-names-config                             | List     | 否   | -        | 表配置列表。例如： [{"table": "db1.schema1.table1","primaryKeys": ["key1"],"snapshotSplitColumn": "key2"}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| startup.mode                                   | List     | 否   | INITIAL  | PostgreSQL CDC 消费者的可选启动模式，有效枚举为 `initial`、`earliest` 和 `latest`。<br/> `initial`: 启动时同步历史数据，然后同步增量数据。<br/> `earliest`: 从可能的最早偏移量启动。<br/> `latest`: 从最新偏移量启动。                                                                                                                                                                                                                                                                                             |
| snapshot.split.size                            | Integer  | 否   | 8096     | 表快照的拆分大小（行数），捕获的表在读取表快照时被拆分成多个拆分。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| snapshot.fetch.size                            | Integer  | 否   | 1024     | 读取表快照时每次轮询的最大获取大小。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| slot.name                                      | String   | 否   | -        | 为特定数据库/模式创建的用于流式传输更改的 PostgreSQL 逻辑解码槽的名称。服务器使用此槽将事件流式传输到您正在配置的连接器。默认值为 seatunnel。                                                                                                                                                                                                                                                                                                                                                      |
| decoding.plugin.name                           | String   | 否   | pgoutput | 安装在服务器上的 Postgres 逻辑解码插件的名称，支持的值有 decoderbufs、wal2json、wal2json_rds、wal2json_streaming、wal2json_rds_streaming 和 pgoutput。                                                                                                                                                                                                                                                                                                                                                                                                                          |
| server-time-zone                               | String   | 否   | UTC      | 数据库服务器中的会话时区。如果未设置，则使用 ZoneId.systemDefault() 来确定服务器时区。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| connect.timeout.ms                             | Duration | 否   | 30000    | 连接器在尝试连接到数据库服务器后应等待的最大时间，以防超时。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| connect.max-retries                            | Integer  | 否   | 3        | 连接器应重试建立数据库服务器连接的最大重试次数。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| connection.pool.size                           | Integer  | 否   | 20       | JDBC 连接池大小。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| chunk-key.even-distribution.factor.upper-bound | Double   | 否   | 100      | 块键分布因子的上限。此因子用于确定表数据是否均匀分布。如果计算出的分布因子小于或等于此上限（即 (MAX(id) - MIN(id) + 1) / 行数），则将优化表块以实现均匀分布。否则，如果分布因子更大，则将认为该表分布不均匀，并且如果估计的分片数量超过 `sample-sharding.threshold` 指定的值，则将使用基于采样的分片策略。默认值为 100.0。 |
| chunk-key.even-distribution.factor.lower-bound | Double   | 否   | 0.05     | 块键分布因子的下限。此因子用于确定表数据是否均匀分布。如果计算出的分布因子大于或等于此下限（即 (MAX(id) - MIN(id) + 1) / 行数），则将优化表块以实现均匀分布。否则，如果分布因子更小，则将认为该表分布不均匀，并且如果估计的分片数量超过 `sample-sharding.threshold` 指定的值，则将使用基于采样的分片策略。默认值为 0.05。  |
| sample-sharding.threshold                      | Integer  | 否   | 1000     | 此配置指定触发采样分片策略的估计分片数量阈值。当分布因子超出由 `chunk-key.even-distribution.factor.upper-bound` 和 `chunk-key.even-distribution.factor.lower-bound` 指定的范围，且估计的分片数量（计算为近似行数 / 块大小）超过此阈值时，将使用采样分片策略。这可以帮助更有效地处理大数据集。默认值为 1000 个分片。                                                                                   |
| inverse-sampling.rate                          | Integer  | 否   | 1000     | 在采样分片策略中使用的采样率的倒数。例如，如果此值设置为 1000，则意味着在采样过程中应用 1/1000 的采样率。此选项提供了控制采样粒度的灵活性，从而影响最终的分片数量。在处理非常大数据集时，较低的采样率尤为有用。默认值为 1000。                                                                                                                                                              |
| exactly_once                                   | Boolean  | 否   | false    | 启用精确一次语义。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| format                                         | Enum     | 否   | DEFAULT  | PostgreSQL CDC 的可选输出格式，有效枚举为 `DEFAULT`、`COMPATIBLE_DEBEZIUM_JSON`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| debezium                                       | Config   | 否   | -        | 将 [Debezium 的属性](https://github.com/debezium/debezium/blob/v1.9.8.Final/documentation/modules/ROOT/pages/connectors/postgresql.adoc#connector-configuration-properties) 传递给用于捕获 PostgreSQL 服务器数据更改的 Debezium 嵌入式引擎。                                                                                                                                                                                                                                                                                                                                |
| common-options                                 |          | 否   | -        | 源插件的公共参数，请参阅 [源公共选项](../source-common-options.md) 获取详细信息。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |

## 任务示例

### 简单

> 支持多表读取

```


env {
  # You can set engine configuration here
  execution.parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
  read_limit.bytes_per_second=7000000
  read_limit.rows_per_second=400
}

source {
  Postgres-CDC {
    plugin_output = "customers_Postgre_cdc"
    username = "postgres"
    password = "postgres"
    database-names = ["postgres_cdc"]
    schema-names = ["inventory"]
    table-names = ["postgres_cdc.inventory.postgres_cdc_table_1,postgres_cdc.inventory.postgres_cdc_table_2"]
    base-url = "jdbc:postgresql://postgres_cdc_e2e:5432/postgres_cdc?loggerLevel=OFF"
  }
}

transform {

}

sink {
  jdbc {
    plugin_input = "customers_Postgre_cdc"
    url = "jdbc:postgresql://postgres_cdc_e2e:5432/postgres_cdc?loggerLevel=OFF"
    driver = "org.postgresql.Driver"
    user = "postgres"
    password = "postgres"

    generate_sink_sql = true
    # You need to configure both database and table
    database = postgres_cdc
    schema = "inventory"
    tablePrefix = "sink_"
    primary_keys = ["id"]
  }
}
```

### 支持自定义表的主键

```
source {
  Postgres-CDC {
    plugin_output = "customers_mysql_cdc"
    username = "postgres"
    password = "postgres"
    database-names = ["postgres_cdc"]
    schema-names = ["inventory"]
    table-names = ["postgres_cdc.inventory.full_types_no_primary_key"]
    base-url = "jdbc:postgresql://postgres_cdc_e2e:5432/postgres_cdc?loggerLevel=OFF"
    decoding.plugin.name = "decoderbufs"
    exactly_once = false
    table-names-config = [
      {
        table = "postgres_cdc.inventory.full_types_no_primary_key"
        primaryKeys = ["id"]
      }
    ]
  }
}
```

## 更新日志

- 添加 PostgreSQL CDC 源连接器

### 下一个版本
