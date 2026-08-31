import ChangeLog from '../changelog/connector-cdc-opengauss.md';

# Opengauss CDC

> Opengauss CDC源连接器

## 支持这些引擎

> SeaTunnel Zeta<br/>
> Flink <br/>

## 主要功能

- [ ] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户定义的拆分](../../introduction/concepts/connector-v2-features.md)

## 描述

Opengauss CDC 连接器允许从 Opengauss 数据库读取快照数据和增量数据。本文档介绍如何配置 Opengauss CDC 连接器。

## 使用步骤

> 这里是启用Opengauss CDC的步骤:

1. 确保wal_level被设置为logical, 你可以直接使用SQL命令来修改这个配置:

```sql
ALTER SYSTEM SET wal_level TO 'logical';
SELECT pg_reload_conf();
```

2. 改变指定表的REPLICA策略为FULL

```sql
ALTER TABLE your_table_name REPLICA IDENTITY FULL;
```

如果你有很多表，你可以使用下面SQL的结果集来改变所有表的REPLICA策略

```sql
select 'ALTER TABLE ' || schemaname || '.' || tablename || ' REPLICA IDENTITY FULL;' from pg_tables where schemaname = 'YourTableSchema'
```

## 数据类型映射

|                                   Opengauss Data type                                   |                                                              SeaTunnel Data type                                                               |
|-----------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| BOOL<br/>                                                                               | BOOLEAN                                                                                                                                        |
| BYTEA<br/>                                                                              | BYTES                                                                                                                                          |
| INT2<br/>SMALLSERIAL<br/>INT4<br/>SERIAL<br/>                                           | INT                                                                                                                                            |
| INT8<br/>BIGSERIAL<br/>                                                                 | BIGINT                                                                                                                                         |
| FLOAT4<br/>                                                                             | FLOAT                                                                                                                                          |
| FLOAT8<br/>                                                                             | DOUBLE                                                                                                                                         |
| NUMERIC(Get the designated column's specified column size>0)                            | DECIMAL(Get the designated column's specified column size,Gets the number of digits in the specified column to the right of the decimal point) |
| NUMERIC(Get the designated column's specified column size<0)                            | DECIMAL(38, 18)                                                                                                                                |
| BPCHAR<br/>CHARACTER<br/>VARCHAR<br/>TEXT<br/>GEOMETRY<br/>GEOGRAPHY<br/>JSON<br/>JSONB | STRING                                                                                                                                         |
| TIMESTAMP<br/>                                                                          | TIMESTAMP                                                                                                                                      |
| TIME<br/>                                                                               | TIME                                                                                                                                           |
| DATE<br/>                                                                               | DATE                                                                                                                                           |
| OTHER DATA TYPES                                                                        | NOT SUPPORTED YET                                                                                                                              |

## 源端可选项

|                      Name                 | Type | Required | Default  | Description                                                                                                                                                                                                        |
|-------------------------------------------|------|----------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | 字符串  | 是        | -        | JDBC连接的URL. 参考: `jdbc:postgresql://localhost:5432/postgres_cdc?loggerLevel=OFF`.                                                                                                                                   |
| username                                  | 字符串  | 是        | -        | 连接数据库的用户名                                                                                                                                                                                                          |
| password                                  | 字符串  | 是        | -        | 连接数据库的密码                                                                                                                                                                                                           |
| database-names                            | 列表   | 否        | -        | 需要监控的数据库名称。                                                                                                                                                                                                           |
| table-names                               | 列表   | 二选一 | -        | 需要监控的表。请使用完整的 `database.schema.table` 格式，例如：`opengauss_cdc.inventory.orders`。                                                                                                                                                              |
| table-pattern                             | 字符串 | 二选一 | -        | 需要监控的表名正则表达式。正则需要匹配完整表名，例如：`opengauss_cdc\\.inventory\\..*`。`table-names` 和 `table-pattern` 互斥。                                                                                                                                                              |
| table-names-config                        | 列表   | 否        | -        | 表级配置列表。例如：`[{"table": "db1.schema1.table1","primaryKeys": ["key1"],"snapshotSplitColumn": "key2"}]`。无物理主键表可通过 `primaryKeys` 指定唯一键。`snapshotSplitColumn` 必须是唯一键，否则 SeaTunnel 会忽略该配置并自动选择拆分列。                                                                                                                                             |
| startup.mode                              | 枚举   | 否        | INITIAL  | Opengauss CDC消费者的可选启动模式，有效枚举为 `initial`、`snapshot-only`、`committed-offset`、`earliest` 和 `latest`。<br/> `initial`: 启动时同步历史数据，然后同步增量数据。<br/> `snapshot-only`: 仅同步启动时的历史数据，然后以有界任务结束，不进入 WAL 流读取。<br/> `committed-offset`: 跳过快照数据，从配置的复制槽已提交 LSN 开始读取 WAL。该模式要求显式配置 `slot.name`，如果复制槽不存在或没有可用的已提交 LSN，则启动失败。<br/> `earliest`: 从可能的最早偏移量启动。<br/> `latest`: 从最新偏移量启动。                                                        |
| stop.mode                                 | 枚举   | 否        | NEVER    | Opengauss CDC 消费者的可选停止模式。唯一有效的枚举值是 `never`：一旦进入增量阶段，数据源会持续读取 WAL 变更，不会自行停止。                                                                                                                                                                                                                                                                                                                             |
| snapshot.split.size                       | 整型   | 否        | 8096     | 表快照的分割大小（行数），在读取表的快照时，捕获的表被分割成多个split                                                                                                                                                                              |
| snapshot.fetch.size                       | 整型   | 否        | 1024     | 读取表快照时，每次轮询的最大读取大小                                                                                                                                                                                                 |
| slot.name                                 | 字符串  | 否        | seatunnel | Opengauss 逻辑解码槽名称。同一个 Opengauss 实例上如果有多个 CDC 任务，请为每个任务配置不同的 `slot.name`。                                                                                                                               |
| decoding.plugin.name                      | 字符串  | 否        | pgoutput | 安装在服务器上的Postgres逻辑解码插件的名称，支持的值是decoderbufs、wal2json、wal2json_rds、wal2json_streaming、wal2json_rds_streaming和pgoutput                                                                                                |
| require-replica-identity-full             | 布尔   | 否        | true     | 要求表设置 REPLICA IDENTITY FULL。设置为 false 时允许其他 replica identity 的表，但 UPDATE/DELETE 事件可能不包含变更前的数据，仅建议用于只追加写入的表（例如 outbox 模式）。默认值 true 以保持向后兼容。                                                              |
| server-time-zone                          | 字符串  | 否        | UTC      | 数据库服务器中的会话时区。如果没有设置，则使用ZoneId.systemDefault()来确定服务器的时区                                                                                                                                                             |
| connect.timeout.ms                        | 时间间隔 | 否        | 30000    | 在尝试连接数据库服务器之后，连接器在超时之前应该等待的最大时间                                                                                                                                                                                    |
| connect.max-retries                       | 整型   | 否        | 3        | 连接器在建立数据库服务器连接时应该重试的最大次数                                                                                                                                                                                           |
| connection.pool.size                      | 整型   | 否        | 20       | jdbc连接池的大小                                                                                                                                                                                                         |
| chunk-key.even-distribution.factor.upper-bound | 双浮点型 | 否        | 100      | chunk的key分布因子的上界。该因子用于确定表数据是否均匀分布。如果分布因子被计算为小于或等于这个上界(即(MAX(id) - MIN(id) + 1) /行数)，表的所有chunk将被优化以达到均匀分布。否则，如果分布因子更大，则认为表分布不均匀，如果估计的分片数量超过`sample-sharding.threshold`指定的值，则将使用基于采样的分片策略。默认值为100.0。                 |
| chunk-key.even-distribution.factor.lower-bound | 双浮点型 | 否        | 0.05     | chunk的key分布因子的下界。该因子用于确定表数据是否均匀分布。如果分布因子的计算结果大于或等于这个下界(即(MAX(id) - MIN(id) + 1) /行数)，那么表的所有块将被优化以达到均匀分布。否则，如果分布因子较小，则认为表分布不均匀，如果估计的分片数量超过`sample-sharding.threshold`指定的值，则使用基于采样的分片策略。缺省值为0.05。                    |
| sample-sharding.threshold                 | 整型   | 否        | 1000     | 此配置指定了用于触发采样分片策略的估计分片数的阈值。当分布因子超出了由`chunk-key.even-distribution.factor.upper-bound `和`chunk-key.even-distribution.factor.lower-bound`，并且估计的分片计数(以近似的行数/块大小计算)超过此阈值，则将使用样本分片策略。这有助于更有效地处理大型数据集。默认值为1000个分片。         |
| inverse-sampling.rate                     | 整型   | 否        | 1000     | 采样分片策略中使用的采样率的倒数。例如，如果该值设置为1000，则意味着在采样过程中应用了1/1000的采样率。该选项提供了控制采样粒度的灵活性，从而影响最终的分片数量。当处理非常大的数据集时，它特别有用，其中首选较低的采样率。缺省值为1000。                                                                                        |
| split.allow-sampling                      | 布尔   | 否        | true     | 是否允许使用基于采样的分片策略。当设置为 false 时，无论预估分片数是否超过阈值，系统都会回退到非均匀分片方式（迭代查询方式）。默认值为 true。                                                                 |
| exactly_once                              | 布尔   | 否        | false    | 在初始快照阶段启用精确一次语义。仅当 `startup.mode` 为 `initial` 时可用。                                                                                                                                                                                                   |
| format                                    | 枚举   | 否        | DEFAULT  | Opengauss CDC可选的输出格式, 有效的枚举是`DEFAULT`, `COMPATIBLE_DEBEZIUM_JSON`.                                                                                                                                                 |
| debezium                                  | 配置   | 否        | -        | 将 [Debezium的属性](https://github.com/debezium/debezium/blob/v1.9.8.Final/documentation/modules/ROOT/pages/connectors/postgresql.adoc#connector-configuration-properties) 传递到Debezium嵌入式引擎，该引擎用于捕获来自Opengauss服务的数据更改  |
| common-options                            |      | 否        | -        | 源码插件通用参数, 请参考[Source Common Options](../common-options/source-common-options.md)获取详情                                                                                                                                              |

## 任务示例

### 简单

> 支持多表读

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
  Opengauss-CDC {
    plugin_output = "customers_opengauss_cdc"
    username = "gaussdb"
    password = "openGauss@123"
    database-names = ["opengauss_cdc"]
    table-names = ["opengauss_cdc.inventory.opengauss_cdc_table_1","opengauss_cdc.inventory.opengauss_cdc_table_2"]
    url = "jdbc:postgresql://opengauss_cdc_e2e:5432/opengauss_cdc"
    decoding.plugin.name = "pgoutput"
    slot.name = "seatunnel_opengauss_cdc"
  }
}

transform {

}

sink {
  jdbc {
    plugin_input = "customers_opengauss_cdc"
    url = "jdbc:postgresql://opengauss_cdc_e2e:5432/opengauss_cdc"
    driver = "org.postgresql.Driver"
    user = "dailai"
    password = "openGauss@123"

    compatible_mode="postgresLow"
    generate_sink_sql = true
    # You need to configure both database and table
    database = "opengauss_cdc"
    schema = "inventory"
    tablePrefix = "sink_"
    primary_keys = ["id"]
  }
}

```

### 支持自定义主键

```
source {
  Opengauss-CDC {
    plugin_output = "customers_opengauss_cdc"
    username = "gaussdb"
    password = "openGauss@123"
    database-names = ["opengauss_cdc"]
    table-names = ["opengauss_cdc.inventory.full_types_no_primary_key"]
    url = "jdbc:postgresql://opengauss_cdc_e2e:5432/opengauss_cdc?loggerLevel=OFF"
    decoding.plugin.name = "pgoutput"
    exactly_once = true
    slot.name = "seatunnel_opengauss_cdc"
    table-names-config = [
      {
        table = "opengauss_cdc.inventory.full_types_no_primary_key"
        primaryKeys = ["id"]
      }
    ]
  }
}
```

## CDC 元数据字段

Opengauss CDC 会提供以下元数据字段，可配合 `Metadata` 转换使用：

| 字段 | 类型 | 说明 |
|------|------|------|
| database | STRING | 源数据库名称。 |
| table | STRING | 源表名称。 |
| rowKind | STRING | 变更类型，例如 insert、update 或 delete。 |
| ts_ms | LONG | 源事件时间，单位为毫秒。 |
| delay | LONG | 事件时间和处理时间之间的延迟，单位为毫秒。 |

示例：

```hocon
transform {
  Metadata {
    metadata_fields {
      Database = database
      Table = table
      RowKind = rowKind
      EventTime = ts_ms
      Delay = delay
    }
  }
}
```

## 变更日志

<ChangeLog />
