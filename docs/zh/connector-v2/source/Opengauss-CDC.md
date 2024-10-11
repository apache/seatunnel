# Opengauss CDC

> Opengauss CDC源连接器

## 支持这些引擎

> SeaTunnel Zeta<br/>
> Flink <br/>

## 主要功能

- [ ] [批处理](../../concept/connector-v2-features.md)
- [x] [流处理](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)
- [ ] [列投影](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)
- [x] [支持用户定义的拆分](../../concept/connector-v2-features.md)

## 描述

Opengauss CDC连接器允许从Opengauss数据库读取快照数据和增量数据。这个文档描述如何设置Opengauss CDC连接器以在Opengauss database中运行SQL查询。

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

|                      Name                      | Type | Required | Default  | Description                                                                                                                                                                                                       |
|------------------------------------------------|------|----------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| base-url                                       | 字符串  | 是        | -        | JDBC连接的URL. 参考: `jdbc:postgresql://localhost:5432/postgres_cdc?loggerLevel=OFF`.                                                                                                                                  |
| username                                       | 字符串  | 是        | -        | 连接数据库的用户名                                                                                                                                                                                                         |
| password                                       | 字符串  | 是        | -        | 连接数据库的密码                                                                                                                                                                                                          |
| database-names                                 | 列表   | 否        | -        | 监控的数据库名称                                                                                                                                                                                                          |
| table-names                                    | 列表   | 是        | -        | 监控的数据表名称. 表名需要包含数据库名称, 例如: `database_name.table_name`                                                                                                                                                             |
| table-names-config                             | 列表   | 否        | -        | 表配置的列表集合. 例如: [{"table": "db1.schema1.table1","primaryKeys":["key1"]}]                                                                                                                                            |
| startup.mode                                   | 枚举   | 否        | INITIAL  | Opengauss CDC消费者的可选启动模式, 有效的枚举是`initial`, `earliest`, `latest` and `specific`. <br/> `initial`: 启动时同步历史数据，然后同步增量数据 <br/> `earliest`: 从可能的最早偏移量启动 <br/> `latest`: 从最近的偏移量启动 <br/> `specific`: 从用户指定的偏移量启动          |
| snapshot.split.size                            | 整型   | 否        | 8096     | 表快照的分割大小（行数），在读取表的快照时，捕获的表被分割成多个split                                                                                                                                                                             |
| snapshot.fetch.size                            | 整型   | 否        | 1024     | 读取表快照时，每次轮询的最大读取大小                                                                                                                                                                                                |
| slot.name                                      | 字符串  | 否        | -        | Opengauss逻辑解码插槽的名称，该插槽是为特定数据库/模式的特定插件的流式更改而创建的。服务器使用此插槽将事件流传输到正在配置的连接器。默认值为seatunnel                                                                                                                              |
| decoding.plugin.name                           | 字符串  | 否        | pgoutput | 安装在服务器上的Postgres逻辑解码插件的名称，支持的值是decoderbufs、wal2json、wal2json_rds、wal2json_streaming、wal2json_rds_streaming和pgoutput                                                                                               |
| server-time-zone                               | 字符串  | 否        | UTC      | 数据库服务器中的会话时区。如果没有设置，则使用ZoneId.systemDefault()来确定服务器的时区                                                                                                                                                            |
| connect.timeout.ms                             | 时间间隔 | 否        | 30000    | 在尝试连接数据库服务器之后，连接器在超时之前应该等待的最大时间                                                                                                                                                                                   |
| connect.max-retries                            | 整型   | 否        | 3        | 连接器在建立数据库服务器连接时应该重试的最大次数                                                                                                                                                                                          |
| connection.pool.size                           | 整型   | 否        | 20       | jdbc连接池的大小                                                                                                                                                                                                        |
| chunk-key.even-distribution.factor.upper-bound | 双浮点型 | 否        | 100      | chunk的key分布因子的上界。该因子用于确定表数据是否均匀分布。如果分布因子被计算为小于或等于这个上界(即(MAX(id) - MIN(id) + 1) /行数)，表的所有chunk将被优化以达到均匀分布。否则，如果分布因子更大，则认为表分布不均匀，如果估计的分片数量超过`sample-sharding.threshold`指定的值，则将使用基于采样的分片策略。默认值为100.0。                |
| chunk-key.even-distribution.factor.lower-bound | 双浮点型 | 否        | 0.05     | chunk的key分布因子的下界。该因子用于确定表数据是否均匀分布。如果分布因子的计算结果大于或等于这个下界(即(MAX(id) - MIN(id) + 1) /行数)，那么表的所有块将被优化以达到均匀分布。否则，如果分布因子较小，则认为表分布不均匀，如果估计的分片数量超过`sample-sharding.threshold`指定的值，则使用基于采样的分片策略。缺省值为0.05。                   |
| sample-sharding.threshold                      | 整型   | 否        | 1000     | 此配置指定了用于触发采样分片策略的估计分片数的阈值。当分布因子超出了由`chunk-key.even-distribution.factor.upper-bound `和`chunk-key.even-distribution.factor.lower-bound`，并且估计的分片计数(以近似的行数/块大小计算)超过此阈值，则将使用样本分片策略。这有助于更有效地处理大型数据集。默认值为1000个分片。        |
| inverse-sampling.rate                          | 整型   | 否        | 1000     | 采样分片策略中使用的采样率的倒数。例如，如果该值设置为1000，则意味着在采样过程中应用了1/1000的采样率。该选项提供了控制采样粒度的灵活性，从而影响最终的分片数量。当处理非常大的数据集时，它特别有用，其中首选较低的采样率。缺省值为1000。                                                                                       |
| exactly_once                                   | 布尔   | 否        | false    | 启用exactly once语义                                                                                                                                                                                                  |
| format                                         | 枚举   | 否        | DEFAULT  | Opengauss CDC可选的输出格式, 有效的枚举是`DEFAULT`, `COMPATIBLE_DEBEZIUM_JSON`.                                                                                                                                                |
| debezium                                       | 配置   | 否        | -        | 将 [Debezium的属性](https://github.com/debezium/debezium/blob/v1.9.8.Final/documentation/modules/ROOT/pages/connectors/postgresql.adoc#connector-configuration-properties) 传递到Debezium嵌入式引擎，该引擎用于捕获来自Opengauss服务的数据更改 |
| common-options                                 |      | 否        | -        | 源码插件通用参数, 请参考[Source Common Options](../source-common-options.md)获取详情                                                                                                                                                                     |

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
    result_table_name = "customers_opengauss_cdc"
    username = "gaussdb"
    password = "openGauss@123"
    database-names = ["opengauss_cdc"]
    schema-names = ["inventory"]
    table-names = ["opengauss_cdc.inventory.opengauss_cdc_table_1","opengauss_cdc.inventory.opengauss_cdc_table_2"]
    base-url = "jdbc:postgresql://opengauss_cdc_e2e:5432/opengauss_cdc"
    decoding.plugin.name = "pgoutput"
  }
}

transform {

}

sink {
  jdbc {
    source_table_name = "customers_opengauss_cdc"
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
    result_table_name = "customers_opengauss_cdc"
    username = "gaussdb"
    password = "openGauss@123"
    database-names = ["opengauss_cdc"]
    schema-names = ["inventory"]
    table-names = ["opengauss_cdc.inventory.full_types_no_primary_key"]
    base-url = "jdbc:postgresql://opengauss_cdc_e2e:5432/opengauss_cdc?loggerLevel=OFF"
    decoding.plugin.name = "pgoutput"
    exactly_once = true
    table-names-config = [
      {
        table = "opengauss_cdc.inventory.full_types_no_primary_key"
        primaryKeys = ["id"]
      }
    ]
  }
}
```

