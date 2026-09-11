import ChangeLog from '../changelog/connector-cdc-postgres.md';

# PostgreSQL CDC

> PostgreSQL CDC 源连接器

## 支持的引擎

> SeaTunnel Zeta<br/>
> Flink <br/>

## 主要特性

- [ ] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行性](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户定义的拆分](../../introduction/concepts/connector-v2-features.md)

## 描述

PostgreSQL CDC 连接器允许从 PostgreSQL 数据库读取快照数据和增量数据。本文档介绍如何配置 PostgreSQL CDC 连接器。

## 支持的数据源信息

| 数据源      |                     支持的版本                      |        驱动        |                  Url                  |                                  Maven                                   |
|------------|-----------------------------------------------------|---------------------|---------------------------------------|--------------------------------------------------------------------------|
| PostgreSQL | 不同的依赖版本有不同的驱动类。                       | org.postgresql.Driver | jdbc:postgresql://localhost:5432/test | [下载](https://mvnrepository.com/artifact/org.postgresql/postgresql) |
| PostgreSQL | 如果您想在 PostgreSQL 中操作 GEOMETRY/GEOGRAPHY 类型。        | org.postgresql.Driver | jdbc:postgresql://localhost:5432/test | [下载](https://mvnrepository.com/artifact/net.postgis/postgis-jdbc)  |

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

2. 将指定表的 REPLICA 策略更改为 FULL，除非 `require-replica-identity-full` 设置为 `false`。

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

|                      名称                   | 类型       | 必需 | 默认  | 描述                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|-------------------------------------------|----------|------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String   | 是   | -        | JDBC 连接的 URL。参考案例：`jdbc:postgresql://localhost:5432/postgres_cdc?loggerLevel=OFF`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| username                                  | String   | 是   | -        | 连接到数据库服务器时使用的数据库名称。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| password                                  | String   | 是   | -        | 连接到数据库服务器时使用的密码。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| database-names                            | List     | 否   | -        | 需要监控的数据库名称。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| table-names                               | List     | 二选一 | -        | 需要监控的表。请使用完整的 `database.schema.table` 格式，例如：`postgres_cdc.inventory.orders`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| table-pattern                             | String   | 二选一 | -        | 需要监控的表名正则表达式。正则需要匹配完整表名，例如：`postgres_cdc\\.inventory\\..*`。`table-names` 和 `table-pattern` 互斥。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| table-names-config                        | List     | 否   | -        | 表级配置列表。例如：`[{"table": "db1.schema1.table1","primaryKeys": ["key1"],"snapshotSplitColumn": "key2"}]`。无物理主键表可通过 `primaryKeys` 指定唯一键。`snapshotSplitColumn` 必须是唯一键，否则 SeaTunnel 会忽略该配置并自动选择拆分列。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| startup.mode                              | Enum     | 否   | INITIAL  | PostgreSQL CDC 消费者的可选启动模式，有效枚举为 `initial`、`snapshot-only`、`committed-offset`、`earliest` 和 `latest`。<br/> `initial`: 启动时同步历史数据，然后同步增量数据。<br/> `snapshot-only`: 仅同步启动时的历史数据，然后以有界任务结束，不进入 WAL 流读取。<br/> `committed-offset`: 跳过快照数据，从配置的复制槽已提交 LSN 开始读取 WAL。该模式要求显式配置 `slot.name`，如果复制槽不存在或没有可用的已提交 LSN，则启动失败。<br/> `earliest`: 从可能的最早偏移量启动。<br/> `latest`: 从最新偏移量启动。 |
| stop.mode                                 | Enum     | 否   | NEVER    | PostgreSQL CDC 消费者的可选停止模式。唯一有效的枚举值是 `never`：一旦进入增量阶段，数据源会持续读取 WAL 变更，不会自行停止。                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| snapshot.split.size                       | Integer  | 否   | 8096     | 表快照的拆分大小（行数），捕获的表在读取表快照时被拆分成多个拆分。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| snapshot.fetch.size                       | Integer  | 否   | 1024     | 读取表快照时每次轮询的最大获取大小。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| slot.name                                 | String   | 否   | seatunnel | PostgreSQL 逻辑解码槽名称。同一个 PostgreSQL 实例上如果有多个 CDC 任务，请为每个任务配置不同的 `slot.name`。                                                                                                                                                                                                                                                                                                                                                      |
| decoding.plugin.name                      | String   | 否   | pgoutput | 安装在服务器上的 Postgres 逻辑解码插件的名称，支持的值有 decoderbufs、wal2json、wal2json_rds、wal2json_streaming、wal2json_rds_streaming 和 pgoutput。                                                                                                                                                                                                                                                                                                                                                                                                                          |
| server-time-zone                          | String   | 否   | UTC      | 数据库服务器中的会话时区。如果未设置，则使用 ZoneId.systemDefault() 来确定服务器时区。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| connect.timeout.ms                        | Duration | 否   | 30000    | 连接器在尝试连接到数据库服务器后应等待的最大时间，以防超时。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| connect.max-retries                       | Integer  | 否   | 3        | 连接器应重试建立数据库服务器连接的最大重试次数。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| connection.pool.size                      | Integer  | 否   | 20       | JDBC 连接池大小。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| chunk-key.even-distribution.factor.upper-bound | Double   | 否   | 100      | 块键分布因子的上限。此因子用于确定表数据是否均匀分布。如果计算出的分布因子小于或等于此上限（即 (MAX(id) - MIN(id) + 1) / 行数），则将优化表块以实现均匀分布。否则，如果分布因子更大，则将认为该表分布不均匀，并且如果估计的分片数量超过 `sample-sharding.threshold` 指定的值，则将使用基于采样的分片策略。默认值为 100.0。 |
| chunk-key.even-distribution.factor.lower-bound | Double   | 否   | 0.05     | 块键分布因子的下限。此因子用于确定表数据是否均匀分布。如果计算出的分布因子大于或等于此下限（即 (MAX(id) - MIN(id) + 1) / 行数），则将优化表块以实现均匀分布。否则，如果分布因子更小，则将认为该表分布不均匀，并且如果估计的分片数量超过 `sample-sharding.threshold` 指定的值，则将使用基于采样的分片策略。默认值为 0.05。  |
| sample-sharding.threshold                 | Integer  | 否   | 1000     | 此配置指定触发采样分片策略的估计分片数量阈值。当分布因子超出由 `chunk-key.even-distribution.factor.upper-bound` 和 `chunk-key.even-distribution.factor.lower-bound` 指定的范围，且估计的分片数量（计算为近似行数 / 块大小）超过此阈值时，将使用采样分片策略。这可以帮助更有效地处理大数据集。默认值为 1000 个分片。                                                                                   |
| inverse-sampling.rate                     | Integer  | 否   | 1000     | 在采样分片策略中使用的采样率的倒数。例如，如果此值设置为 1000，则意味着在采样过程中应用 1/1000 的采样率。此选项提供了控制采样粒度的灵活性，从而影响最终的分片数量。在处理非常大数据集时，较低的采样率尤为有用。默认值为 1000。                                                                                                                                                              |
| split.allow-sampling                      | Boolean  | 否   | true     | 是否允许基于采样的分片策略。当设置为 false 时，无论预估分片数是否超过阈值，系统都将回退到非均匀分片方式（迭代查询方式）。默认值为 true。 |
| enable_concurrent_read                    | Boolean  | 否   | true     | 是否在快照阶段启用基于分片的并发读取。当设置为 false 时，source 会跳过分片分析，并以单个 split 读取整张表，适合没有索引的表。默认值为 true。 |
| exactly_once                              | Boolean  | 否   | false    | 在快照阶段启用精确一次语义。仅当 `startup.mode` 为 `initial` 或 `snapshot-only` 时可用。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| format                                    | Enum     | 否   | DEFAULT  | PostgreSQL CDC 的可选输出格式，有效枚举为 `DEFAULT`、`COMPATIBLE_DEBEZIUM_JSON`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| require-replica-identity-full             | Boolean  | 否   | true     | 要求表具有 REPLICA IDENTITY FULL。设置为 false 时，允许表使用其他副本标识设置，但 UPDATE/DELETE 事件可能不包含之前的状态。此选项仅应用于仅追加的表（例如 outbox 模式）。默认为 true 以保持向后兼容性。                                                                                                                                                                                                                                                                                                             |
| schema-changes.enabled                    | Boolean  | 否   | false    | 启用 Schema 演进事件。PostgreSQL CDC 当前仅支持 `ADD COLUMN`，并且要求 `decoding.plugin.name = "pgoutput"`。PostgreSQL 发送下一条 RELATION 消息时才能感知该变更，通常发生在该表 DDL 后的第一条行变更之前。 |
| schema-changes.include                    | List     | 否   | -        | Schema 演进启用后，仅向下游发送列出的事件类型。当前支持的操作请使用 `add.column`（或分组别名 `update.columns`）。为空表示允许全部受支持的类型。 |
| schema-changes.exclude                    | List     | 否   | -        | 不向下游发送列出的 Schema change 事件类型。先应用 include，再应用 exclude；同一类型同时出现时 exclude 优先。 |
| debezium                                  | Config   | 否   | -        | 将 [Debezium 的属性](https://github.com/debezium/debezium/blob/v1.9.8.Final/documentation/modules/ROOT/pages/connectors/postgresql.adoc#connector-configuration-properties) 传递给用于捕获 PostgreSQL 服务器数据更改的 Debezium 嵌入式引擎。                                                                                                                                                                                                                                                                                                                                |
| common-options                            |          | 否   | -        | 源插件的公共参数，请参阅 [源公共选项](../common-options/source-common-options.md) 获取详细信息。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |

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
    plugin_output = "customers_postgres_cdc"
    username = "postgres"
    password = "postgres"
    database-names = ["postgres_cdc"]
    table-names = ["postgres_cdc.inventory.postgres_cdc_table_1", "postgres_cdc.inventory.postgres_cdc_table_2"]
    url = "jdbc:postgresql://postgres_cdc_e2e:5432/postgres_cdc?loggerLevel=OFF"
    decoding.plugin.name = "decoderbufs"
    slot.name = "seatunnel_postgres_cdc"
  }
}

transform {

}

sink {
  jdbc {
    plugin_input = "customers_postgres_cdc"
    url = "jdbc:postgresql://postgres_cdc_e2e:5432/postgres_cdc?loggerLevel=OFF"
    driver = "org.postgresql.Driver"
    username = "postgres"
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

### ADD COLUMN Schema 演进

PostgreSQL 不会把原始 `ALTER TABLE` SQL 文本写入逻辑复制流。使用 `pgoutput` 时，SeaTunnel 会从
RELATION 消息中检测变化后的表结构，在后续行事件之前发出 `ADD COLUMN` 事件，并更新下游表结构。
因此，执行 DDL 后该表必须再发生一条行变更，连接器才能感知 Schema 变化。

如果 RELATION 消息包含 `ADD COLUMN` 之外的行 Schema 变更，作业会在处理新 Schema 的数据行之前
失败。恢复同一 Checkpoint 时仍会再次遇到该变更；只有升级到支持该变更的连接器，或完成受控的
Schema 迁移并重新启动作业后，才能继续处理。

```hocon
source {
  Postgres-CDC {
    # ...
    decoding.plugin.name = "pgoutput"
    schema-changes.enabled = true
    schema-changes.include = ["add.column"]
  }
}
```

### 支持自定义表的主键

```
source {
  Postgres-CDC {
    plugin_output = "customers_postgres_cdc"
    username = "postgres"
    password = "postgres"
    database-names = ["postgres_cdc"]
    table-names = ["postgres_cdc.inventory.full_types_no_primary_key"]
    url = "jdbc:postgresql://postgres_cdc_e2e:5432/postgres_cdc?loggerLevel=OFF"
    decoding.plugin.name = "decoderbufs"
    exactly_once = true
    slot.name = "seatunnel_postgres_cdc"
    table-names-config = [
      {
        table = "postgres_cdc.inventory.full_types_no_primary_key"
        primaryKeys = ["id"]
      }
    ]
  }
}
```

### 配置 Debezium 心跳

对于低流量表，Postgres 逻辑解码槽的位置只有在 WAL 中发生行变更时才会推进。使用 Debezium 心跳让槽位持续推进，便于 checkpoint 定期记录偏移，并让复制延迟可观测。心跳表必须提前在 Postgres 服务端创建。

```hocon
source {
  Postgres-CDC {
    username = "postgres"
    password = "postgres"
    database-names = ["postgres_cdc"]
    schema-names = ["inventory"]
    table-names = ["postgres_cdc.inventory.postgres_cdc_table_1"]
    url = "jdbc:postgresql://postgres_cdc_e2e:5432/postgres_cdc?loggerLevel=OFF"
    decoding.plugin.name = "decoderbufs"
    slot.name = "seatunnel_postgres_cdc"
    debezium {
      heartbeat.interval.ms = 100
      heartbeat.action.query = "INSERT INTO inventory.heartbeat (ts) VALUES (NOW())"
    }
  }
}
```

### 仅运行一次性快照

当任务只需要执行初始快照并停止（不进入 WAL 流式读取）时，使用 `startup.mode = "snapshot-only"`。该模式适合一次性数据回填。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
  checkpoint.interval = 5000
}

source {
  Postgres-CDC {
    username = "postgres"
    password = "postgres"
    database-names = ["postgres_cdc"]
    schema-names = ["inventory"]
    table-names = ["postgres_cdc.inventory.postgres_cdc_table_1"]
    url = "jdbc:postgresql://postgres_cdc_e2e:5432/postgres_cdc?loggerLevel=OFF"
    decoding.plugin.name = "decoderbufs"
    slot.name = "seatunnel_postgres_cdc"
    startup.mode = "snapshot-only"
  }
}

sink {
  Jdbc {
    url = "jdbc:postgresql://postgres_cdc_e2e:5432/postgres_cdc?loggerLevel=OFF"
    driver = "org.postgresql.Driver"
    username = "postgres"
    password = "postgres"
    generate_sink_sql = true
    database = postgres_cdc
    table = inventory.sink_postgres_cdc_table_1
    primary_keys = ["id"]
  }
}
```

`snapshot-only` 模式下，connector 完全跳过 WAL 流式读取；如果快照读取需要独立的复制槽，请配置 `slot.name`。

### 读取没有主键的表

根据源表能够提供的保证来选择合适的路径：

- **仅追加（append-only）场景**：源表不会产生 UPDATE/DELETE 事件，保持 `exactly_once = false` 且不声明主键，源端会退回到尽力而为的行标识。在没有可用主键的情况下，connector 无法安全地应用 UPDATE/DELETE 事件。
- **存在唯一非主键列**：通过 `table-names-config.primaryKeys` 显式声明该列，并设置 `exactly_once = true`，让快照阶段与 WAL 阶段都使用同一配置主键作为稳定的行标识。

```hocon
source {
  Postgres-CDC {
    username = "postgres"
    password = "postgres"
    database-names = ["postgres_cdc"]
    schema-names = ["inventory"]
    table-names = ["postgres_cdc.inventory.full_types_no_primary_key"]
    url = "jdbc:postgresql://postgres_cdc_e2e:5432/postgres_cdc?loggerLevel=OFF"
    decoding.plugin.name = "decoderbufs"
    table-names-config = [
      {
        table = "postgres_cdc.inventory.full_types_no_primary_key"
        primaryKeys = ["id"]
      }
    ]
    exactly_once = true
    slot.name = "seatunnel_postgres_cdc"
  }
}
```

没有可用的主键时，connector 无法安全地应用 UPDATE/DELETE 事件。仅在仅追加（append-only）场景下使用此模式。

## CDC 元数据字段

PostgreSQL CDC 会提供以下元数据字段，可配合 `Metadata` 转换使用：

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

## 常见问题

### PostgreSQL CDC 需要哪些权限？

CDC 用户需要具备 `REPLICATION` 角色以及对监控表的 `SELECT` 权限：

```sql
CREATE USER replication_user REPLICATION LOGIN PASSWORD 'password';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replication_user;
```

同时需要在 `postgresql.conf` 中设置 `wal_level = logical`，并在 `pg_hba.conf` 中添加允许该用户复制连接的条目。

### 支持哪些逻辑解码插件？

SeaTunnel PostgreSQL CDC 支持 `pgoutput`（PostgreSQL 10 起内置）、`wal2json` 和 `decoderbufs`，默认使用 `pgoutput`。通过 `decoding.plugin.name` 参数指定插件。

### SeaTunnel 能从 PostgreSQL 备库读取 CDC 数据吗？

不能。PostgreSQL 逻辑复制槽必须在主库上创建和消费，SeaTunnel 无法直接从备库读取逻辑复制槽，需将 CDC 连接器指向主库实例。

### PostgreSQL CDC 是否支持无主键表？

默认需要主键。如果表有可作为唯一标识的列，可以通过 `table-names-config` 中的 `primaryKeys` 字段自定义主键。

### 复制槽如何管理？

SeaTunnel 在任务启动时会创建或复用 `slot.name` 指定的复制槽。当 `startup.mode` 为
`committed-offset` 时，复制槽必须已存在，因为 SeaTunnel 会使用其 `confirmed_flush_lsn`
作为启动偏移量。未使用的复制槽会持续占用磁盘上的 WAL 段，导致 WAL 持续增长。当 CDC
任务永久下线时，应在 PostgreSQL 侧手动删除不再使用的复制槽。

### PostgreSQL CDC 为什么会滞后？

滞后可能由逻辑解码插件处理慢或 WAL sender 负载过高引起。可通过监控 `pg_replication_slots` 中的 `confirmed_flush_lsn` 漂移情况来排查。确保 CDC 任务持续消费事件，并保持 SeaTunnel 与 PostgreSQL 之间的网络低延迟。

## 另请参阅

若需要一份面向生产的端到端实践指南，涵盖全量 + 增量同步生命周期、2PC sink 配置、Schema 演进与常见故障排查，请参阅 [CDC 生产实战手册](../cdc-production-cookbook.md)。

## 变更日志

<ChangeLog />
