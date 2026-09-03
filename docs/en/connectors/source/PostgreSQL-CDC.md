import ChangeLog from '../changelog/connector-cdc-postgres.md';

# PostgreSQL CDC

> PostgreSQL CDC source connector

## Support Those Engines

> SeaTunnel Zeta<br/>
> Flink <br/>

## Key features

- [ ] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Description

The PostgreSQL CDC connector allows for reading snapshot data and incremental data from PostgreSQL databases. This document
describes how to set up the PostgreSQL CDC connector.

## Supported DataSource Info

| Datasource |                     Supported versions                     |        Driver         |                  Url                  |                                  Maven                                   |
|------------|------------------------------------------------------------|-----------------------|---------------------------------------|--------------------------------------------------------------------------|
| PostgreSQL | Different dependency version has different driver class.   | org.postgresql.Driver | jdbc:postgresql://localhost:5432/test | [Download](https://mvnrepository.com/artifact/org.postgresql/postgresql) |
| PostgreSQL | If you want to manipulate the GEOMETRY/GEOGRAPHY type in PostgreSQL. | org.postgresql.Driver | jdbc:postgresql://localhost:5432/test | [Download](https://mvnrepository.com/artifact/net.postgis/postgis-jdbc)  |

## Using Dependency

### Install Jdbc Driver

#### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/org.postgresql/postgresql) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

#### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/org.postgresql/postgresql) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

Please download and put PostgreSQL driver in `${SEATUNNEL_HOME}/lib/` dir. For example: cp postgresql-xxx.jar `$SEATUNNEL_HOME/lib/`

> Here are the steps to enable CDC (Change Data Capture) in PostgreSQL:

1. Ensure the wal_level is set to logical: Modify the postgresql.conf configuration file by adding "wal_level = logical",
   restart the PostgreSQL server for the changes to take effect.
   Alternatively, you can use SQL commands to modify the configuration directly:

```sql
ALTER SYSTEM SET wal_level TO 'logical';
SELECT pg_reload_conf();
```

2. Change the REPLICA policy of the specified table to FULL, unless `require-replica-identity-full` is set to `false`.

```sql
ALTER TABLE your_table_name REPLICA IDENTITY FULL;
```

## Data Type Mapping

|                                  PostgreSQL Data type                                   |                                                              SeaTunnel Data type                                                               |
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
| NUMERIC(Get the designated column's specified column size>0)                            | DECIMAL(Get the designated column's specified column size,Gets the number of digits in the specified column to the right of the decimal point) |
| NUMERIC(Get the designated column's specified column size<0)                            | DECIMAL(38, 18)                                                                                                                                |
| BPCHAR<br/>CHARACTER<br/>VARCHAR<br/>TEXT<br/>GEOMETRY<br/>GEOGRAPHY<br/>JSON<br/>JSONB | STRING                                                                                                                                         |
| _BPCHAR<br/>_CHARACTER<br/>_VARCHAR<br/>_TEXT                                           | ARRAY&LT;STRING&GT;                                                                                                                            |
| TIMESTAMP<br/>                                                                          | TIMESTAMP                                                                                                                                      |
| TIME<br/>                                                                               | TIME                                                                                                                                           |
| DATE<br/>                                                                               | DATE                                                                                                                                           |
| OTHER DATA TYPES                                                                        | NOT SUPPORTED YET                                                                                                                              |

## Source Options

|                      Name                 |   Type   | Required | Default  | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|-------------------------------------------|----------|----------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String   | Yes      | -        | The URL of the JDBC connection. Refer to a case: `jdbc:postgresql://localhost:5432/postgres_cdc?loggerLevel=OFF`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| username                                  | String   | Yes      | -        | Name of the database to use when connecting to the database server.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| password                                  | String   | Yes      | -        | Password to use when connecting to the database server.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| database-names                            | List     | No       | -        | Database names to monitor.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| table-names                               | List     | Yes, if `table-pattern` is not used | -        | Tables to monitor. Use the fully qualified `database.schema.table` format, for example: `postgres_cdc.inventory.orders`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| table-pattern                             | String   | Yes, if `table-names` is not used | -        | Regular expression for tables to monitor. Use the fully qualified table name in the pattern, for example: `postgres_cdc\\.inventory\\..*`. `table-names` and `table-pattern` are mutually exclusive.                                                                                                                                                                                                                                                                                                                                                                                                            |
| table-names-config                        | List     | No       | -       | Per-table config list. Example: `[{"table": "db1.schema1.table1","primaryKeys": ["key1"],"snapshotSplitColumn": "key2"}]`. Use `primaryKeys` for tables without a physical primary key. `snapshotSplitColumn` must be a unique key; otherwise SeaTunnel ignores it and selects a split column internally.                                                                                                                                                                                                                                                                                                                                                                          |
| startup.mode                              | Enum     | No       | INITIAL  | Optional startup mode for PostgreSQL CDC consumer, valid enumerations are `initial`, `snapshot-only`, `committed-offset`, `earliest` and `latest`. <br/> `initial`: Synchronize historical data at startup, and then synchronize incremental data.<br/> `snapshot-only`: Synchronize historical data at startup and finish as a bounded job without entering WAL streaming.<br/> `committed-offset`: Skip snapshot data and start WAL streaming from the configured replication slot's committed LSN. This mode requires an explicit `slot.name` and fails if the slot does not exist or has no usable committed LSN.<br/> `earliest`: Startup from the earliest offset possible.<br/> `latest`: Startup from the latest offset. |
| stop.mode                                 | Enum     | No       | NEVER    | Optional stop mode for PostgreSQL CDC consumer. The only valid enumeration is `never`: the source keeps streaming WAL changes and never stops on its own once it reaches the incremental phase.                                                                                                                                                                                                                                                                                                                                                                                                                     |
| snapshot.split.size                       | Integer  | No       | 8096     | The split size (number of rows) of table snapshot, captured tables are split into multiple splits when read the snapshot of table.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| snapshot.fetch.size                       | Integer  | No       | 1024     | The maximum fetch size for per poll when read table snapshot.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| slot.name                                 | String   | No       | seatunnel | The PostgreSQL logical decoding slot name. Use a different slot name for each CDC job that reads from the same PostgreSQL instance.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| decoding.plugin.name                      | String   | No       | pgoutput | The name of the Postgres logical decoding plug-in installed on the server,Supported values are decoderbufs, wal2json, wal2json_rds, wal2json_streaming,wal2json_rds_streaming and pgoutput.                                                                                                                                                                                                                                                                                                                                                                                                                          |
| server-time-zone                          | String   | No       | UTC      | The session time zone in database server. If not set, then ZoneId.systemDefault() is used to determine the server time zone.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| connect.timeout.ms                        | Duration | No       | 30000    | The maximum time that the connector should wait after trying to connect to the database server before timing out.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| connect.max-retries                       | Integer  | No       | 3        | The max retry times that the connector should retry to build database server connection.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| connection.pool.size                      | Integer  | No       | 20       | The jdbc connection pool size.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| chunk-key.even-distribution.factor.upper-bound | Double   | No       | 100      | The upper bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be less than or equal to this upper bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is greater, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 100.0. |
| chunk-key.even-distribution.factor.lower-bound | Double   | No       | 0.05     | The lower bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be greater than or equal to this lower bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is less, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 0.05.  |
| sample-sharding.threshold                 | Integer  | No       | 1000     | This configuration specifies the threshold of estimated shard count to trigger the sample sharding strategy. When the distribution factor is outside the bounds specified by `chunk-key.even-distribution.factor.upper-bound` and `chunk-key.even-distribution.factor.lower-bound`, and the estimated shard count (calculated as approximate row count / chunk size) exceeds this threshold, the sample sharding strategy will be used. This can help to handle large datasets more efficiently. The default value is 1000 shards.                                                                                   |
| inverse-sampling.rate                     | Integer  | No       | 1000     | The inverse of the sampling rate used in the sample sharding strategy. For example, if this value is set to 1000, it means a 1/1000 sampling rate is applied during the sampling process. This option provides flexibility in controlling the granularity of the sampling, thus affecting the final number of shards. It's especially useful when dealing with very large datasets where a lower sampling rate is preferred. The default value is 1000.                                                                                                                                                              |
| split.allow-sampling                      | Boolean  | No       | true     | Whether to allow sampling-based sharding strategy. When set to false, the system will fall back to unevenly-sized chunk splitting (iterative query approach) regardless of the shard count. The default value is true. |
| enable_concurrent_read                    | Boolean  | No       | true     | Whether to enable concurrent read with split during the snapshot phase. When set to false, the source skips split analysis and reads the table as a single split, which is useful for tables without indexes. The default value is true. |
| exactly_once                              | Boolean  | No       | false    | Enable exactly-once semantics during the snapshot phase. This option is only available when `startup.mode` is `initial` or `snapshot-only`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| format                                    | Enum     | No       | DEFAULT  | Optional output format for PostgreSQL CDC, valid enumerations are `DEFAULT`, `COMPATIBLE_DEBEZIUM_JSON`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| require-replica-identity-full             | Boolean  | No       | true     | Require the table to have REPLICA IDENTITY FULL. When set to false, allows tables with other replica identity settings, but UPDATE/DELETE events may not contain the previous state. This should only be used for append-only tables (e.g., outbox pattern). Default is true for backward compatibility.                                                                                                                                                                                                                                                                                                             |
| debezium                                  | Config   | No       | -        | Pass-through [Debezium's properties](https://github.com/debezium/debezium/blob/v1.9.8.Final/documentation/modules/ROOT/pages/connectors/postgresql.adoc#connector-configuration-properties) to Debezium Embedded Engine which is used to capture data changes from PostgreSQL server.                                                                                                                                                                                                                                                                                                                                |
| common-options                            |          | no       | -        | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |

## Task Example

### Simple

> Support multi-table reading

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

### Support custom primary key for table

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

### Configure Debezium heartbeat

For low-traffic tables, the Postgres logical decoding slot position only advances when row changes are written to the WAL. A Debezium heartbeat keeps the slot advancing so checkpoint offsets are recorded regularly and replication lag stays observable. The heartbeat table must exist on the Postgres server before the job starts.

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

### Run a snapshot-only batch

Use `startup.mode = "snapshot-only"` when the job must perform an initial snapshot and stop without entering WAL streaming. This is useful for one-time backfills.

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

In `snapshot-only` mode, the connector skips WAL streaming entirely; configure `slot.name` if you need a dedicated slot for the snapshot read.

### Read tables without a primary key

Pick the path that matches what the source table guarantees:

- **Append-only workload** (no UPDATE/DELETE will ever be produced downstream): keep
  `exactly_once = false` and do not declare a primary key. The source falls back to a best-effort
  row identity. Without a usable key, the connector cannot apply UPDATE/DELETE events safely.
- **Unique non-primary column is available**: declare it via `table-names-config.primaryKeys` and
  set `exactly_once = true` so the snapshot and WAL phases both use the configured key for
  consistent row identity.

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

Without a usable primary key, the connector cannot safely apply UPDATE/DELETE events. Use this mode only for append-only workloads.

## CDC Metadata Fields

PostgreSQL CDC exposes metadata fields that can be used by the `Metadata` transform:

| Field | Type | Description |
|-------|------|-------------|
| database | STRING | Source database name. |
| table | STRING | Source table name. |
| rowKind | STRING | Change type, such as insert, update, or delete. |
| ts_ms | LONG | Source event timestamp in milliseconds. |
| delay | LONG | Delay between event time and processing time in milliseconds. |

Example:

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

## FAQ

### What PostgreSQL permissions are required for CDC?

The CDC user must have the `REPLICATION` role and `SELECT` access to the monitored tables:

```sql
CREATE USER replication_user REPLICATION LOGIN PASSWORD 'password';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replication_user;
```

Also set `wal_level = logical` in `postgresql.conf` and add an entry in `pg_hba.conf` to allow the replication connection.

### Which logical decoding plugins are supported?

SeaTunnel PostgreSQL CDC supports `pgoutput` (built-in since PostgreSQL 10), `wal2json`, and `decoderbufs`. The default is `pgoutput`. Use the `decoding.plugin.name` parameter to select the plugin.

### Can SeaTunnel read CDC from a PostgreSQL standby?

PostgreSQL logical replication slots must be created and consumed on the primary server. SeaTunnel cannot read a logical replication slot directly from a standby. Point the CDC connector at the primary instance.

### Does PostgreSQL CDC support tables without primary keys?

By default, PostgreSQL CDC requires primary keys. You can specify a custom primary key via `table-names-config` with the `primaryKeys` field if the table has a unique column that can serve as an identifier.

### How are replication slots managed?

SeaTunnel creates or reuses the replication slot identified by `slot.name` when the job starts.
When `startup.mode` is `committed-offset`, the replication slot must already exist because SeaTunnel
uses its `confirmed_flush_lsn` as the startup offset.
Unused replication slots hold WAL segments on disk, which can cause unbounded WAL growth. When a
CDC job is permanently decommissioned, drop the unused replication slot manually on PostgreSQL.

When `exactly_once = true` and `startup.mode = initial`, SeaTunnel prepares the configured
streaming slot before any snapshot reader records its low watermark. Each snapshot reader then uses
a short-lived backfill slot derived from `slot.name` and the reader subtask id to read the bounded
WAL range between the snapshot low and high watermarks. The generated backfill slot name is kept
within PostgreSQL's 63-byte identifier limit and is dropped explicitly after the bounded backfill
reader finishes.

If Debezium `slot.drop.on.stop` is set to `true`, the snapshot enumerator drops the configured
streaming slot when the exactly-once initial snapshot job closes and no active incremental reader
still owns that slot. Temporary backfill slots are always cleaned up by the snapshot reader. During
snapshot startup, operators may therefore briefly see both the configured `slot.name` and generated
`*_st_backfill_*` slots in `pg_replication_slots`.

### Why does PostgreSQL CDC fall behind?

Replication lag can occur when the logical decoding plugin is slow or when the WAL sender is under load. Monitor `pg_replication_slots` for `confirmed_flush_lsn` drift. Ensure the CDC job consumes events continuously and that network latency between SeaTunnel and PostgreSQL is low.

## See Also

For a production-grade end-to-end guide covering full + incremental synchronization lifecycle,
2PC sink configuration, schema evolution, and troubleshooting, see
[CDC Production Cookbook](../cdc-production-cookbook.md).

## Changelog

<ChangeLog />
