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

On cloud RDS, prefer a dedicated line or VPC endpoint together with `sslmode=disable`.

Some PostgreSQL-compatible databases (for example HighGo) return the same physical table more than
once from `information_schema.tables`. The connector deduplicates discovered tables by their fully
qualified `catalog.schema.table` id, so these repeated rows are ignored and the job starts normally.

On a type-name cache miss, an unqualified name shared by multiple schemas resolves to the type
visible on the source connection's `search_path`. This does not change cached names or add source
type namespaces to downstream `ADD COLUMN` statements.

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
| username                                  | String   | Yes      | -        | Username to use when connecting to the database server.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| password                                  | String   | Yes      | -        | Password to use when connecting to the database server.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| database-names                            | List     | No       | -        | Database names to monitor.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| table-names                               | List     | Yes, if `table-pattern` is not used | -        | Tables to monitor. Use the fully qualified `database.schema.table` format, for example: `postgres_cdc.inventory.orders`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| table-pattern                             | String   | Yes, if `table-names` is not used | -        | Regular expression for tables to monitor. Use the fully qualified table name in the pattern, for example: `postgres_cdc\\.inventory\\..*`. `table-names` and `table-pattern` are mutually exclusive.                                                                                                                                                                                                                                                                                                                                                                                                            |
| table-names-config                        | List     | No       | -       | Per-table config list. Example: `[{"table": "db1.schema1.table1","primaryKeys": ["key1"],"snapshotSplitColumn": "key2"}]`. Use `primaryKeys` for tables without a physical primary key. `snapshotSplitColumn` must be a unique key; otherwise SeaTunnel ignores it and selects a split column internally.                                                                                                                                                                                                                                                                                                                                                                          |
| startup.mode                              | Enum     | No       | INITIAL  | Optional startup mode for PostgreSQL CDC consumer, valid enumerations are `initial`, `snapshot-only`, `committed-offset`, `earliest` and `latest`. <br/> `initial`: Synchronize historical data at startup, and then synchronize incremental data.<br/> `snapshot-only`: Synchronize historical data at startup and finish as a bounded job without entering WAL streaming.<br/> `committed-offset`: Skip snapshot data and start WAL streaming from the configured replication slot's committed LSN. This mode requires an explicit `slot.name` and fails if the slot does not exist or has no usable committed LSN.<br/> `earliest`: Request streaming from the earliest WAL position (LSN `0/0`). PostgreSQL logical replication can only read WAL segments that have not been recycled and are not earlier than the slot's `restart_lsn`; if the earliest WAL has already been recycled, the job fails (`POSTGRES-05`) rather than skipping missing changes. Use `initial`, `latest`, or (for an existing slot) `committed-offset` instead.<br/> `latest`: Startup from the latest offset. |
| stop.mode                                 | Enum     | No       | NEVER    | Optional stop mode for PostgreSQL CDC consumer. The only valid enumeration is `never`: the source keeps streaming WAL changes and never stops on its own once it reaches the incremental phase.                                                                                                                                                                                                                                                                                                                                                                                                                     |
| snapshot.split.size                       | Integer  | No       | 8096     | The split size (number of rows) of table snapshot, captured tables are split into multiple splits when read the snapshot of table.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| snapshot.fetch.size                       | Integer  | No       | 1024     | The maximum fetch size for per poll when read table snapshot.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| slot.name                                 | String   | No       | seatunnel | The PostgreSQL logical replication slot name, which tracks the job's WAL consumption offset and prevents PostgreSQL from recycling unconsumed WAL segments. A replication slot can only be consumed by one active job at a time; configure a distinct `slot.name` for each CDC job on the same PostgreSQL instance (even when reading the same table).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| heartbeat.interval.ms                     | Long     | No       | 30000    | Interval in milliseconds for sending heartbeat events, default `30000` (30 seconds), or `0` to disable. When other tables in the database are being written to while the monitored tables are idle, heartbeat events keep advancing the replication slot offset (LSN) to prevent WAL accumulation. If the entire database has no writes at all, also configure `debezium.heartbeat.action.query` to periodically write to a heartbeat table. Also compatible with `debezium.heartbeat.interval.ms`; the top-level option takes precedence when both are set. |
| sslmode                                   | String   | No       | -        | SSL mode for the logical replication connection. Valid values: `disable`, `allow`, `prefer`, `require`, `verify-ca`, `verify-full`. If unset, `sslmode` in the JDBC `url` query parameters is used when present; otherwise it defaults to `prefer`.                                                                                                                                                                                                                                                                                              |
| publication.name                          | String   | No       | -        | PostgreSQL publication name used by the `pgoutput` plugin, defaulting to `dbz_publication` when unset. A publication defines which tables' changes PostgreSQL outputs on the server side (table filtering only; it does not store consumption progress). Effective only when `decoding.plugin.name = "pgoutput"` (default).                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| publication.autocreate.mode               | String   | No       | -        | Controls how the connector creates or uses the publication specified by `publication.name` (effective only for `pgoutput`, default `all_tables`):<br/> `all_tables`: If the publication does not exist, automatically runs `CREATE PUBLICATION ... FOR ALL TABLES` (requires database superuser privilege), and PostgreSQL outputs changes for all tables in the database.<br/> `filtered`: If the publication does not exist, automatically runs `CREATE PUBLICATION ... FOR TABLE ...` for the monitored tables only (requires `CREATE` privilege on the database and ownership of the monitored tables). When running multiple jobs with `filtered`, configure a different `publication.name` per job so they do not overwrite each other's table list.<br/> `disabled`: Never creates or alters the publication; uses the existing `publication.name` directly (and fails if it does not exist). Recommended for least-privilege production setups where an administrator pre-creates a `FOR TABLE` publication; multiple jobs can safely share the same pre-created publication. |
| decoding.plugin.name                      | String   | No       | pgoutput | The name of the Postgres logical decoding plug-in installed on the server,Supported values are decoderbufs, wal2json, wal2json_rds, wal2json_streaming,wal2json_rds_streaming and pgoutput.                                                                                                                                                                                                                                                                                                                                                                                                                          |
| server-time-zone                          | String   | No       | UTC      | The session time zone in database server. If not set, then ZoneId.systemDefault() is used to determine the server time zone.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| connect.timeout.ms                        | Duration | No       | 30000    | The maximum time that the connector should wait after trying to connect to the database server before timing out.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| connect.max-retries                       | Integer  | No       | 3        | Maximum number of automatic reconnect attempts when establishing a database connection or recovering from a transient disconnect during incremental streaming (logical replication stream or JDBC connection drop). Default is `3`. On a recoverable disconnect during incremental reading, the connector reconnects automatically and resumes from the last processed LSN in memory; once retries are exhausted, the job fails.                                                                                                                                                                                                                                                                                                                                                                                                                                        |
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
| schema-changes.enabled                    | Boolean  | No       | false    | Enable schema evolution events. PostgreSQL CDC currently supports only `ADD COLUMN`, and this option requires `decoding.plugin.name = "pgoutput"`. The change is observed when PostgreSQL sends the next RELATION message, normally immediately before the first subsequent row change for that table. |
| schema-changes.include                    | List     | No       | -        | Only the listed schema change event types are sent downstream when schema evolution is enabled. For the currently supported operation, use `add.column` (or the `update.columns` group alias). Empty means all supported types are eligible. |
| schema-changes.exclude                    | List     | No       | -        | Schema change event types listed here are not sent downstream. Exclude is applied after include and wins when the same type appears in both lists. |
| debezium                                  | Config   | No       | -        | Pass-through [Debezium's properties](https://github.com/debezium/debezium/blob/v1.9.8.Final/documentation/modules/ROOT/pages/connectors/postgresql.adoc#connector-configuration-properties) to Debezium Embedded Engine which is used to capture data changes from PostgreSQL server.                                                                                                                                                                                                                                                                                                                                |
| common-options                            |          | no       | -        | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |

## Task Example

### Simple

> Support multi-table reading

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Postgres-CDC {
    plugin_output = "customers_postgres_cdc"
    username = "postgres"
    password = "postgres"
    database-names = ["postgres_cdc"]
    table-names = ["postgres_cdc.inventory.postgres_cdc_table_1", "postgres_cdc.inventory.postgres_cdc_table_2"]
    url = "jdbc:postgresql://localhost:5432/postgres_cdc"
    slot.name = "seatunnel_postgres_cdc"
  }
}

transform {
}

sink {
  Jdbc {
    plugin_input = "customers_postgres_cdc"
    url = "jdbc:postgresql://localhost:5432/postgres_cdc"
    driver = "org.postgresql.Driver"
    username = "postgres"
    password = "postgres"
    generate_sink_sql = true
    database = "postgres_cdc"
    table = "inventory.sink_${table_name}"
    primary_keys = ["id"]
  }
}
```

### Schema evolution for ADD COLUMN

PostgreSQL does not put the original `ALTER TABLE` SQL text in the logical replication stream.
With the default `pgoutput` plugin, SeaTunnel detects the changed table definition from the RELATION message, emits an
`ADD COLUMN` event before the following row event, and updates the downstream table schema. A table
must therefore receive a row change after the DDL before the schema change can be observed.

If a RELATION message contains a row-schema change other than `ADD COLUMN`, the job fails before
processing rows with that schema. Restoring the same checkpoint will encounter the change again;
resume only after upgrading to a connector that supports the change or performing a controlled
schema migration and restart.

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

### Support custom primary key for table

```hocon
source {
  Postgres-CDC {
    plugin_output = "customers_postgres_cdc"
    username = "postgres"
    password = "postgres"
    database-names = ["postgres_cdc"]
    table-names = ["postgres_cdc.inventory.full_types_no_primary_key"]
    url = "jdbc:postgresql://localhost:5432/postgres_cdc"
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

### Configure sslmode and publication

In cloud RDS or permission-restricted production environments, the CDC user typically does not have database superuser privileges to run `CREATE PUBLICATION ... FOR ALL TABLES`. An administrator can pre-create a table-scoped publication on PostgreSQL:

```sql
CREATE PUBLICATION seatunnel_pub FOR TABLE inventory.postgres_cdc_table_1;
```

Then configure `publication.name = "seatunnel_pub"` and `publication.autocreate.mode = "disabled"` in the job, and set `sslmode = "disable"` when connecting over a dedicated line or VPC intranet:

```hocon
source {
  Postgres-CDC {
    username = "postgres"
    password = "postgres"
    database-names = ["postgres_cdc"]
    table-names = ["postgres_cdc.inventory.postgres_cdc_table_1"]
    url = "jdbc:postgresql://localhost:5432/postgres_cdc"
    slot.name = "seatunnel_postgres_cdc"
    sslmode = "disable"
    publication.name = "seatunnel_pub"
    publication.autocreate.mode = "disabled"
  }
}
```

### Configure Debezium heartbeat

A PostgreSQL replication slot offset only advances when WAL events are received:

- **Other tables in the same database have writes while the monitored tables are low-traffic**: `heartbeat.interval.ms` is enabled by default (`30000` ms) so the connector periodically advances the slot offset using WAL activity from other tables, preventing WAL accumulation. Set it to `0` to disable.
- **The entire database has no writes for extended periods**: also configure `debezium.heartbeat.action.query` to periodically write to a pre-created heartbeat table and generate WAL events (if using `publication.autocreate.mode = "disabled"` or `filtered`, include the heartbeat table in the publication as well).

```hocon
source {
  Postgres-CDC {
    username = "postgres"
    password = "postgres"
    database-names = ["postgres_cdc"]
    table-names = ["postgres_cdc.inventory.postgres_cdc_table_1"]
    url = "jdbc:postgresql://localhost:5432/postgres_cdc"
    slot.name = "seatunnel_postgres_cdc"
    heartbeat.interval.ms = 30000
    debezium {
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
    table-names = ["postgres_cdc.inventory.postgres_cdc_table_1"]
    url = "jdbc:postgresql://localhost:5432/postgres_cdc"
    slot.name = "seatunnel_postgres_cdc"
    startup.mode = "snapshot-only"
  }
}

sink {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/postgres_cdc"
    driver = "org.postgresql.Driver"
    username = "postgres"
    password = "postgres"
    generate_sink_sql = true
    database = "postgres_cdc"
    table = "inventory.sink_postgres_cdc_table_1"
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
  consistent row identity. The column must be NOT NULL in the database: a nullable column is not
  used to split the snapshot, and with `exactly_once = true` the job fails at startup.

```hocon
source {
  Postgres-CDC {
    username = "postgres"
    password = "postgres"
    database-names = ["postgres_cdc"]
    table-names = ["postgres_cdc.inventory.full_types_no_primary_key"]
    url = "jdbc:postgresql://localhost:5432/postgres_cdc"
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
GRANT USAGE ON SCHEMA public TO replication_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO replication_user;
```

Also set `wal_level = logical` in `postgresql.conf` and allow the user to make replication connections in `pg_hba.conf`.

When using the default `pgoutput` plugin, publication permissions depend on `publication.autocreate.mode`:
- `disabled` (recommended for production, least privilege): an administrator pre-creates `CREATE PUBLICATION <publication.name> FOR TABLE ...;`, and the CDC user needs no object-creation privileges.
- `filtered`: the CDC user needs `CREATE` privilege on the database and must be the owner of the monitored tables.
- `all_tables` (default): if the publication does not yet exist, the CDC user needs superuser (or cloud `rds_superuser`) privileges to create a `FOR ALL TABLES` publication.

### Which logical decoding plugins are supported?

SeaTunnel PostgreSQL CDC supports `pgoutput` (built-in since PostgreSQL 10), `wal2json`, and `decoderbufs`. The default is `pgoutput`. Use the `decoding.plugin.name` parameter to select the plugin.

### Can SeaTunnel read CDC from a PostgreSQL standby?

PostgreSQL logical replication slots must be created and consumed on the primary server. SeaTunnel cannot read a logical replication slot directly from a standby. Point the CDC connector at the primary instance.

### Does PostgreSQL CDC support tables without primary keys?

By default, PostgreSQL CDC requires primary keys. You can specify a custom primary key via `table-names-config` with the `primaryKeys` field if the table has a unique column that can serve as an identifier.

### How do replication slots (`slot.name`) and publications (`publication.name`) differ, and how are they managed?

- **Replication slot (`slot.name`)**: tracks a single CDC job's WAL consumption offset (LSN) and prevents PostgreSQL from recycling unconsumed WAL segments.
  - **Multiple jobs cannot share a replication slot**: even if multiple CDC jobs read the same table, each job must use a distinct `slot.name` (a slot can only have one active consumer at a time).
  - SeaTunnel creates or reuses `slot.name` automatically when a job starts (except under `startup.mode = committed-offset`, which requires an existing slot with a committed `confirmed_flush_lsn`).
  - When a CDC job is permanently decommissioned, drop its unused slot manually on PostgreSQL (`SELECT pg_drop_replication_slot('<slot.name>');`) so unconsumed WAL segments do not accumulate on disk.
- **Publication (`publication.name`)**: used only with the `pgoutput` plugin to define which tables' changes PostgreSQL outputs on the server side; **it does not store consumption progress**.
  - A table can belong to multiple publications at the same time.
  - With `publication.autocreate.mode = "disabled"`, multiple CDC jobs can safely share the same pre-created `publication.name` as long as it covers their tables. With `filtered`, configure a different `publication.name` for each job so jobs do not overwrite each other's publication table list on startup.

### Why does `startup.mode=earliest` fail with WAL LSN unavailable?

`startup.mode=earliest` requests streaming from the earliest WAL position (LSN `0/0`). Because PostgreSQL logical replication can only read WAL segments that have not been recycled and are not earlier than the slot's `restart_lsn`, if the target WAL segments have already been removed by PostgreSQL, SeaTunnel fails the job (`POSTGRES-05`) rather than silently skipping missing historical changes. Depending on your use case, switch to `initial` (snapshot + incremental), `latest` (incremental from current head), or `committed-offset` (resume from an existing slot's committed LSN).

### Why does the job fail with "replication slot ... has been invalidated"?

PostgreSQL 13 and later can invalidate a replication slot, for example when it falls behind
`max_slot_wal_keep_size`, stays inactive longer than `idle_replication_slot_timeout` (PostgreSQL 18),
or, on a standby, conflicts with recovery. `pg_replication_slots` then shows `wal_status = 'lost'`,
`conflicting = true` (PostgreSQL 16 and later) or `invalidation_reason` (PostgreSQL 17 and later).
The changes after the slot's confirmed position are gone, so SeaTunnel fails a job that reads changes from the slot
with error `POSTGRES-04`; `snapshot-only` jobs do not use the slot and are not affected.
Drop the slot with `SELECT pg_drop_replication_slot('<slot.name>')` and start the job
again without restoring from a checkpoint or savepoint; use `startup.mode = initial` to take a new snapshot.

### How are transaction boundaries handled during savepoint recovery?

Recovery distinguishes a saved transaction commit from a processed row, because a commit and the
next transaction's first row can share the same LSN. Rows committed while the job is stopped remain
eligible for replay. Existing savepoints without the saved operation type remain readable; recovery
may replay a row at an ambiguous boundary to avoid losing it.

### Why does PostgreSQL CDC fall behind?

Replication lag can occur when the logical decoding plugin is slow or when the WAL sender is under load. Monitor `pg_replication_slots` for `confirmed_flush_lsn` drift. Ensure the CDC job consumes events continuously and that network latency between SeaTunnel and PostgreSQL is low.

## See Also

For a production-grade end-to-end guide covering full + incremental synchronization lifecycle,
2PC sink configuration, schema evolution, and troubleshooting, see
[CDC Production Cookbook](../cdc-production-cookbook.md).

## Changelog

<ChangeLog />
