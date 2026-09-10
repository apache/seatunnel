import ChangeLog from '../changelog/connector-cdc-gaussdb.md';

# GaussDB CDC

> GaussDB CDC source connector

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

The GaussDB CDC connector reads snapshot data and incremental data from GaussDB databases through the PostgreSQL-compatible logical replication protocol.

The current implementation reuses SeaTunnel's PostgreSQL CDC runtime. Configure a PostgreSQL-compatible JDBC URL, for example `jdbc:postgresql://host:port/database`, and use a Debezium PostgreSQL logical decoding plugin such as `pgoutput`.

The `mppdb_decoding` binary decoding protocol is not parsed by this connector.

## Using steps

1. Enable logical WAL on the GaussDB instance.

```sql
ALTER SYSTEM SET wal_level TO 'logical';
SELECT pg_reload_conf();
```

2. Make sure the CDC user can connect to the database and use logical replication.

3. Set replica identity to `FULL` for captured tables when update and delete events need complete row values.

```sql
ALTER TABLE your_schema.your_table REPLICA IDENTITY FULL;
```

## Source Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| url | String | Yes | - | JDBC URL of the GaussDB database. Use the PostgreSQL-compatible form, such as `jdbc:postgresql://localhost:5432/gaussdb_cdc?loggerLevel=OFF`. |
| username | String | Yes | - | Username used to connect to the database. |
| password | String | Yes | - | Password used to connect to the database. |
| database-names | List | No | - | Database names to monitor. |
| schema-name | List | No | - | Schema names to monitor. |
| table-names | List | Yes, if `table-pattern` is not used | - | Tables to monitor. Use the fully qualified `database.schema.table` format, for example `gaussdb_cdc.inventory.orders`. |
| table-pattern | String | Yes, if `table-names` is not used | - | Regular expression for fully qualified table names. `table-names` and `table-pattern` are mutually exclusive. |
| table-names-config | List | No | - | Per-table config list. Use `primaryKeys` for tables without a physical primary key and `snapshotSplitColumn` for a custom snapshot split column. |
| startup.mode | Enum | No | INITIAL | Startup mode. Supported values are `initial`, `snapshot-only`, `committed-offset`, `earliest`, and `latest`. |
| stop.mode | Enum | No | NEVER | Stop mode. The supported value is `never`. |
| snapshot.split.size | Integer | No | 8096 | Split size, in rows, for table snapshots. |
| snapshot.fetch.size | Integer | No | 1024 | Maximum fetch size for each snapshot query. |
| slot.name | String | No | seatunnel | Logical replication slot name. Use a different slot for each CDC job on the same GaussDB instance. |
| decoding.plugin.name | String | No | pgoutput | Logical decoding plugin name. Supported values follow the PostgreSQL CDC connector, such as `pgoutput`, `decoderbufs`, and `wal2json`. `mppdb_decoding` is not supported. |
| server-time-zone | String | No | UTC | Session time zone of the database server. |
| connect.timeout.ms | Duration | No | 30000 | Maximum connection timeout in milliseconds. |
| connect.max-retries | Integer | No | 3 | Maximum connection retry count. |
| connection.pool.size | Integer | No | 20 | JDBC connection pool size. |
| exactly_once | Boolean | No | false | Enable exactly-once semantics during the initial snapshot phase. This option is only available when `startup.mode` is `initial`. |
| format | Enum | No | DEFAULT | Output format. Supported values are `DEFAULT` and `COMPATIBLE_DEBEZIUM_JSON`. |
| debezium | Config | No | - | Pass-through Debezium properties for the embedded PostgreSQL CDC engine. |
| common-options |  | No | - | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details. |

## Task Example

```hocon
env {
  execution.parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  GaussDB-CDC {
    plugin_output = "customers_gaussdb_cdc"
    username = "gaussdb"
    password = "gaussdb_password"
    database-names = ["gaussdb_cdc"]
    schema-name = ["inventory"]
    table-names = ["gaussdb_cdc.inventory.orders"]
    url = "jdbc:postgresql://localhost:5432/gaussdb_cdc?loggerLevel=OFF"
    decoding.plugin.name = "pgoutput"
    slot.name = "seatunnel_gaussdb_cdc"
  }
}

sink {
  Console {
    plugin_input = "customers_gaussdb_cdc"
  }
}
```

## CDC Metadata Fields

GaussDB CDC exposes metadata fields that can be used by the `Metadata` transform:

| Field | Type | Description |
| --- | --- | --- |
| database | STRING | Source database name. |
| table | STRING | Source table name. |
| rowKind | STRING | Change type, such as insert, update, or delete. |
| ts_ms | LONG | Source event timestamp in milliseconds. |
| delay | LONG | Delay between event time and processing time in milliseconds. |

## Changelog

<ChangeLog />
