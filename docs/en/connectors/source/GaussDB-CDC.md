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

The connector natively reads the GaussDB `mppdb_decoding` logical decoding format. It supports serial JSON output and parallel binary, JSON, and TEXT output, including batched frames. Server-side slot progress is acknowledged only after a SeaTunnel checkpoint completes.

The repository's automated E2E uses openGauss only to exercise the shared `mppdb_decoding` wire protocol. It is protocol-compatibility coverage, not certification against a Huawei GaussDB release. Release validation must also run against a real GaussDB instance because this public E2E does not provision one.

Configure a PostgreSQL-compatible JDBC URL, for example `jdbc:postgresql://host:port/database`. PostgreSQL-compatible Debezium plugins such as `pgoutput` remain supported and use the PostgreSQL CDC runtime.

When `mppdb_decoding` is selected, the configured JDBC driver and replication port must expose the PostgreSQL-compatible logical replication API. The connector fails at startup if that API is unavailable; it does not consume uncheckpointed WAL through SQL polling. The driver placed in the `GaussDB-CDC` plugin directory must also implement the GaussDB replication protocol (for example the GaussDB or openGauss JDBC driver, which keep the `org.postgresql` package and API). The stock PostgreSQL JDBC driver can run the snapshot but cannot stream: GaussDB and openGauss reject its standby status update with `insufficient data left in message` because their `StandbyReplyMessage` carries additional fields.

## Using steps

1. Enable logical WAL on the GaussDB instance.

```sql
ALTER SYSTEM SET wal_level TO 'logical';
SELECT pg_reload_conf();
```

2. Make sure the CDC user can connect to the database, create or use a logical replication slot, and use logical replication.

3. Set replica identity to `FULL` for captured tables when update and delete events need complete row values.

```sql
ALTER TABLE your_schema.your_table REPLICA IDENTITY FULL;
```

4. Use a distinct `slot.name` for every concurrent CDC job. The slot must either not exist or already use the configured decoding plugin.

`mppdb_decoding` emits row changes but no PostgreSQL `RELATION` messages, so `schema-changes.enabled` must remain `false`. Use `pgoutput` when schema evolution is required.

When an unfinished transaction blocks checkpoint progress for five minutes, the reader logs a warning with its transaction id, age, buffered transaction count, and buffered row count. The warning repeats at most once every five minutes per reader, including while no new WAL arrives. Inspect long-running source transactions and COMMIT delivery. This diagnostic does not limit buffer memory, discard rows, or advance checkpoint acknowledgements.

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
| decoding.plugin.name | String | No | mppdb_decoding | Logical decoding plugin name. `mppdb_decoding` uses the native GaussDB reader. PostgreSQL-compatible Debezium plugins such as `pgoutput`, `decoderbufs`, and `wal2json` use the PostgreSQL CDC reader. |
| replication.port | Integer | No | Port in `url` | Dedicated port for the GaussDB replication connection. The valid range is 1 through 65535. |
| parallel-decode-num | Integer | No | 1 | Number of GaussDB server-side decoder threads for `mppdb_decoding`. The valid range is 1 through 20. A value greater than 1 enables parallel decoding. |
| decode-style | String | No | b | Parallel `mppdb_decoding` output style: `b` for binary, `j` for JSON, or `t` for TEXT. Effective only when `parallel-decode-num` is greater than 1. |
| sending-batch | Boolean | No | false | Whether parallel `mppdb_decoding` sends accumulated records in batches. Effective only when `parallel-decode-num` is greater than 1. |
| require-replica-identity-full | Boolean | No | true | Require captured tables to use `REPLICA IDENTITY FULL`. Set to `false` only when incomplete previous values for UPDATE and DELETE are acceptable. |
| schema-changes.enabled | Boolean | No | false | Enable schema evolution events. This requires a compatible plugin such as `pgoutput`; `mppdb_decoding` does not emit the required `RELATION` messages. |
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
    decoding.plugin.name = "mppdb_decoding"
    slot.name = "seatunnel_gaussdb_cdc"
    parallel-decode-num = 4
    decode-style = "b"
    sending-batch = true
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
