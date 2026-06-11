import ChangeLog from '../changelog/connector-cdc-vitess.md';

# Vitess CDC

> Vitess CDC source connector

## Support Those Engines

> SeaTunnel Zeta<br/>
> Flink <br/>

## Description

The Vitess CDC connector captures change events from Vitess VTGate through the VStream gRPC API.
The first delivery keeps the connector intentionally narrow:

- streaming only, no initial snapshot phase
- explicit schema metadata only, provided through `schema` or `tables_configs`
- optional `table-names` / `table-pattern` filters over those declared schemas
- checkpoint / restore based on serialized Vitess VGTID state
- rows emitted as SeaTunnel CDC rows for existing multi-table downstream paths

If you need a reproducible bootstrap position, use `startup.mode = SPECIFIC` with a concrete
Vitess VGTID. `LATEST` is provided as a convenience startup mode aligned with existing Vitess CDC
backends, but its initial position is symbolic until the first CDC event materializes a concrete
offset.

## Key features

- [ ] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Supported DataSource Info

| Datasource | Supported versions | Driver | Url | Maven |
| --- | --- | --- | --- | --- |
| Vitess VTGate VStream | VTGate deployments compatible with Debezium Vitess 1.9.8.Final | gRPC client built into the connector | `hostname` + `port` | https://mvnrepository.com/artifact/io.debezium/debezium-connector-vitess/1.9.8.Final |

## Using Dependency

No JDBC driver is required for the connector runtime itself because CDC traffic is read through
VTGate gRPC. If you use JDBC for verification or downstream examples, add the MySQL JDBC driver
separately.

## Source Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| hostname | String | Yes | - | Hostname or IP address of the Vitess VTGate gRPC server. |
| port | Int | No | 15991 | Port of the Vitess VTGate gRPC server. |
| keyspace | String | Yes | - | Vitess keyspace captured by the connector. |
| schema | Config | Yes* | - | Single-table schema definition. The schema block must provide `table` plus either `columns` or `metadata_table_id`. |
| tables_configs | List\<Map\> | Yes* | - | Multi-table schema definitions. Each entry must contain a `schema` block with `table` plus either `columns` or `metadata_table_id`. |
| table-names | List | No** | - | Optional database-qualified tables to capture from the declared schema set, for example `commerce.orders`. |
| table-pattern | String | No** | - | Optional regular expression used to filter the declared schema set. |
| metalake_type | Enum | No | GRAVITINO | Metadata lake implementation used when a schema block resolves columns through `metadata_table_id`. |
| startup.mode | Enum | No | LATEST | Supported values are `latest` and `specific`. `specific` is the stable startup mode for reproducible restore. |
| startup.specific-offset.vgtid | String | No | - | Vitess VGTID used when `startup.mode = specific`. |
| tablet-type | Enum | No | MASTER | VTGate tablet type used by VStream. Supported values are `MASTER`, `REPLICA`, `RDONLY`. |
| shard | String | No | - | Optional shard restriction. Omit it to capture all shards in the keyspace. |
| stop-on-reshard | Boolean | No | false | Whether the connector should stop after resharding. |
| keepalive.interval.ms | Long | No | Long.MAX_VALUE | gRPC keepalive interval in milliseconds. |
| grpc.headers | String | No | - | Optional comma-separated gRPC headers in `key:value` format. |
| grpc.max-inbound-message-size | Int | No | 4194304 | Maximum inbound gRPC message size in bytes. |
| server-time-zone | String | No | UTC | Time zone used by SeaTunnel row deserialization. |
| format | Enum | No | DEFAULT | Optional output format. Supported values are `DEFAULT` and `COMPATIBLE_DEBEZIUM_JSON`. |
| debezium | Config | No | - | Pass-through Debezium properties for the Vitess connector backend. |

\* Configure exactly one of `schema` and `tables_configs`.

\** Configure at most one of `table-names` and `table-pattern`. When both are omitted, the connector captures every declared schema table.

## Notes

- The first delivery does not read an initial table snapshot.
- Dynamic discovery of newly added tables is out of scope.
- Schema evolution events are not emitted in this first delivery.
- Restore uses the checkpointed SeaTunnel table schema snapshot instead of re-parsing the latest config shape.
- A focused integration path is provided by `TestVitessSourceReaderIT`, which runs against
  `vitess/vttestserver`.

## Task Example

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Vitess-CDC {
    plugin_output = "vitess_cdc"
    hostname = "127.0.0.1"
    port = 15992
    keyspace = "test"
    tables_configs = [
      {
        schema = {
          table = "test.products"
          columns = [
            { name = "id", type = "int" }
            { name = "name", type = "string" }
            { name = "description", type = "string" }
            { name = "weight", type = "float" }
          ]
          primaryKey = {
            name = "pk_products"
            columnNames = ["id"]
          }
        }
      },
      {
        schema = {
          table = "test.customers"
          columns = [
            { name = "id", type = "int" }
            { name = "name", type = "string" }
          ]
          primaryKey = {
            name = "pk_customers"
            columnNames = ["id"]
          }
        }
      }
    ]
    table-names = ["test.products", "test.customers"]
    startup.mode = "specific"
    startup.specific-offset.vgtid = "[{\"keyspace\":\"test\",\"shard\":\"-\",\"gtid\":\"MySQL56/uuid:1-200\"}]"
    server-time-zone = "UTC"
  }
}

transform {
}

sink {
  Console {}
}
```

## Changelog

<ChangeLog />
