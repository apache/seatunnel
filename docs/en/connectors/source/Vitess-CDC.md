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
- explicitly configured tables or table patterns only
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
| table-names | List | Yes* | - | Explicit tables to capture. Table names must be database-qualified, for example `commerce.orders`. |
| table-pattern | String | Yes* | - | Regular expression for database-qualified table names. |
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

\* Configure exactly one of `table-names` and `table-pattern`.

## Notes

- The first delivery does not read an initial table snapshot.
- Dynamic discovery of newly added tables is out of scope.
- Schema evolution events are not emitted in this first delivery.
- A focused integration path is provided by `VitessSourceReaderIT`, which runs against
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
    table-names = ["test.products", "test.customers"]
    startup.mode = "specific"
    startup.specific-offset.vgtid = "{\"shard_gtids\":[{\"keyspace\":\"test\",\"gtid\":\"MySQL56/uuid:1-200\"}]}"
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
