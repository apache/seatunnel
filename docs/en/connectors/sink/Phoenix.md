import ChangeLog from '../changelog/connector-jdbc.md';

# Phoenix

> JDBC Phoenix sink connector

## Description

Write data to Phoenix through the [JDBC connector](Jdbc.md). Phoenix writes are normally expressed as `UPSERT` statements and are applied to the underlying HBase table through the Phoenix JDBC driver.

Phoenix can be accessed with either the thick JDBC driver that connects through ZooKeeper or the thin JDBC driver that connects through Phoenix Query Server.

> By default, the connector uses the Phoenix thin driver bundled with the JDBC connector module. If you need the thick driver or another Phoenix thin-driver version, rebuild the JDBC connector module with that driver.
>
> Phoenix Sink does not support exactly-once semantics because Phoenix does not provide XA transaction support for this connector path.

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Options

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| driver | String | Yes | - | Phoenix JDBC driver class. Use `org.apache.phoenix.jdbc.PhoenixDriver` for the thick driver or `org.apache.phoenix.queryserver.client.Driver` for the thin driver. |
| url | String | Yes | - | Phoenix JDBC URL. Thick-driver example: `jdbc:phoenix:localhost:2182/hbase`. Thin-driver example: `jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF`. |
| query | String | Yes | - | SQL used to write rows. For Phoenix, use an `UPSERT INTO ... VALUES (?, ?)` statement or named parameters supported by JDBC Sink. |
| batch_size | Int | No | 1000 | Flush buffered rows when the buffered row count reaches this value. |
| batch_interval_ms | Long | No | 1000 | Flush buffered rows when this interval is reached, even if `batch_size` has not been reached. |
| common-options | | No | - | Sink plugin common parameters. See [Sink Common Options](../common-options/sink-common-options.md). |

Because Phoenix Sink is implemented by the shared JDBC Sink, advanced JDBC sink options such as `max_retries`, `properties`, `field_ide`, and `auto_commit` follow the same rules as [JDBC Sink](Jdbc.md). Do not enable `is_exactly_once` for Phoenix because the Phoenix JDBC path does not support XA transactions.

## Example

### Thick Driver

```hocon
sink {
  Jdbc {
    driver = org.apache.phoenix.jdbc.PhoenixDriver
    url = "jdbc:phoenix:localhost:2182/hbase"
    query = "upsert into test.SINK(age, name) values(?, ?)"
  }
}
```

### Thin Driver

```hocon
sink {
  Jdbc {
    driver = org.apache.phoenix.queryserver.client.Driver
    url = "jdbc:phoenix:thin:url=http://seatunnel_e2e_phoenix:8765;serialization=PROTOBUF"
    query = "upsert into test.SINK(age, name) values(?, ?)"
    batch_size = 1000
    batch_interval_ms = 2000
  }
}
```

## Changelog

<ChangeLog />
