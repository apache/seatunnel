import ChangeLog from '../changelog/connector-jdbc.md';

# Phoenix

> JDBC Phoenix source connector

## Description

Read Phoenix data through the [JDBC connector](Jdbc.md). Phoenix can be accessed with either the thick JDBC driver that connects through ZooKeeper or the thin JDBC driver that connects through Phoenix Query Server.

The connector supports batch jobs. Streaming jobs can use Phoenix as a bounded JDBC source, but Phoenix Source does not continuously capture new changes.

> By default, the connector uses the Phoenix thin driver bundled with the JDBC connector module. If you need the thick driver or another Phoenix thin-driver version, rebuild the JDBC connector module with that driver.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)

supports query SQL and can achieve projection effect.

- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Options

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| driver | String | Yes | - | Phoenix JDBC driver class. Use `org.apache.phoenix.jdbc.PhoenixDriver` for the thick driver or `org.apache.phoenix.queryserver.client.Driver` for the thin driver. |
| url | String | Yes | - | Phoenix JDBC URL. Thick-driver example: `jdbc:phoenix:localhost:2182/hbase`. Thin-driver example: `jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF`. |
| query | String | Yes | - | SQL used to read data from Phoenix. Use this option to select the columns and rows that should be read. |
| common-options | | No | - | Source plugin common parameters. See [Source Common Options](../common-options/source-common-options.md). |

Because Phoenix Source is implemented by the shared JDBC Source, advanced JDBC source options such as `fetch_size`, `partition_column`, `partition_num`, `properties`, and `table_list` follow the same rules as [JDBC Source](Jdbc.md).

## Example

### Thick Driver

```hocon
source {
  Jdbc {
    driver = org.apache.phoenix.jdbc.PhoenixDriver
    url = "jdbc:phoenix:localhost:2182/hbase"
    query = "select age, name from test.SOURCE"
  }
}
```

### Thin Driver

```hocon
source {
  Jdbc {
    driver = org.apache.phoenix.queryserver.client.Driver
    url = "jdbc:phoenix:thin:url=http://seatunnel_e2e_phoenix:8765;serialization=PROTOBUF"
    query = "select age, name from test.SOURCE"
  }
}
```

## Changelog

<ChangeLog />
