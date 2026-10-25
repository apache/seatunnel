import ChangeLog from '../changelog/connector-jdbc.md';

# Phoenix

> Phoenix sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Write data to Apache Phoenix through the [Jdbc connector](Jdbc.md). The connector identifier used in
the job configuration is `Jdbc`. Tested Phoenix versions are 4.x and 5.x.

Under the hood, the connector uses Phoenix's JDBC driver to execute an `upsert` statement that
writes each row to HBase.

There are two ways to connect Phoenix through Java JDBC:

- Connect to the ZooKeeper quorum with the **thick** driver.
- Connect to the Phoenix Query Server with the **thin** driver.

> **Tip 1:** The (thin) driver jar is used by default. If you want to use the (thick) driver or
> another version of the Phoenix (thin) driver, you need to recompile the `connector-jdbc` module.
>
> **Tip 2:** This connector does not support exactly-once semantics, because Phoenix does not yet
> support XA transactions.

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)

## Options

| Name           | Type   | Required | Default Value | Description                                                                                              |
|----------------|--------|----------|---------------|----------------------------------------------------------------------------------------------------------|
| driver         | String | Yes      | -             | JDBC driver class. Use `org.apache.phoenix.jdbc.PhoenixDriver` for the thick driver, or `org.apache.phoenix.queryserver.client.Driver` for the thin driver. |
| url            | String | Yes      | -             | JDBC connection URL. Use `jdbc:phoenix:localhost:2182/hbase` for the thick driver, or `jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF` for the thin driver. |
| query          | String | Yes      | -             | Phoenix upsert statement executed for every write, for example `upsert into test.sink(age, name) values(?, ?)`. `?` placeholders are bound positionally from the upstream row. |
| common-options |        | No       | -             | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details. |

### driver [string]

JDBC driver class. Use `org.apache.phoenix.jdbc.PhoenixDriver` for the thick driver, or
`org.apache.phoenix.queryserver.client.Driver` for the thin driver.

### url [string]

JDBC connection URL. Use `jdbc:phoenix:localhost:2182/hbase` for the thick driver, or
`jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF` for the thin driver.

### query [string]

Phoenix upsert statement executed for every write. The `?` placeholders are bound positionally from
the upstream row, so the column order in the upsert must match the field order declared by the
upstream schema. Use the table name with a fully-qualified schema (for example
`test.sink`), not the bare table name.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

## Task Example

### Use the thick client driver

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 2
    schema = {
      fields {
        age = int
        name = string
      }
    }
    rows = [
      { kind = INSERT, fields = [10, "jared"] }
      { kind = INSERT, fields = [20, "huan"] }
    ]
  }
}

sink {
  Jdbc {
    driver = org.apache.phoenix.jdbc.PhoenixDriver
    url = "jdbc:phoenix:localhost:2182/hbase"
    query = "upsert into test.sink(age, name) values(?, ?)"
  }
}
```

### Use the thin client driver

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 2
    schema = {
      fields {
        age = int
        name = string
      }
    }
    rows = [
      { kind = INSERT, fields = [10, "jared"] }
      { kind = INSERT, fields = [20, "huan"] }
    ]
  }
}

sink {
  Jdbc {
    driver = org.apache.phoenix.queryserver.client.Driver
    url = "jdbc:phoenix:thin:url=http://spark_e2e_phoenix_sink:8765;serialization=PROTOBUF"
    query = "upsert into test.sink(age, name) values(?, ?)"
  }
}
```

## Changelog

<ChangeLog />