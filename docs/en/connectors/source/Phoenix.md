import ChangeLog from '../changelog/connector-jdbc.md';

# Phoenix

> Phoenix source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Read data from Apache Phoenix through the [Jdbc connector](Jdbc.md). The connector identifier used
in the job configuration is `Jdbc`. Tested Phoenix versions are 4.x and 5.x.

Under the hood, the connector uses Phoenix's JDBC driver to execute the query and read rows from
HBase. Supports column projection through the standard `SELECT ...` syntax.

There are two ways to connect Phoenix through Java JDBC:

- Connect to the ZooKeeper quorum with the **thick** driver.
- Connect to the Phoenix Query Server with the **thin** driver.

> **Tip:** The (thin) driver jar is used by default. If you want to use the (thick) driver or another
> version of the Phoenix (thin) driver, you need to recompile the `connector-jdbc` module.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

Supports standard SQL queries and column projection.

## Options

| Name           | Type   | Required | Default Value | Description                                                                                              |
|----------------|--------|----------|---------------|----------------------------------------------------------------------------------------------------------|
| driver         | String | Yes      | -             | JDBC driver class. Use `org.apache.phoenix.jdbc.PhoenixDriver` for the thick driver, or `org.apache.phoenix.queryserver.client.Driver` for the thin driver. |
| url            | String | Yes      | -             | JDBC connection URL. Use `jdbc:phoenix:localhost:2182/hbase` for the thick driver, or `jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF` for the thin driver. |
| query          | String | Yes      | -             | SELECT query executed to read rows, for example `select age, name from test.source`. The column order in the SELECT list must match `schema.fields`. |
| common-options |        | No       | -             | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details. |

### driver [string]

JDBC driver class. Use `org.apache.phoenix.jdbc.PhoenixDriver` for the thick driver, or
`org.apache.phoenix.queryserver.client.Driver` for the thin driver.

### url [string]

JDBC connection URL. Use `jdbc:phoenix:localhost:2182/hbase` for the thick driver, or
`jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF` for the thin driver.

### query [string]

SELECT query executed to read rows from Phoenix. Use the table name with a fully-qualified schema
(for example `test.source`), not the bare table name. Column projection is supported by listing
only the columns you need in the SELECT clause.

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

## Task Example

### Use the thick client driver

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = org.apache.phoenix.jdbc.PhoenixDriver
    url = "jdbc:phoenix:localhost:2182/hbase"
    query = "select age, name from test.source"
  }
}

sink {
  Console {}
}
```

### Use the thin client driver

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = org.apache.phoenix.queryserver.client.Driver
    url = "jdbc:phoenix:thin:url=http://spark_e2e_phoenix_sink:8765;serialization=PROTOBUF"
    query = "select age, name from test.source"
  }
}

sink {
  Console {}
}
```

### Project Specific Columns With A Predicate

Combine column projection with a `WHERE` clause to narrow the rows that flow downstream. This
example only fetches rows whose `name` starts with `A`:

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = org.apache.phoenix.jdbc.PhoenixDriver
    url = "jdbc:phoenix:localhost:2182/hbase"
    query = "select name, score from test.source where name like 'A%'"
  }
}

sink {
  Console {}
}
```

## Changelog

<ChangeLog />