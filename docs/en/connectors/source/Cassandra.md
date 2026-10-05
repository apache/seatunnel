import ChangeLog from '../changelog/connector-cassandra.md';

# Cassandra

> Cassandra source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Read data from Apache Cassandra in batch mode.

The Cassandra source supports two read modes:

- Single-table read with `cql`.
- Multi-table read with `tables_configs`, where each entry contains one `cql`.

The source gets column names and data types from the result set returned by the configured CQL, so
the CQL should return the columns that downstream steps need.

## Supported DataSource Info

| Datasource | Supported Versions | Dependency |
|------------|--------------------|------------|
| Cassandra  | Universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-cassandra) |

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Data Type Mapping

| Cassandra Data Type | SeaTunnel Data Type |
|---------------------|---------------------|
| ascii               | STRING              |
| varchar/text        | STRING              |
| varint              | STRING              |
| uuid/timeuuid       | STRING              |
| inet                | STRING              |
| tinyint             | BYTE                |
| smallint            | SHORT               |
| int                 | INT                 |
| bigint/counter      | LONG                |
| float               | FLOAT               |
| double/decimal      | DOUBLE              |
| boolean             | BOOLEAN             |
| time                | TIME                |
| date                | DATE                |
| timestamp           | TIMESTAMP           |
| blob                | ARRAY\<BYTE\>       |
| list                | ARRAY               |
| set                 | ARRAY               |
| map                 | MAP                 |

## Source Options

| Name              | Type       | Required | Default     | Description |
|-------------------|------------|----------|-------------|-------------|
| host              | String     | Yes      | -           | Cassandra cluster address. Use `host:port`, and separate multiple hosts with commas. |
| keyspace          | String     | Yes      | -           | Cassandra keyspace used by the session. |
| cql               | String     | No *     | -           | CQL used to read one table. |
| tables_configs    | List\<Map\> | No *     | -           | Multi-table read configuration. Each item must contain one `cql`. |
| username          | String     | No       | -           | Cassandra username. Configure it together with `password`. |
| password          | String     | No       | -           | Cassandra password. Configure it together with `username`. |
| datacenter        | String     | No       | datacenter1 | Local datacenter name used by the Cassandra Java driver. |
| consistency_level | String     | No       | LOCAL_ONE   | Read consistency level, such as `LOCAL_ONE`, `ONE`, `QUORUM`, or `LOCAL_QUORUM`. |
| common-options    |            | No       | -           | Source plugin common parameters, such as `plugin_output`. |

> \* Exactly one of `cql` or `tables_configs` must be provided.

### host [string]

`Cassandra` cluster address, the format is `host:port` , allowing multiple `hosts` to be specified. Such as
`"cassandra1:9042,cassandra2:9042"`.

### keyspace [string]

The `Cassandra` keyspace.

### cql [String]

The query CQL used to read data from Cassandra. Use this for single-table reads. It is mutually
exclusive with `tables_configs`.

The connector uses the CQL result metadata to build the output schema. In normal cases, use a query
that returns real table columns, for example `select * from source_table` or
`select id, name from source_table`.

### tables_configs [List\<Map\>]

Multi-table read configuration. Each entry must contain a `cql` field with the query for that table.
It is mutually exclusive with root-level `cql`.

Do not configure the same source table more than once in `tables_configs`; the connector checks for
duplicate table names during startup.

Example entry:

```hocon
{
  cql = "SELECT id, name FROM keyspace.table1"
}
```

### username [string]

`Cassandra` user username.

### password [string]

`Cassandra` user password.

### datacenter [String]

The `Cassandra` datacenter, default is `datacenter1`.

### consistency_level [String]

The `Cassandra` read consistency level, default is `LOCAL_ONE`.

### common-options

Source plugin common parameters. For details, see [Source Common Options](../common-options/source-common-options.md).

## Notes

- `username` and `password` are a pair. Configure both when authentication is enabled; omit both
  when the cluster does not require authentication.
- `datacenter` must match the Cassandra cluster local datacenter name. The default is
  `datacenter1`, which is also the common Testcontainers default.
- `cql` and `tables_configs` are mutually exclusive. Use `cql` for one result table and
  `tables_configs` when one source should read multiple Cassandra tables.
- The source is a batch source. It reads the current query result and then finishes.
- A single CQL query is read as one source split. Increasing job parallelism does not split one
  Cassandra table scan automatically.
- The connector uses the Cassandra Java driver. The connection options documented above are the
  only settings the connector reads; any other DataStax driver option uses its built-in default.

## Task Example

### Single-table read

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Cassandra {
    host = "localhost:9042"
    username = "cassandra"
    password = "cassandra"
    datacenter = "datacenter1"
    keyspace = "test"
    cql = "SELECT * FROM test.source_table"
    plugin_output = "source_table"
  }
}

sink {
  Console {}
}
```

### Multi-table read

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Cassandra {
    host = "localhost:9042"
    username = "cassandra"
    password = "cassandra"
    datacenter = "datacenter1"
    keyspace = "test"
    tables_configs = [
      {
        cql = "select id, c_int from mt_source_a"
      },
      {
        cql = "select id, c_int from mt_source_b"
      }
    ]
  }
}

sink {
  Cassandra {
    host = "localhost:9042"
    username = "cassandra"
    password = "cassandra"
    datacenter = "datacenter1"
    keyspace = "test"
    table = "mt_sink_table"
  }
}
```

### Read With A Stricter Consistency Level

Use `consistency_level = "QUORUM"` when the read result must satisfy the configured replication
factor. Combine it with `datacenter` so the driver talks to the right local coordinator:

```hocon
source {
  Cassandra {
    host = "cassandra1:9042,cassandra2:9042"
    username = "cassandra"
    password = "cassandra"
    datacenter = "datacenter1"
    keyspace = "test"
    consistency_level = "QUORUM"
    cql = "SELECT id, name, score FROM test.accounts"
  }
}
```

## Changelog

<ChangeLog />
