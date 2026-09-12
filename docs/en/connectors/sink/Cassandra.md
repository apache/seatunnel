import ChangeLog from '../changelog/connector-cassandra.md';

# Cassandra

> Cassandra sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Write data to Apache Cassandra in batch mode.

The sink writes rows to an existing Cassandra table. If `fields` is not configured, the connector
uses all columns from the target Cassandra table schema. If `fields` is configured, only those
columns are written, and every configured field must exist in the target table.

The connector does not create keyspaces, tables, or missing columns. Prepare the target Cassandra
schema before starting the job.

## Supported DataSource Info

| Datasource | Supported Versions | Dependency |
|------------|--------------------|------------|
| Cassandra  | Universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-cassandra) |

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)

## Sink Options

| Name              | Type    | Required | Default     | Description |
|-------------------|---------|----------|-------------|-------------|
| host              | String  | Yes      | -           | Cassandra cluster address. Use `host:port`, and separate multiple hosts with commas. |
| keyspace          | String  | Yes      | -           | Cassandra keyspace used by the session. |
| table             | String  | Yes      | -           | Target Cassandra table name. |
| username          | String  | No       | -           | Cassandra username. Configure it together with `password`. |
| password          | String  | No       | -           | Cassandra password. Configure it together with `username`. |
| datacenter        | String  | No       | datacenter1 | Local datacenter name used by the Cassandra Java driver. |
| consistency_level | String  | No       | LOCAL_ONE   | Write consistency level, such as `LOCAL_ONE`, `ONE`, `QUORUM`, or `LOCAL_QUORUM`. |
| fields            | Array   | No       | -           | Target columns to write. If not set, all target table columns are used. |
| batch_size        | int     | No       | 5000        | Maximum number of rows buffered before one flush. |
| batch_type        | String  | No       | UNLOGGED    | Cassandra batch type. Supported driver values include `LOGGED`, `UNLOGGED`, and `COUNTER`. |
| async_write       | boolean | No       | true        | Whether to execute writes asynchronously. |
| common-options    |         | No       | -           | Sink plugin common parameters, such as `plugin_input`. |

### host [string]

`Cassandra` cluster address, the format is `host:port` , allowing multiple `hosts` to be specified. Such as
`"cassandra1:9042,cassandra2:9042"`.

### keyspace [string]

The `Cassandra` keyspace.

### table [String]

The `Cassandra` table name.

### username [string]

`Cassandra` user username.

### password [string]

`Cassandra` user password.

### datacenter [String]

The `Cassandra` datacenter, default is `datacenter1`.

### consistency_level [String]

The `Cassandra` write consistency level, default is `LOCAL_ONE`.

### fields [array]

The data fields that need to be written to `Cassandra`. If this option is not configured, the
connector reads the target table schema and writes all columns from that table.

When this option is configured, the field names must exist in the target Cassandra table and must
also exist in the upstream SeaTunnel row.

Use this option when the upstream row has extra fields that should not be written to Cassandra.

### batch_size [number]

The number of rows written through [Cassandra-Java-Driver](https://github.com/datastax/java-driver) each time,
default is `5000`.

### batch_type [String]

The `Cassandra` batch processing mode, default is `UNLOGGED`.

### async_write [boolean]

Whether `cassandra` writes in asynchronous mode, default is `true`.

### common-options

Sink plugin common parameters. For details, see [Sink Common Options](../common-options/sink-common-options.md).

## Notes

- The target keyspace and table must already exist before the job starts.
- `fields` is useful when the upstream row has extra columns. It is not a schema creation option.
- `async_write = true` improves throughput, while `batch_size` controls how many rows are grouped
  before a flush.
- The sink uses the Cassandra Java driver. Authentication and consistency options match the
  driver's terminology; configure `consistency_level` together with the cluster's replication
  strategy when stronger guarantees are required.
- `batch_type = "UNLOGGED"` is the typical default. Use `LOGGED` for atomic batches when
  correctness outweighs throughput, or `COUNTER` for counter tables.

## Task Example

### Write to Cassandra

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
    cql = "select * from source_table"
    plugin_output = "source_table"
  }
}

sink {
  Cassandra {
    host = "localhost:9042"
    username = "cassandra"
    password = "cassandra"
    datacenter = "datacenter1"
    keyspace = "test"
    table = "sink_table"
    async_write = true
  }
}
```

### Write Selected Fields

```hocon
sink {
  Cassandra {
    host = "localhost:9042"
    username = "cassandra"
    password = "cassandra"
    datacenter = "datacenter1"
    keyspace = "test"
    table = "sink_table"
    fields = ["id", "c_int", "c_text"]
    batch_size = 1000
    batch_type = "UNLOGGED"
    async_write = true
  }
}
```

### Stream MySQL CDC Events Into Cassandra

Pipe MySQL CDC events through a Cassandra sink by mapping the CDC row kinds to the table
columns. Cassandra uses `INSERT` semantics per row, so a CDC `DELETE` is expressed by writing
a tombstone column (`is_deleted`) and filtering it out downstream if needed:

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  MySQL-CDC {
    base-url = "jdbc:mysql://mysql:3306/test"
    username = "root"
    password = "mysqlpw"
    table-names = ["test.orders"]
  }
}

sink {
  Cassandra {
    host = "cassandra1:9042,cassandra2:9042"
    keyspace = "test"
    table = "orders"
    fields = ["id", "order_id", "customer", "amount", "is_deleted"]
    consistency_level = "LOCAL_QUORUM"
    batch_size = 2000
    batch_type = "UNLOGGED"
    async_write = true
  }
}
```

> **Note:** The `is_deleted` column shown above is not produced by MySQL-CDC and is not
> derived from `RowKind` by the Cassandra sink. You must populate it yourself — either by
> carrying an `is_deleted` column in the upstream MySQL table, or by adding a Transform-V2
> (for example `sql` / `replace`) between the source and the sink that synthesizes it from
> the CDC `RowKind`. Without that, `DELETE` events will be written back to Cassandra as a
> regular upsert.

## Changelog

<ChangeLog />
