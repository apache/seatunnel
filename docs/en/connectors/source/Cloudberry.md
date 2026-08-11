import ChangeLog from '../changelog/connector-cloudberry.md';

# Cloudberry

> JDBC Cloudberry Source Connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/org.postgresql/postgresql) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/org.postgresql/postgresql) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

> supports query SQL and can achieve projection effect.

## Description

Read external data source data through JDBC. Cloudberry currently does not have its own native JDBC driver, using PostgreSQL's drivers and implementation.

## Supported DataSource Info

| Datasource |            Supported Versions            |        Driver         |                  Url                  |                                  Maven                                   |
|------------|------------------------------------------|------------------------|---------------------------------------|--------------------------------------------------------------------------|
| Cloudberry | Uses PostgreSQL driver implementation | org.postgresql.Driver | jdbc:postgresql://localhost:5432/test | [Download](https://mvnrepository.com/artifact/org.postgresql/postgresql) |

## Database Dependency

> Please download the PostgreSQL driver jar and copy it to the '$SEATUNNEL_HOME/plugins/jdbc/lib/' working directory<br/>
> For example: cp postgresql-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## Data Type Mapping

Cloudberry uses PostgreSQL's data type implementation. Please refer to PostgreSQL documentation for data type compatibility and mappings.

## Options

Cloudberry is a thin wrapper over the PostgreSQL dialect inside the JDBC source. The job still uses the `Jdbc`
plugin name; only the connection settings differ. Every option below is identical to the PostgreSQL/JDBC source
option of the same name — please refer to the [PostgreSQL source documentation](../source/PostgreSQL.md) and the
shared [JDBC source options](../source/Jdbc.md) for the full description, valid values, and default behavior.

Key options include:

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| url | String | Yes | - | JDBC connection URL. Use a `jdbc:postgresql://host:port/database` style URL because Cloudberry speaks the PostgreSQL wire protocol. |
| driver | String | Yes | - | JDBC driver class name. Always `org.postgresql.Driver`. |
| user / username | String | Yes | - | Database login. `user` is also accepted as a fallback key for `username`. |
| password | String | Yes | - | Database password. Use a secrets manager or environment variable in production; never commit it into the job config. |
| query | String | Conditional | - | SELECT statement that drives the read. Required when not using `table_path` or `table_list`. |
| table_path | String | Conditional | - | `schema.table` of the table to read. Required when not using `query`. |
| table_list | List | No | - | Read several tables in one source. Each entry is `{ table_path = "schema.table" }`. Parallelism is split across tables when this option is set. |
| split.size | Int | No | 8096 | Target row count per split. The connector uses this to derive a parallel scan plan when `partition_column` is provided. |
| split.even_partition_num | Boolean | No | false | Force a balanced number of partitions; otherwise the connector lets the optimizer decide. |
| split.sample_shard_threshold | Int | No | 1000 | Threshold (rows) for sampling when computing the partition count. |
| split.inverse_parallelism | Int | No | 1 | Inverse of the parallelism — i.e. the number of splits per source subtask. Higher values mean smaller, more numerous splits. |
| partition_column | String | No | - | Numeric column used for partition splits. Must be a monotonically increasing or unique column (e.g. `id`, `created_at`). |
| partition_upper_bound | Long | No | - | Upper bound of the partition column. Set when you know the range to avoid a costly `MIN()` probe. |
| partition_lower_bound | Long | No | - | Lower bound of the partition column. Set when you know the range to avoid a costly `MAX()` probe. |
| fetch_size | Int | No | 0 | JDBC fetch size. `0` means driver default. Increase for large rows. |
| common-options | Config | No | - | Source common options. See [Source Common Options](../common-options/source-common-options.md). |

## Parallel Reader

Cloudberry inherits the PostgreSQL parallel reading strategy from the JDBC source:

- **No partition column** — the connector runs the configured `query` on a single subtask. Use this when the
  query already aggregates or the table is small.
- **Numeric `partition_column`** — the connector probes `MIN()`/`MAX()` (or uses `partition_lower_bound` /
  `partition_upper_bound` if set), then issues each split as a `WHERE partition_column BETWEEN ? AND ?` clause.
  This is the typical pattern for large fact tables.
- **`table_list`** — the connector assigns one table per subtask when parallelism ≥ number of tables, otherwise
  it splits each table as described above.

For the full split-strategy reference and edge-case guidance (e.g. non-numeric partition columns, parallel
`UPDATE` snapshots), see the [PostgreSQL source documentation](../source/PostgreSQL.md).

## Notes

- Use the `Jdbc` plugin name for Cloudberry jobs.
- Cloudberry uses the PostgreSQL JDBC driver and PostgreSQL-compatible dialect path, so keep `driver = "org.postgresql.Driver"`.
- Use a PostgreSQL-style URL such as `jdbc:postgresql://host:5432/database`.
- Keep database passwords out of shared examples, logs, and screenshots.
- Cloudberry does not yet ship CDC support. For change-data capture, use the dedicated CDC source for the
  upstream database, or capture changes with a logical replication slot and ingest via the JDBC source.
- For very large parallel scans, prefer setting `partition_lower_bound` / `partition_upper_bound` over letting
  the connector probe `MIN()` / `MAX()`. The probe query is the slowest part of job startup.

## Task Example

### Simple

```hocon
env {
  parallelism = 4
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    user = "dbadmin"
    password = "password"
    query = "select * from mytable limit 100"
  }
}

sink {
  Console {}
}
```

### Parallel reading with table_path

```hocon
env {
  parallelism = 4
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    user = "dbadmin"
    password = "password"
    table_path = "public.mytable"
    partition_column = "id"
    partition_lower_bound = 1
    partition_upper_bound = 1000000
    split.size = 10000
  }
}

sink {
  Console {}
}
```

### Multiple table read

```hocon
env {
  job.mode = "BATCH"
  parallelism = 4
}

source {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    user = "dbadmin"
    password = "password"
    "table_list" = [
      {
        "table_path" = "public.table1"
      },
      {
        "table_path" = "public.table2"
      }
    ]
    split.size = 10000
  }
}

sink {
  Console {}
}
```

### Streaming source from a query (single-subtask)

For continuous ingestion (e.g. polling a `WHERE updated_at > now() - interval '5 minutes'` query),
drop `parallelism` to 1 and rely on the database-side filter. The query needs to be idempotent and
must include a deterministic ordering.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    user = "dbadmin"
    password = "password"
    query = "select id, payload, updated_at from events where updated_at > now() - interval '5 minutes' order by updated_at"
  }
}

sink {
  Console {}
}
```

For more detailed examples and configurations, please refer to the [PostgreSQL source documentation](../source/PostgreSQL.md) and the shared [JDBC source options](../source/Jdbc.md).

## Changelog

<ChangeLog />
