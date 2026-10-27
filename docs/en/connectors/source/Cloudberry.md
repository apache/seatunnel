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
| table_list | List | No | - | Read several tables in one source. Each entry includes a `table_path`; generated splits are assigned to readers. |
| split.size | Int | No | 8096 | Target row count per split. Used by the dynamic split planner. |
| split.even-distribution.factor.lower-bound | Double | No | 0.05 | Lower bound of the distribution factor used to choose evenly calculated splits. |
| split.even-distribution.factor.upper-bound | Double | No | 100.0 | Upper bound of the distribution factor used to choose evenly calculated splits. |
| split.sample-sharding.threshold | Int | No | 1000 | Estimated shard-count threshold for sampling when the split-key distribution is uneven. |
| split.inverse-sampling.rate | Int | No | 1000 | Sampling-rate denominator; `1000` means a sampling rate of `1/1000`. |
| partition_column | String | No | - | Column used for legacy partition splits. See JDBC source documentation for supported types and split-key selection. |
| partition_upper_bound | Long | No | - | Upper bound of the partition column. Set when you know the range to avoid a costly `MAX()` probe. |
| partition_lower_bound | Long | No | - | Lower bound of the partition column. Set when you know the range to avoid a costly `MIN()` probe. |
| fetch_size | Int | No | 0 | JDBC fetch size. `0` means driver default. Increase for large rows. |
| common-options | Config | No | - | Source common options. See [Source Common Options](../common-options/source-common-options.md). |

## Parallel Reader

Cloudberry uses the JDBC source split planner. The selected split key, query/table configuration,
and `enable_concurrent_read` determine whether a table can be split. The enumerator assigns generated
splits to readers by split ID; `table_list` does not guarantee one table per reader.

For the available split strategies and configuration requirements, see the
[JDBC source documentation](../source/Jdbc.md#parallel-reader).

## Notes

- Use the `Jdbc` plugin name for Cloudberry jobs.
- Cloudberry uses the PostgreSQL JDBC driver and PostgreSQL-compatible dialect path, so keep `driver = "org.postgresql.Driver"`.
- Use a PostgreSQL-style URL such as `jdbc:postgresql://host:5432/database`.
- Keep database passwords out of shared examples, logs, and screenshots.
- The JDBC source performs bounded reads; it does not poll queries or consume logical replication slots.
  For continuous change capture, use a CDC connector that explicitly supports the upstream database.
- For very large parallel scans, prefer setting `partition_lower_bound` / `partition_upper_bound` over letting
  the connector probe `MIN()` / `MAX()`. Probe cost depends on the table and its indexes.

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

### Bounded extraction with a query filter

The filter restricts this batch's result set. It is evaluated during one bounded read; SeaTunnel does
not repeat the query or persist an incremental watermark for later runs.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
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
