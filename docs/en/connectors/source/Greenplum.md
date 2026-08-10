import ChangeLog from '../changelog/connector-jdbc.md';

# Greenplum

> Greenplum source connector

## Description

Read Greenplum data through the [Jdbc connector](Jdbc.md). Greenplum uses the PostgreSQL protocol, so you can usually use the PostgreSQL JDBC driver. If you use the Greenplum native JDBC driver, provide the driver jar yourself.

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Using Dependency

### For Spark/Flink Engine

> 1. When using `org.postgresql.Driver`, ensure the [PostgreSQL JDBC driver](https://mvnrepository.com/artifact/org.postgresql/postgresql) has been placed in `${SEATUNNEL_HOME}/plugins/`.
> 2. When using `com.pivotal.jdbc.GreenplumDriver`, download the Greenplum JDBC driver by yourself and place it in `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. When using `org.postgresql.Driver`, ensure the [PostgreSQL JDBC driver](https://mvnrepository.com/artifact/org.postgresql/postgresql) has been placed in `${SEATUNNEL_HOME}/lib/`.
> 2. When using `com.pivotal.jdbc.GreenplumDriver`, download the Greenplum JDBC driver by yourself and place it in `${SEATUNNEL_HOME}/lib/`.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

> Supports query SQL and can achieve column projection.

## Supported DataSource Info

| Datasource | Driver | Url | Maven |
|------------|--------|-----|-------|
| Greenplum by PostgreSQL driver | `org.postgresql.Driver` | `jdbc:postgresql://localhost:5432/testdb` | [Download](https://mvnrepository.com/artifact/org.postgresql/postgresql) |
| Greenplum native driver | `com.pivotal.jdbc.GreenplumDriver` | `jdbc:pivotal:greenplum://localhost:5432;DatabaseName=testdb` | Download from Greenplum |

:::tip

For license compliance, SeaTunnel does not ship the Greenplum native JDBC driver. If you use `com.pivotal.jdbc.GreenplumDriver`, copy `greenplum-xxx.jar` to the engine dependency directory before running the job.

:::

## Options

Only Greenplum-specific commonly used options are listed here. Other JDBC source options, such as `fetch_size`, `connection_check_timeout_sec`, `properties`, `table_path`, and multi-table reading, are inherited from [Jdbc Source](Jdbc.md).

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| url | String | Yes | - | JDBC connection URL. Use `jdbc:postgresql://host:port/database` with the PostgreSQL driver, or `jdbc:pivotal:greenplum://host:port;DatabaseName=database` with the Greenplum native driver. |
| driver | String | Yes | - | JDBC driver class name, usually `org.postgresql.Driver` or `com.pivotal.jdbc.GreenplumDriver`. |
| username | String | No | - | Greenplum username. |
| password | String | No | - | Greenplum password. |
| query | String | Yes | - | SQL used to read data. Select only the columns you need; the source uses the SELECT column list as the output schema. |
| partition_column | String | No | - | Column used to split data for parallel reading. Can be numeric or string. Numeric columns are split into `partition_num` numeric ranges; string columns are split by hash (`split.string_split_mode = sample`, the default) or by lexicographic range (`split.string_split_mode = charset_based`). |
| partition_lower_bound | String | No | - | Lower bound of `partition_column`. If not set, SeaTunnel queries the minimum value. |
| partition_upper_bound | String | No | - | Upper bound of `partition_column`. If not set, SeaTunnel queries the maximum value. |
| partition_num | Int | No | job parallelism | Number of source splits. Each split issues a range query for numeric (and `charset_based` string) columns, or a hash-modulo predicate for `sample` string columns. |
| split.string_split_mode | String | No | sample | String split algorithm. `sample` estimates splits from a sampled value list; `charset_based` performs deterministic range-like splitting and is preferred when the split column contains printable ASCII strings. |
| common-options | | No | - | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details. |

### Tips

> If `partition_column` is not set, the source reads with one split. If it is set, SeaTunnel reads data in parallel according to `partition_num` or job parallelism.

## Task Example

### Read Greenplum Data

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = "org.postgresql.Driver"
    url = "jdbc:postgresql://localhost:5432/testdb"
    username = "tester"
    password = "pivotal"
    query = "select age, name from source"
  }
}

sink {
  Console {}
}
```

### Parallel Read By String Column

```hocon
source {
  Jdbc {
    driver = "org.postgresql.Driver"
    url = "jdbc:postgresql://localhost:5432/testdb"
    username = "tester"
    password = "pivotal"
    query = "select age, name from source"
    partition_column = "name"
    split.string_split_mode = charset_based
  }
}
```

### Read And Write Greenplum

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = "org.postgresql.Driver"
    url = "jdbc:postgresql://localhost:5432/testdb"
    username = "tester"
    password = "pivotal"
    query = "select age, name from source"
    partition_column = "name"
    split.string_split_mode = charset_based
  }
}

sink {
  Jdbc {
    driver = "org.postgresql.Driver"
    url = "jdbc:postgresql://localhost:5432/testdb"
    username = "tester"
    password = "pivotal"
    query = "insert into sink(age, name) values(?, ?)"
  }
}
```

## Changelog

<ChangeLog />
