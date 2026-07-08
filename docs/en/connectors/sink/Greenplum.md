import ChangeLog from '../changelog/connector-jdbc.md';

# Greenplum

> Greenplum sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Write data to Greenplum using the [Jdbc connector](Jdbc.md). Greenplum uses the PostgreSQL protocol, so you can usually use the PostgreSQL JDBC driver. If you use the Greenplum native JDBC driver, provide the driver jar yourself.

## Using Dependency

### For Spark/Flink Engine

> 1. When using `org.postgresql.Driver`, ensure the [PostgreSQL JDBC driver](https://mvnrepository.com/artifact/org.postgresql/postgresql) has been placed in `${SEATUNNEL_HOME}/plugins/`.
> 2. When using `com.pivotal.jdbc.GreenplumDriver`, download the Greenplum JDBC driver by yourself and place it in `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. When using `org.postgresql.Driver`, ensure the [PostgreSQL JDBC driver](https://mvnrepository.com/artifact/org.postgresql/postgresql) has been placed in `${SEATUNNEL_HOME}/lib/`.
> 2. When using `com.pivotal.jdbc.GreenplumDriver`, download the Greenplum JDBC driver by yourself and place it in `${SEATUNNEL_HOME}/lib/`.

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

:::tip

Greenplum sink does not support exactly-once semantics because XA transaction is not supported by Greenplum.

:::

## Supported DataSource Info

| Datasource | Driver | Url | Maven |
|------------|--------|-----|-------|
| Greenplum by PostgreSQL driver | `org.postgresql.Driver` | `jdbc:postgresql://localhost:5432/testdb` | [Download](https://mvnrepository.com/artifact/org.postgresql/postgresql) |
| Greenplum native driver | `com.pivotal.jdbc.GreenplumDriver` | `jdbc:pivotal:greenplum://localhost:5432;DatabaseName=testdb` | Download from Greenplum |

## Options

Only Greenplum-specific commonly used options are listed here. Other JDBC sink options, such as `batch_size`, `max_retries`, `generate_sink_sql`, `database`, `table`, `primary_keys`, and `properties`, are inherited from [Jdbc Sink](Jdbc.md).

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| url | String | Yes | - | JDBC connection URL. Use `jdbc:postgresql://host:port/database` with PostgreSQL driver, or `jdbc:pivotal:greenplum://host:port;DatabaseName=database` with Greenplum native driver. |
| driver | String | Yes | - | JDBC driver class name, usually `org.postgresql.Driver` or `com.pivotal.jdbc.GreenplumDriver`. |
| username | String | No | - | Greenplum username. |
| password | String | No | - | Greenplum password. |
| query | String | No | - | SQL used to write upstream rows, for example `insert into sink(age, name) values(?, ?)`. `query` has higher priority than generated sink SQL. |
| batch_size | Int | No | 1000 | Maximum records buffered before flushing to Greenplum. |
| max_retries | Int | No | 0 | Retry times after `executeBatch` fails. |
| generate_sink_sql | Boolean | No | false | Generate insert SQL automatically from `database` and `table`. |
| database | String | No | - | Database name used when `generate_sink_sql = true`. |
| table | String | No | - | Target table name used when `generate_sink_sql = true`. |
| common-options | | No | - | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details. |

:::tip

For license compliance, SeaTunnel does not ship the Greenplum native JDBC driver. If you use `com.pivotal.jdbc.GreenplumDriver`, copy `greenplum-xxx.jar` to the engine dependency directory before running the job.

:::

## Task Example

### Write To Greenplum

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 16
    schema = {
      fields {
        age = "int"
        name = "string"
      }
    }
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

### Generate Sink SQL

```hocon
sink {
  Jdbc {
    driver = "org.postgresql.Driver"
    url = "jdbc:postgresql://localhost:5432/testdb"
    username = "tester"
    password = "pivotal"
    generate_sink_sql = true
    database = "testdb"
    table = "sink"
  }
}
```

## Changelog

<ChangeLog />
