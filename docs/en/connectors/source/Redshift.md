import ChangeLog from '../changelog/connector-jdbc.md';

# Redshift

> JDBC Redshift Source Connector

## Support those engines

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## Description

Read data from Amazon Redshift through the standard JDBC interface. The connector uses the Redshift JDBC
driver (`com.amazon.redshift.jdbc.Driver`) and submits the configured `query` to fetch rows. Redshift is
PostgreSQL-compatible, so the connector can read any table accessible to the JDBC user, including
columnar SUPER values and standard scalar types. Parallel reads via `partition_column` and multi-table
reads via `table_list` are supported.

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

> supports query SQL and can achieve projection effect.

## Supported DataSource list

| datasource |                    supported versions                    |             driver              |                   url                   |                                       maven                                        |
|------------|----------------------------------------------------------|---------------------------------|-----------------------------------------|------------------------------------------------------------------------------------|
| redshift   | Different dependency version has different driver class. | com.amazon.redshift.jdbc.Driver | jdbc:redshift://localhost:5439/database | [Download](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) |

## Source Options

|        Name        |   Type   | Required |     Default     |                                                                 Description                                                                 |
|--------------------|----------|----------|-----------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| url                | String   | Yes      | -               | The URL of the JDBC connection. Example: `jdbc:redshift://localhost:5439/database`.                                                         |
| driver             | String   | Yes      | -               | The JDBC class name used to connect to Redshift. The value is `com.amazon.redshift.jdbc.Driver`.                                            |
| username           | String   | No       | -               | Connection instance user name.                                                                                                              |
| password           | String   | No       | -               | Connection instance password.                                                                                                               |
| query              | String   | No       | -               | SELECT statement. Use this or `table_path`/`table_list` to define the rows to read. The query column list determines the output schema.    |
| table_path         | String   | No       | -               | The fully-qualified table to read, for example `public.table2`. Useful as a shortcut for a single-table read.                               |
| table_list         | Array    | No       | -               | List of tables to read. Each entry can override `table_path` and `query`. Enables multi-table reading and auto-split.                       |
| connection_check_timeout_sec | Int | No       | 30              | The time, in seconds, to wait for the database operation used to validate the connection to complete.                                       |
| partition_column   | String   | No       | -               | The column name used to split the data for parallel reading. Only numeric primary key columns are supported.                               |
| partition_lower_bound | BigDecimal | No   | -               | The `partition_column` minimum value for the scan. If not set, SeaTunnel queries the database for the minimum value.                       |
| partition_upper_bound | BigDecimal | No   | -               | The `partition_column` maximum value for the scan. If not set, SeaTunnel queries the database for the maximum value.                       |
| partition_num      | Int      | No       | job parallelism | Number of partitions. Only positive integers are supported. Default value is the job parallelism.                                            |
| fetch_size         | Int      | No       | 0               | For queries that return a large number of rows, configure the row fetch size used in the query to improve performance. `0` uses the driver default. |
| where_condition    | String   | No       | -               | Common row filter applied to all tables/queries. Must start with `where`, for example `where id > 100`.                                     |
| split.size         | Int      | No       | 8096            | The split size (rows) for auto-split when `table_path` or `table_list` is used.                                                              |
| common-options     |          | No       | -               | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.          |

## Database dependency

> Place the [Redshift JDBC driver](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42)
> in `${SEATUNNEL_HOME}/plugins/jdbc/lib/` for Spark/Flink, or `${SEATUNNEL_HOME}/lib/` for SeaTunnel Zeta.

## Data Type Mapping

|                                                Redshift Data type                                                 |                                                                 Seatunnel Data type                                                                 |
|-------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| SMALLINT<br />INT2                                                                                                | SHORT                                                                                                                                               |
| INTEGER<br />INT<br />INT4                                                                                        | INT                                                                                                                                                 |
| BIGINT<br />INT8<br />OID                                                                                         | LONG                                                                                                                                                |
| DECIMAL<br />NUMERIC                                                                                              | DECIMAL((Get the designated column's specified column size)+1, (Get the designated column's number of digits to the right of the decimal point.))  |
| REAL<br />FLOAT4                                                                                                  | FLOAT                                                                                                                                               |
| DOUBLE_PRECISION<br />FLOAT8<br />FLOAT                                                                           | DOUBLE                                                                                                                                              |
| BOOLEAN<br />BOOL                                                                                                 | BOOLEAN                                                                                                                                             |
| CHAR<br />CHARACTER<br />NCHAR<br />BPCHAR<br />VARCHAR<br />CHARACTER_VARYING<br />NVARCHAR<br />TEXT<br />SUPER | STRING                                                                                                                                              |
| VARBYTE<br />BINARY_VARYING                                                                                       | BYTES                                                                                                                                               |
| TIME<br />TIME_WITH_TIME_ZONE<br />TIMETZ                                                                         | LOCALTIME                                                                                                                                           |
| TIMESTAMP<br />TIMESTAMP_WITH_OUT_TIME_ZONE<br />TIMESTAMPTZ                                                      | LOCALDATETIME                                                                                                                                       |

## Example

### Simple

> This example reads a single table from Redshift through `table_path` and writes the rows to the console.

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}
source {
    Jdbc {
        url = "jdbc:redshift://localhost:5439/dev"
        driver = "com.amazon.redshift.jdbc.Driver"
        username = "root"
        password = "123456"

        table_path = "public.table2"
        # Use query to filter rows & columns
        query = "select id, name from public.table2 where id > 100"

        #split.size = 8096
        #split.even-distribution.factor.upper-bound = 100
        #split.even-distribution.factor.lower-bound = 0.05
        #split.sample-sharding.threshold = 1000
        #split.inverse-sampling.rate = 1000
    }
}

sink {
    Console {}
}
```

### Multiple table read

> Configuring `table_list` turns on auto split. Configure `split.*` to adjust the split strategy.

```hocon
env {
  job.mode = "BATCH"
  parallelism = 2
}
source {
  Jdbc {
    url = "jdbc:redshift://localhost:5439/dev"
    driver = "com.amazon.redshift.jdbc.Driver"
    username = "root"
    password = "123456"

    table_list = [
      {
        table_path = "public.table1"
      },
      {
        table_path = "public.table2"
        # Use query to filter rows & columns
        query = "select id, name from public.table2 where id > 100"
      }
    ]
    #split.size = 8096
    #split.even-distribution.factor.upper-bound = 100
    #split.even-distribution.factor.lower-bound = 0.05
    #split.sample-sharding.threshold = 1000
    #split.inverse-sampling.rate = 1000
  }
}

sink {
  Console {}
}
```

## Changelog

<ChangeLog />