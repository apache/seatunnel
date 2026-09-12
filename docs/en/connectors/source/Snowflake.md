import ChangeLog from '../changelog/connector-jdbc.md';

# Snowflake

> JDBC Snowflake Source Connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Read data from Snowflake through JDBC. SeaTunnel uses the official Snowflake JDBC driver and the JDBC source plugin. Provide your Snowflake account identifier in the `url` and select only the columns you need in `query` to control the output schema.

## Database Dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATUNNEL_HOME/plugins/jdbc/lib/' working directory<br/>
> For example Snowflake datasource: cp snowflake-connector-java-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

> Supports query SQL and can achieve column projection.

## Supported DataSource Info

| Datasource | Supported versions                                   | Driver                                | Url                                          | Maven                                                          |
|------------|------------------------------------------------------|---------------------------------------|----------------------------------------------|----------------------------------------------------------------|
| Snowflake  | Different dependency version has different driver class. | net.snowflake.client.jdbc.SnowflakeDriver | jdbc:snowflake://<account_name>.snowflakecomputing.com | [Download](https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc) |

## Data Type Mapping

|                             Snowflake Data Type                             | SeaTunnel Data Type |
|-----------------------------------------------------------------------------|---------------------|
| BOOLEAN                                                                     | BOOLEAN             |
| TINYINT<br/>SMALLINT<br/>BYTEINT                                            | SHORT               |
| INT<br/>INTEGER                                                             | INT                 |
| BIGINT                                                                      | LONG                |
| DECIMAL<br/>NUMERIC<br/>NUMBER<br/>                                         | DECIMAL(p, s)       |
| DECIMAL(p, s) (with `p > 38`)                                               | DECIMAL(38, 18)     |
| REAL<br/>FLOAT4                                                             | FLOAT               |
| DOUBLE<br/>DOUBLE PRECISION<br/>FLOAT8<br/>FLOAT                            | DOUBLE              |
| CHAR<br/>CHARACTER<br/>VARCHAR<br/>STRING<br/>TEXT<br/>VARIANT<br/>OBJECT   | STRING              |
| DATE                                                                        | DATE                |
| TIME                                                                        | TIME                |
| DATETIME<br/>TIMESTAMP<br/>TIMESTAMP_LTZ<br/>TIMESTAMP_NTZ<br/>TIMESTAMP_TZ | TIMESTAMP           |
| BINARY<br/>VARBINARY                                                        | BYTES               |
| GEOGRAPHY (WKB or EWKB)<br/>GEOMETRY (WKB or EWKB)                          | BYTES               |
| GEOGRAPHY (GeoJSON, WKT or EWKT)<br/>GEOMETRY (GeoJSON, WKB or EWKB)        | STRING              |

## Source Options

|             Name             |    Type    | Required | Default | Description                                                                                                                                                                                                                                                  |
|------------------------------|------------|----------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                          | String     | Yes      | -       | JDBC connection URL, for example `jdbc:snowflake://<account_name>.snowflakecomputing.com`. Add Snowflake JDBC parameters (e.g. `?GEOGRAPHY_OUTPUT_FORMAT='EWKT'`) directly in the URL.                                                                        |
| driver                       | String     | Yes      | -       | JDBC driver class name. Use `net.snowflake.client.jdbc.SnowflakeDriver` for Snowflake.                                                                                                                                                                       |
| username                     | String     | No       | -       | Username for the Snowflake account.                                                                                                                                                                                                                          |
| password                     | String     | No       | -       | Password for the Snowflake account.                                                                                                                                                                                                                          |
| query                        | String     | Yes      | -       | SELECT statement used to read data. The column list of the SELECT defines the output schema; select only the columns you need.                                                                                                                                |
| connection_check_timeout_sec | Int        | No       | 30      | Seconds to wait for the connection check before failing.                                                                                                                                                                                                     |
| partition_column             | String     | No       | -       | Column used to split data for parallel reading. Supports numeric columns and string columns (with `split.string_split_mode`); only one column can be configured.                                                                                              |
| partition_lower_bound        | String     | No       | -       | Lower bound of `partition_column` for range splitting. If not set, SeaTunnel queries the minimum value.                                                                                                                                                      |
| partition_upper_bound        | String     | No       | -       | Upper bound of `partition_column` for range splitting. If not set, SeaTunnel queries the maximum value.                                                                                                                                                      |
| partition_num                | Int        | No       | 10      | Number of source splits used in parallel reading. Defaults to `10`. Increase this value if `env.parallelism` is larger and you want one split per reader task.                                                                                                |
| fetch_size                   | Int        | No       | 0       | JDBC fetch size for the query. `0` means use the JDBC driver default. Use a positive value to reduce database round-trips for large result sets.                                                                                                              |
| properties                   | Map        | No       | -       | Extra JDBC connection properties. When the same key appears in both `properties` and `url`, the precedence is driver-specific.                                                                                                                               |
| common-options               |            | No       | -       | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.                                                                                                                            |

### Tips

> If `partition_column` is not set, the source reads with one split. If it is set, SeaTunnel reads data in parallel according to `partition_num` (default 10) or the job's parallelism, whichever is greater.
>
> Snowflake JDBC URL parameters such as `GEOGRAPHY_OUTPUT_FORMAT` can be appended with `?` (e.g. `?GEOGRAPHY_OUTPUT_FORMAT='EWKT'`). See the Snowflake [Geospatial Data Types](https://docs.snowflake.com/en/sql-reference/data-types-geospatial) reference for the full list.

## Task Example

### Simple

This example queries all fields of `type_bin` from Snowflake and prints them to the console.

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:snowflake://<account_name>.snowflakecomputing.com"
    driver = "net.snowflake.client.jdbc.SnowflakeDriver"
    connection_check_timeout_sec = 100
    username = "USER"
    password = "PASSWORD"
    query = "select * from type_bin limit 16"
  }
}

sink {
  Console {}
}
```

### Parallel Reading By Numeric Column

Read the table in parallel by a numeric `partition_column` and let SeaTunnel pick the lower and upper bounds for you.

```hocon
source {
  Jdbc {
    url = "jdbc:snowflake://<account_name>.snowflakecomputing.com"
    driver = "net.snowflake.client.jdbc.SnowflakeDriver"
    username = "USER"
    password = "PASSWORD"
    query = "select * from type_bin"
    partition_column = "id"
    partition_num = 10
  }
}
```

### Parallel Reading With Explicit Bounds

Provide explicit `partition_lower_bound` and `partition_upper_bound` to skip the extra `MIN`/`MAX` query SeaTunnel would otherwise issue to learn the column range.

```hocon
source {
  Jdbc {
    url = "jdbc:snowflake://<account_name>.snowflakecomputing.com"
    driver = "net.snowflake.client.jdbc.SnowflakeDriver"
    username = "USER"
    password = "PASSWORD"
    query = "select * from type_bin"
    partition_column = "id"
    partition_lower_bound = 1
    partition_upper_bound = 500
    partition_num = 10
  }
}
```

## Notes

- Use the `Jdbc` plugin name for Snowflake jobs and set `driver = "net.snowflake.client.jdbc.SnowflakeDriver"`.
- Place the Snowflake JDBC driver jar in `$SEATUNNEL_HOME/plugins/jdbc/lib/` before running the job.
- When reading in parallel, `partition_column`, `partition_lower_bound`, `partition_upper_bound`, and `partition_num` must describe the same numeric column range.
- Snowflake geospatial columns are returned as bytes or string depending on Snowflake JDBC URL parameters such as `GEOGRAPHY_OUTPUT_FORMAT`.

## Changelog

<ChangeLog />
