# Snowflake

> JDBC Snowflake Source Connector
>
> ## Support those engines
>
> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>
>
  ## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

> supports query SQL and can achieve projection effect.
>
  ## Description

Read external data source data through JDBC.

## Supported DataSource list

| datasource |                    supported versions                    |                  driver                   |                          url                           |                                    maven                                    |
|------------|----------------------------------------------------------|-------------------------------------------|--------------------------------------------------------|-----------------------------------------------------------------------------|
| snowflake  | Different dependency version has different driver class. | net.snowflake.client.jdbc.SnowflakeDriver | jdbc:snowflake://<account_name>.snowflakecomputing.com | [Download](https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc) |

## Database dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATNUNNEL_HOME/plugins/jdbc/lib/' working directory<br/>
> For example Snowflake datasource: cp snowflake-connector-java-xxx.jar $SEATNUNNEL_HOME/plugins/jdbc/lib/
>
  ## Data Type Mapping

|                             Snowflake Data type                             | SeaTunnel Data type |
|-----------------------------------------------------------------------------|---------------------|
| BOOLEAN                                                                     | BOOLEAN             |
| TINYINT<br/>SMALLINT<br/>BYTEINT<br/>                                       | SHORT_TYPE          |
| INT<br/>INTEGER<br/>                                                        | INT                 |
| BIGINT                                                                      | LONG                |
| DECIMAL<br/>NUMERIC<br/>NUMBER<br/>                                         | DECIMAL(x,y)        |
| DECIMAL(x,y)(Get the designated column's specified column size.>38)         | DECIMAL(38,18)      |
| REAL<br/>FLOAT4                                                             | FLOAT               |
| DOUBLE<br/>DOUBLE PRECISION<br/>FLOAT8<br/>FLOAT<br/>                       | DOUBLE              |
| CHAR<br/>CHARACTER<br/>VARCHAR<br/>STRING<br/>TEXT<br/>VARIANT<br/>OBJECT   | STRING              |
| DATE                                                                        | DATE                |
| TIME                                                                        | TIME                |
| DATETIME<br/>TIMESTAMP<br/>TIMESTAMP_LTZ<br/>TIMESTAMP_NTZ<br/>TIMESTAMP_TZ | TIMESTAMP           |
| BINARY<br/>VARBINARY                                                        | BYTES               |
| GEOGRAPHY (WKB or EWKB)<br/>GEOMETRY (WKB or EWKB)                          | BYTES               |
| GEOGRAPHY (GeoJSON, WKT or EWKT)<br/>GEOMETRY (GeoJSON, WKB or EWKB)        | STRING              |

## Options

|             name             |    type    | required |     default     |                                                                                                                            description                                                                                                                            |
|------------------------------|------------|----------|-----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                          | String     | Yes      | -               | The URL of the JDBC connection. Refer to a case: jdbc:snowflake://<account_name>.snowflakecomputing.com                                                                                                                                                           |
| driver                       | String     | Yes      | -               | The jdbc class name used to connect to the remote data source,<br/> if you use Snowflake the value is `net.snowflake.client.jdbc.SnowflakeDriver`.                                                                                                                |
| user                         | String     | No       | -               | Connection instance user name                                                                                                                                                                                                                                     |
| password                     | String     | No       | -               | Connection instance password                                                                                                                                                                                                                                      |
| query                        | String     | Yes      | -               | Query statement                                                                                                                                                                                                                                                   |
| connection_check_timeout_sec | Int        | No       | 30              | The time in seconds to wait for the database operation used to validate the connection to complete                                                                                                                                                                |
| partition_column             | String     | No       | -               | The column name for parallelism's partition, only support numeric type,Only support numeric type primary key, and only can config one column.                                                                                                                     |
| partition_lower_bound        | BigDecimal | No       | -               | The partition_column min value for scan, if not set SeaTunnel will query database get min value.                                                                                                                                                                  |
| partition_upper_bound        | BigDecimal | No       | -               | The partition_column max value for scan, if not set SeaTunnel will query database get max value.                                                                                                                                                                  |
| partition_num                | Int        | No       | job parallelism | The number of partition count, only support positive integer. default value is job parallelism                                                                                                                                                                    |
| fetch_size                   | Int        | No       | 0               | For queries that return a large number of objects,you can configure<br/> the row fetch size used in the query toimprove performance by<br/> reducing the number database hits required to satisfy the selection criteria.<br/> Zero means use jdbc default value. |
| common-options               |            | No       | -               | Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details                                                                                                                                                           |

## tips

> If partition_column is not set, it will run in single concurrency, and if partition_column is set, it will be executed  in parallel according to the concurrency of tasks.
>
> JDBC Driver Connection Parameters are supported in JDBC connection string. E.g, you can add `?GEOGRAPHY_OUTPUT_FORMAT='EWKT'` to specify the Geospatial Data Types. For more information about configurable parameters, and geospatial data types please visit Snowflake official [document](https://docs.snowflake.com/en/sql-reference/data-types-geospatial)

## Task Example

### simple:

> This example queries type_bin 'table' 16 data in your test "database" in single parallel and queries all of its fields. You can also specify which fields to query for final output to the console.
>
> ```
> # Defining the runtime environment
> env {
> # You can set flink configuration here
> execution.parallelism = 2
> job.mode = "BATCH"
> }
> source{
> Jdbc {
> url = "jdbc:snowflake://<account_name>.snowflakecomputing.com"
> driver = "net.snowflake.client.jdbc.SnowflakeDriver"
> connection_check_timeout_sec = 100
> user = "root"
> password = "123456"
> query = "select * from type_bin limit 16"
> }
> }
> transform {
> # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
> # please go to https://seatunnel.apache.org/docs/transform-v2/sql
> }
> sink {
> Console {}
> }
> ```

### parallel:

> Read your query table in parallel with the shard field you configured and the shard data  You can do this if you want to read the whole table
>
> ```
> Jdbc {
> url = "jdbc:snowflake://<account_name>.snowflakecomputing.com"
> driver = "net.snowflake.client.jdbc.SnowflakeDriver"
> connection_check_timeout_sec = 100
> user = "root"
> password = "123456"
> # Define query logic as required
> query = "select * from type_bin"
> # Parallel sharding reads fields
> partition_column = "id"
> # Number of fragments
> partition_num = 10
> }
> ```

### parallel boundary:

> It is more efficient to specify the data within the upper and lower bounds of the query It is more efficient to read your data source according to the upper and lower boundaries you configured
>
> ```
> Jdbc {
> url = "jdbc:snowflake://<account_name>.snowflakecomputing.com"
> driver = "net.snowflake.client.jdbc.SnowflakeDriver"
> connection_check_timeout_sec = 100
> user = "root"
> password = "123456"
> # Define query logic as required
> query = "select * from type_bin"
> partition_column = "id"
> # Read start boundary
> partition_lower_bound = 1
> # Read end boundary
> partition_upper_bound = 500
> partition_num = 10
> }
> ```

