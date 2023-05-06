# SQL Server

> JDBC SQL Server Source Connector

## Support those engines

> Spark <br/>
> Flink <br/>
> Seatunnel Zeta <br/>

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)

supports query SQL and can achieve projection effect.

- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

## Description

Read external data source data through JDBC.

## Supported DataSource list

| datasource | supported versions      | driver                                       | url                             | maven                                                        |
| ---------- | ----------------------- | -------------------------------------------- | ------------------------------- | ------------------------------------------------------------ |
| SQL Server | support version >= 2008 | com.microsoft.sqlserver.jdbc.SQLServerDriver | jdbc:sqlserver://localhost:1433 | [Download](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc) |

## Database dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATNUNNEL_HOME/plugins/jdbc/lib/' working directory<br>
> For example Redshift datasource: cp RedshiftJDBC42-xxx.jar $SEATNUNNEL_HOME/plugins/jdbc/lib/

## Data Type Mapping

| SQLserver Data type                                          | Seatunnel Data type                                          |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| BIT                                                          | BOOLEAN                                                      |
| TINYINT<br/>SMALLINT                                         | SHORT                                                        |
| INTEGER                                                      | INT                                                          |
| BIGINT                                                       | LONG                                                         |
| DECIMAL<br />NUMERIC<br />MONEY<br />SMALLMONEY              | DECIMAL((Get the designated column's specified column size)+1,<br/>(Gets the designated column's number of digits to right of the <br />decimal point.))) |
| REAL                                                         | FLOAT                                                        |
| FLOAT                                                        | DOUBLE                                                       |
| CHAR<br />NCHAR<br />VARCHAR<br />NTEXT<br />NVARCHAR<br />TEXT | STRING                                                       |
| DATE                                                         | LOCAL_DATE                                                   |
| TIME                                                         | LOCAL_TIME                                                   |
| DATETIME<br />DATETIME2<br />SMALLDATETIME<br />DATETIMEOFFSET | LOCAL_DATE_TIME                                              |
| TIMESTAMP<br />BINARY<br />VARBINARY<br />IMAGE<br />UNKNOWN | Not supported yet                                            |

## Options

| name                         | type   | required | default         | Description                                                  |
| ---------------------------- | ------ | -------- | --------------- | ------------------------------------------------------------ |
| url                          | String | Yes      | -               | The URL of the JDBC connection. Refer to a case: jdbc:sqlserver://localhost:1433 |
| driver                       | String | Yes      | -               | The jdbc class name used to connect to the remote data source,<br/> if you use SQLserver the value is `com.microsoft.sqlserver.jdbc.SQLServerDriver`. |
| user                         | String | No       | -               | Connection instance user name                                |
| password                     | String | No       | -               | Connection instance password                                 |
| query                        | String | Yes      | -               | Query statement                                              |
| connection_check_timeout_sec | Int    | No       | 30              | The time in seconds to wait for the database operation used to validate the connection to complete |
| partition_column             | String | No       | -               | The column name for parallelism's partition, only support numeric type. |
| partition_lower_bound        | Long   | No       | -               | The partition_column min value for scan, if not set SeaTunnel will query database get min value. |
| partition_upper_bound        | Long   | No       | -               | The partition_column max value for scan, if not set SeaTunnel will query database get max value. |
| partition_num                | Int    | No       | job parallelism | The number of partition count, only support positive integer. default value is job parallelism |
| fetch_size                   | Int    | No       | 0               | For queries that return a large number of objects,you can configure<br/> the row fetch size used in the query toimprove performance by<br/> reducing the number database hits required to satisfy the selection criteria.<br/> Zero means use jdbc default value. |
| common-options               |        | No       | -               | Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details |

## tips

> If partition_column is not set, it will run in single concurrency, and if partition_column is set, it will be executed  in parallel according to the concurrency of tasks.

## Task Example

### simple:

> This is to access your data source according to your query parameter We can use this if we don't have a speed requirement

```
# Defining the runtime environment
env {
# You can set SQLServer configuration here
  execution.parallelism = 2
  job.mode = "BATCH"
}
source {
  Jdbc {
    url = "jdbc:sqlserver://localhost:1433;databaseName=test"
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    connection_check_timeout_sec = 100
    user = "username"
    password = "password"
    query = "SELECT * FROM my_table"
  }
}

transform {
  # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/transform/sql
}

sink {
  Console {}
}

```

### parallel:

> Read your query table in parallel with the shard field you configured and the shard data  You can do this if you want to read the whole table

```
Jdbc {
    url = "jdbc:sqlserver://localhost:1433;databaseName=myDatabase"
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    connection_check_timeout_sec = 100
    user = "myUsername"
    password = "myPassword"
    # Define query logic as required
    query = "SELECT * FROM myTable"
    # Parallel sharding reads fields
    partition_column = "id" 
    # Number of fragments
    partition_num = 10
}
```

### parallel boundary:

> It is more efficient to specify the data within the upper and lower bounds of the query It is more efficient to read your data source according to the upper and lower boundaries you configured

```
Jdbc {
    url = "jdbc:sqlserver://localhost:1433;databaseName=myDatabase"
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    connection_check_timeout_sec = 100
    user = "myUsername"
    password = "myPassword"
    # Define query logic as required
    query = "SELECT * FROM myTable"
    partition_column = "id" 
    # Read start boundary
    partition_lower_bound = 1
    # Read end boundary
    partition_upper_bound = 500
    partition_num = 10
}
```

## Changelog

