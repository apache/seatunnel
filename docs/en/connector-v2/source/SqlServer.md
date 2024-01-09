# SQL Server

> JDBC SQL Server Source Connector

## Support SQL Server Version

- server:2008 (Or later version for information only)

## Support Those Engines

> Spark <br/>
> Flink <br/>
> Seatunnel Zeta <br/>

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

> supports query SQL and can achieve projection effect.

## Description

Read external data source data through JDBC.

## Supported DataSource Info

| datasource |   supported versions    |                    driver                    |               url               |                                       maven                                       |
|------------|-------------------------|----------------------------------------------|---------------------------------|-----------------------------------------------------------------------------------|
| SQL Server | support version >= 2008 | com.microsoft.sqlserver.jdbc.SQLServerDriver | jdbc:sqlserver://localhost:1433 | [Download](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc) |

## Database dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATNUNNEL_HOME/plugins/jdbc/lib/' working directory<br/>
> For example SQL Server datasource: cp mssql-jdbc-xxx.jar $SEATNUNNEL_HOME/plugins/jdbc/lib/

## Data Type Mapping

|                         SQLserver Data type                          | Seatunnel Data type |
|----------------------------------------------------------------------|---------------------|
| BIT                                                                  | BOOLEAN             |
| TINYINT<br/>SMALLINT                                                 | SMALLINT            |
| INTEGER<br/>INT                                                      | INT                 |
| BIGINT                                                               | BIGINT              |
| NUMERIC(p,s)<br/>DECIMAL(p,s)<br/>MONEY<br/>SMALLMONEY               | DECIMAL(p,s)        |
| FLOAT(1~24)<br/>REAL                                                 | FLOAT               |
| DOUBLE<br/>FLOAT(>24)                                                | DOUBLE              |
| CHAR<br/>NCHAR<br/>VARCHAR<br/>NTEXT<br/>NVARCHAR<br/>TEXT<br/>XML   | STRING              |
| DATE                                                                 | DATE                |
| TIME(s)                                                              | TIME(s)             |
| DATETIME(s)<br/>DATETIME2(s)<br/>DATETIMEOFFSET(s)<br/>SMALLDATETIME | TIMESTAMP(s)        |
| BINARY<br/>VARBINARY<br/>IMAGE                                       | BYTES               |

## Source Options

|             name             |  type  | required |     default     |                                                                                                                            Description                                                                                                                            |
|------------------------------|--------|----------|-----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                          | String | Yes      | -               | The URL of the JDBC connection. Refer to a case: jdbc:sqlserver://127.0.0.1:1434;database=TestDB                                                                                                                                                                  |
| driver                       | String | Yes      | -               | The jdbc class name used to connect to the remote data source,<br/> if you use SQLserver the value is `com.microsoft.sqlserver.jdbc.SQLServerDriver`.                                                                                                             |
| user                         | String | No       | -               | Connection instance user name                                                                                                                                                                                                                                     |
| password                     | String | No       | -               | Connection instance password                                                                                                                                                                                                                                      |
| query                        | String | Yes      | -               | Query statement                                                                                                                                                                                                                                                   |
| connection_check_timeout_sec | Int    | No       | 30              | The time in seconds to wait for the database operation used to validate the connection to complete                                                                                                                                                                |
| partition_column             | String | No       | -               | The column name for parallelism's partition, only support numeric type.                                                                                                                                                                                           |
| partition_lower_bound        | Long   | No       | -               | The partition_column min value for scan, if not set SeaTunnel will query database get min value.                                                                                                                                                                  |
| partition_upper_bound        | Long   | No       | -               | The partition_column max value for scan, if not set SeaTunnel will query database get max value.                                                                                                                                                                  |
| partition_num                | Int    | No       | job parallelism | The number of partition count, only support positive integer. default value is job parallelism                                                                                                                                                                    |
| fetch_size                   | Int    | No       | 0               | For queries that return a large number of objects,you can configure<br/> the row fetch size used in the query toimprove performance by<br/> reducing the number database hits required to satisfy the selection criteria.<br/> Zero means use jdbc default value. |
| common-options               |        | No       | -               | Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details                                                                                                                                                           |

## tips

> If partition_column is not set, it will run in single concurrency, and if partition_column is set, it will be executed  in parallel according to the concurrency of tasks.

## Task Example

### Simple:

> Simple single task to read the data table

```
# Defining the runtime environment
env {
  parallelism = 1
  job.mode = "BATCH"
}
source{
    Jdbc {
        driver = com.microsoft.sqlserver.jdbc.SQLServerDriver
        url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
        user = SA
        password = "Y.sa123456"
        query = "select * from full_types_jdbc"
    }
}

transform {
    # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/transform-v2/sql
}

sink {
    Console {}
}
```

### Parallel:

> Read your query table in parallel with the shard field you configured and the shard data You can do this if you want to read the whole table

```
env {
  parallelism = 10
  job.mode = "BATCH"
}

source {
    Jdbc {
        driver = com.microsoft.sqlserver.jdbc.SQLServerDriver
        url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
        user = SA
        password = "Y.sa123456"
        # Define query logic as required
        query = "select * from full_types_jdbc"
        # Parallel sharding reads fields
        partition_column = "id"
        # Number of fragments
        partition_num = 10
    }
}

transform {
    # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/transform-v2/sql
}

sink {
    Console {}
}

```

### Fragmented Parallel Read Simple:

> It is a shard that reads data in parallel fast

```
env {
  # You can set engine configuration here
  parallelism = 10
}

source {
  # This is a example source plugin **only for test and demonstrate the feature source plugin**
  Jdbc {
    driver = com.microsoft.sqlserver.jdbc.SQLServerDriver
    url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
    user = SA
    password = "Y.sa123456"
    query = "select * from column_type_test.dbo.full_types_jdbc"
    # Parallel sharding reads fields
    partition_column = "id"
    # Number of fragments
    partition_num = 10

  }
  # If you would like to get more information about how to configure seatunnel and see full list of source plugins,
  # please go to https://seatunnel.apache.org/docs/connector-v2/source/Jdbc
}


transform {
  # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
  # please go to https://seatunnel.apache.org/docs/transform-v2/sql
}

sink {
  Console {}
  # If you would like to get more information about how to configure seatunnel and see full list of sink plugins,
  # please go to https://seatunnel.apache.org/docs/connector-v2/sink/Jdbc
}
```

