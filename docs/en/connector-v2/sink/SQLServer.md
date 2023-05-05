# SQL Server

> JDBC SQL Server Sink Connector

## Support those engines

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

> Use `Xa transactions` to ensure `exactly-once`. So only support `exactly-once` for the database which is
> support `Xa transactions`. You can set `is_exactly_once=true` to enable it.

## Description

Write data through jdbc. Support Batch mode and Streaming mode, support concurrent writing, support exactly-once
semantics (using XA transaction guarantee).

## Supported DataSource list

| datasource | supported versions                                       | driver                                       | url                             | maven                                                        |
| ---------- | -------------------------------------------------------- | -------------------------------------------- | ------------------------------- | ------------------------------------------------------------ |
| SQL Server | Different dependency version has different driver class. | com.microsoft.sqlserver.jdbc.SQLServerDriver | jdbc:sqlserver://localhost:1433 | [Download](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc) |

## Database dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATNUNNEL_HOME/plugins/jdbc/lib/' working directory<br/>
> For example SQL Server datasource: cp mssql-jdbc-xxx.jar $SEATNUNNEL_HOME/plugins/jdbc/lib/

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

> This example defines a SeaTunnel synchronization task that automatically generates data through FakeSource and sends it to JDBC Sink. FakeSource generates a total of 16 rows of data (row.num=16), with each row having two fields, name (string type) and age (int type). The final target table is test_table will also be 16 rows of data in the table. Before run this job, you need create database test and table test_table in your SQL Server. And if you have not yet installed and deployed SeaTunnel, you need to follow the instructions in [Install SeaTunnel](../../start-v2/locally/deployment.md) to install and deploy SeaTunnel. And then follow the instructions in [Quick Start With SeaTunnel Engine](../../start-v2/locally/quick-start-seatunnel-engine.md) to run this job.

```
# Defining the runtime environment
env {
  # You can set flink configuration here
  execution.parallelism = 1
  job.mode = "BATCH"
}

source {
  # This is a example source plugin **only for test and demonstrate the feature source plugin**
  FakeSource {
    parallelism = 1
    result_table_name = "fake"
    row.num = 16
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
  # If you would like to get more information about how to configure seatunnel and see full list of source plugins,
  # please go to https://seatunnel.apache.org/docs/category/source-v2
}

transform {
  # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/category/transform-v2
}

sink {
    jdbc {
        url = "jdbc:sqlserver://localhost:1433;databaseName=mydatabase"
        driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        user = "root"
        password = "123456"
        query = "insert into test_table(name,age) values(?,?)"
        }
  # If you would like to get more information about how to configure seatunnel and see full list of sink plugins,
  # please go to https://seatunnel.apache.org/docs/category/sink-v2
}
```

### Exactly-once :

> For accurate write scene we guarantee accurate once

```
jdbc {
    url = "jdbc:sqlserver://localhost:1433;databaseName=testdb"
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

    max_retries = 0
    user = "root"
    password = "123456"
    query = "insert into test_table(name,age) values(?,?)"

    is_exactly_once = "true"

    xa_data_source_class_name = "com.mysql.cj.jdbc.MysqlXADataSource"
}
```

### CDC(Change data capture) event

> CDC change data is also supported by us In this case, you need config database, table and primary_keys.

```
jdbc {
    url = "jdbc:mysql://localhost:3306/test"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "123456"
    
    # You need to configure both database and table
    database = test
    table = sink_table
    primary_keys = ["id","name"]
}
```