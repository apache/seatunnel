# OceanBase

> JDBC OceanBase Source Connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

## Description

Read external data source data through JDBC.

## Supported DataSource Info

| Datasource |       Supported versions       |          Driver           |                 Url                  |                                     Maven                                     |
|------------|--------------------------------|---------------------------|--------------------------------------|-------------------------------------------------------------------------------|
| OceanBase  | All OceanBase server versions. | com.oceanbase.jdbc.Driver | jdbc:oceanbase://localhost:2883/test | [Download](https://mvnrepository.com/artifact/com.oceanbase/oceanbase-client) |

## Database Dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATNUNNEL_HOME/plugins/jdbc/lib/' working directory<br/>
> For example: cp oceanbase-client-xxx.jar $SEATNUNNEL_HOME/plugins/jdbc/lib/

## Data Type Mapping

### Mysql Mode

|                                                          Mysql Data type                                                          |                                                                 SeaTunnel Data type                                                                 |
|-----------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| BIT(1)<br/>INT UNSIGNED                                                                                                           | BOOLEAN                                                                                                                                             |
| TINYINT<br/>TINYINT UNSIGNED<br/>SMALLINT<br/>SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED<br/>INT<br/>INTEGER<br/>YEAR | INT                                                                                                                                                 |
| INT UNSIGNED<br/>INTEGER UNSIGNED<br/>BIGINT                                                                                      | BIGINT                                                                                                                                              |
| BIGINT UNSIGNED                                                                                                                   | DECIMAL(20,0)                                                                                                                                       |
| DECIMAL(x,y)(Get the designated column's specified column size.<38)                                                               | DECIMAL(x,y)                                                                                                                                        |
| DECIMAL(x,y)(Get the designated column's specified column size.>38)                                                               | DECIMAL(38,18)                                                                                                                                      |
| DECIMAL UNSIGNED                                                                                                                  | DECIMAL((Get the designated column's specified column size)+1,<br/>(Gets the designated column's number of digits to right of the decimal point.))) |
| FLOAT<br/>FLOAT UNSIGNED                                                                                                          | FLOAT                                                                                                                                               |
| DOUBLE<br/>DOUBLE UNSIGNED                                                                                                        | DOUBLE                                                                                                                                              |
| CHAR<br/>VARCHAR<br/>TINYTEXT<br/>MEDIUMTEXT<br/>TEXT<br/>LONGTEXT<br/>JSON                                                       | STRING                                                                                                                                              |
| DATE                                                                                                                              | DATE                                                                                                                                                |
| TIME                                                                                                                              | TIME                                                                                                                                                |
| DATETIME<br/>TIMESTAMP                                                                                                            | TIMESTAMP                                                                                                                                           |
| TINYBLOB<br/>MEDIUMBLOB<br/>BLOB<br/>LONGBLOB<br/>BINARY<br/>VARBINAR<br/>BIT(n)                                                  | BYTES                                                                                                                                               |
| GEOMETRY<br/>UNKNOWN                                                                                                              | Not supported yet                                                                                                                                   |

### Oracle Mode

|                     Oracle Data type                      | SeaTunnel Data type |
|-----------------------------------------------------------|---------------------|
| Number(p), p <= 9                                         | INT                 |
| Number(p), p <= 18                                        | BIGINT              |
| Number(p), p > 18                                         | DECIMAL(38,18)      |
| REAL<br/> BINARY_FLOAT                                    | FLOAT               |
| BINARY_DOUBLE                                             | DOUBLE              |
| CHAR<br/>NCHAR<br/>NVARCHAR2<br/>NCLOB<br/>CLOB<br/>ROWID | STRING              |
| DATE                                                      | DATE                |
| TIMESTAMP<br/>TIMESTAMP WITH LOCAL TIME ZONE              | TIMESTAMP           |
| BLOB<br/>RAW<br/>LONG RAW<br/>BFILE                       | BYTES               |
| UNKNOWN                                                   | Not supported yet   |

## Source Options

|             Name             |  Type  | Required |     Default     |                                                                                                                              Description                                                                                                                              |
|------------------------------|--------|----------|-----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                          | String | Yes      | -               | The URL of the JDBC connection. Refer to a case: jdbc:oceanbase://localhost:2883/test                                                                                                                                                                                 |
| driver                       | String | Yes      | -               | The jdbc class name used to connect to the remote data source, should be `com.oceanbase.jdbc.Driver`.                                                                                                                                                                 |
| user                         | String | No       | -               | Connection instance user name                                                                                                                                                                                                                                         |
| password                     | String | No       | -               | Connection instance password                                                                                                                                                                                                                                          |
| compatible_mode              | String | Yes      | -               | The compatible mode of OceanBase, can be 'mysql' or 'oracle'.                                                                                                                                                                                                         |
| query                        | String | Yes      | -               | Query statement                                                                                                                                                                                                                                                       |
| connection_check_timeout_sec | Int    | No       | 30              | The time in seconds to wait for the database operation used to validate the connection to complete                                                                                                                                                                    |
| partition_column             | String | No       | -               | The column name for parallelism's partition, only support numeric type column and string type column.                                                                                                                                                                 |
| partition_lower_bound        | Long   | No       | -               | The partition_column min value for scan, if not set SeaTunnel will query database get min value.                                                                                                                                                                      |
| partition_upper_bound        | Long   | No       | -               | The partition_column max value for scan, if not set SeaTunnel will query database get max value.                                                                                                                                                                      |
| partition_num                | Int    | No       | job parallelism | The number of partition count, only support positive integer. Default value is job parallelism.                                                                                                                                                                       |
| fetch_size                   | Int    | No       | 0               | For queries that return a large number of objects, you can configure <br/> the row fetch size used in the query to improve performance by <br/> reducing the number database hits required to satisfy the selection criteria.<br/> Zero means use jdbc default value. |
| common-options               |        | No       | -               | Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details                                                                                                                                                               |

### Tips

> If partition_column is not set, it will run in single concurrency, and if partition_column is set, it will be executed  in parallel according to the concurrency of tasks.

## Task Example

### Simple:

```
env {
  execution.parallelism = 2
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = "com.oceanbase.jdbc.Driver"
    url = "jdbc:oceanbase://localhost:2883/test?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true"
    user = "root"
    password = ""
    compatible_mode = "mysql"
    query = "select * from source"
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

### Parallel:

> Read your query table in parallel with the shard field you configured and the shard data. You can do this if you want to read the whole table

```
source {
  Jdbc {
    driver = "com.oceanbase.jdbc.Driver"
    url = "jdbc:oceanbase://localhost:2883/test?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true"
    user = "root"
    password = ""
    compatible_mode = "mysql"
    query = "select * from source"
    # Parallel sharding reads fields
    partition_column = "id"
    # Number of fragments
    partition_num = 10
  }
}
```

### Parallel Boundary:

> It is more efficient to read your data source according to the upper and lower boundaries you configured

```
source {
  Jdbc {
    driver = "com.oceanbase.jdbc.Driver"
    url = "jdbc:oceanbase://localhost:2883/test?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true"
    user = "root"
    password = ""
    compatible_mode = "mysql"
    query = "select * from source"
    partition_column = "id"
    partition_num = 10
    # Read start boundary
    partition_lower_bound = 1
    # Read end boundary
    partition_upper_bound = 500
  }
}
```

