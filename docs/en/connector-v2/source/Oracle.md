# Oracle

> JDBC Oracle Source Connector

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

> supports query SQL and can achieve projection effect.

## Description

Read external data source data through JDBC.

## Supported DataSource Info

| Datasource |                    Supported versions                    |          Driver          |                  Url                   |                               Maven                                |
|------------|----------------------------------------------------------|--------------------------|----------------------------------------|--------------------------------------------------------------------|
| Oracle     | Different dependency version has different driver class. | oracle.jdbc.OracleDriver | jdbc:oracle:thin:@datasource01:1523:xe | https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8 |

## Database Dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATNUNNEL_HOME/plugins/jdbc/lib/' working directory<br/>
> For example Oracle datasource: cp ojdbc8-xxxxxx.jar $SEATNUNNEL_HOME/lib/<br/>
> To support the i18n character set, copy the orai18n.jar to the $SEATNUNNEL_HOME/lib/ directory.

## Data Type Mapping

|                                   Oracle Data type                                   | SeaTunnel Data type |
|--------------------------------------------------------------------------------------|---------------------|
| INTEGER                                                                              | INT                 |
| FLOAT                                                                                | DECIMAL(38, 18)     |
| NUMBER(precision <= 9, scale == 0)                                                   | INT                 |
| NUMBER(9 < precision <= 18, scale == 0)                                              | BIGINT              |
| NUMBER(18 < precision, scale == 0)                                                   | DECIMAL(38, 0)      |
| NUMBER(scale != 0)                                                                   | DECIMAL(38, 18)     |
| BINARY_DOUBLE                                                                        | DOUBLE              |
| BINARY_FLOAT<br/>REAL                                                                | FLOAT               |
| CHAR<br/>NCHAR<br/>NVARCHAR2<br/>VARCHAR2<br/>LONG<br/>ROWID<br/>NCLOB<br/>CLOB<br/> | STRING              |
| DATE                                                                                 | DATE                |
| TIMESTAMP<br/>TIMESTAMP WITH LOCAL TIME ZONE                                         | TIMESTAMP           |
| BLOB<br/>RAW<br/>LONG RAW<br/>BFILE                                                  | BYTES               |

## Source Options

|             Name             |    Type    | Required |     Default     |                                                                                                                            Description                                                                                                                            |
|------------------------------|------------|----------|-----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                          | String     | Yes      | -               | The URL of the JDBC connection. Refer to a case: jdbc:oracle:thin:@datasource01:1523:xe                                                                                                                                                                           |
| driver                       | String     | Yes      | -               | The jdbc class name used to connect to the remote data source,<br/> if you use MySQL the value is `oracle.jdbc.OracleDriver`.                                                                                                                                     |
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

### Tips

> If partition_column is not set, it will run in single concurrency, and if partition_column is set, it will be executed  in parallel according to the concurrency of tasks.

## Task Example

### Simple:

> This example queries type_bin 'table' 16 data in your test "database" in single parallel and queries all of its fields. You can also specify which fields to query for final output to the console.

```
# Defining the runtime environment
env {
  # You can set flink configuration here
  execution.parallelism = 2
  job.mode = "BATCH"
}
source{
    Jdbc {
        url = "jdbc:oracle:thin:@datasource01:1523:xe"
        driver = "oracle.jdbc.OracleDriver"
        user = "root"
        password = "123456"
        query = "SELECT * FROM TEST_TABLE"
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

> Read your query table in parallel with the shard field you configured and the shard data  You can do this if you want to read the whole table

```
source {
    Jdbc {
        url = "jdbc:oracle:thin:@datasource01:1523:xe"
        driver = "oracle.jdbc.OracleDriver"
        connection_check_timeout_sec = 100
        user = "root"
        password = "123456"
        # Define query logic as required
        query = "SELECT * FROM TEST_TABLE"
        # Parallel sharding reads fields
        partition_column = "ID"
        # Number of fragments
        partition_num = 10
    }
}
```

### Parallel Boundary:

> It is more efficient to specify the data within the upper and lower bounds of the query It is more efficient to read your data source according to the upper and lower boundaries you configured

```
source {
    Jdbc {
        url = "jdbc:oracle:thin:@datasource01:1523:xe"
        driver = "oracle.jdbc.OracleDriver"
        connection_check_timeout_sec = 100
        user = "root"
        password = "123456"
        # Define query logic as required
        query = "SELECT * FROM TEST_TABLE"
        partition_column = "ID"
        # Read start boundary
        partition_lower_bound = 1
        # Read end boundary
        partition_upper_bound = 500
        partition_num = 10
    }
}
```

