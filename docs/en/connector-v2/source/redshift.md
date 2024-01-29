# Redshift

> JDBC Redshift Source Connector

## Support those engines

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

> supports query SQL and can achieve projection effect.

## Description

Read external data source data through JDBC.

## Supported DataSource list

| datasource |                    supported versions                    |             driver              |                   url                   |                                       maven                                        |
|------------|----------------------------------------------------------|---------------------------------|-----------------------------------------|------------------------------------------------------------------------------------|
| redshift   | Different dependency version has different driver class. | com.amazon.redshift.jdbc.Driver | jdbc:redshift://localhost:5439/database | [Download](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) |

## Database dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATNUNNEL_HOME/plugins/jdbc/lib/' working directory<br/>
> For example Redshift datasource: cp RedshiftJDBC42-xxx.jar $SEATNUNNEL_HOME/plugins/jdbc/lib/

## Data Type Mapping

|                                                Redshift Data type                                                 |                                                                 Seatunnel Data type                                                                 |
|-------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| SMALLINT<br />INT2                                                                                                | SHORT                                                                                                                                               |
| INTEGER<br />INT<br />INT4                                                                                        | INT                                                                                                                                                 |
| BIGINT<br />INT8<br />OID                                                                                         | LONG                                                                                                                                                |
| DECIMAL<br />NUMERIC                                                                                              | DECIMAL((Get the designated column's specified column size)+1,<br/>(Gets the designated column's number of digits to right of the decimal point.))) |
| REAL<br />FLOAT4                                                                                                  | FLOAT                                                                                                                                               |
| DOUBLE_PRECISION<br />FLOAT8<br />FLOAT                                                                           | DOUBLE                                                                                                                                              |
| BOOLEAN<br />BOOL                                                                                                 | BOOLEAN                                                                                                                                             |
| CHAR<br />CHARACTER<br />NCHAR<br />BPCHAR<br />VARCHAR<br />CHARACTER_VARYING<br />NVARCHAR<br />TEXT<br />SUPER | STRING                                                                                                                                              |
| DATE                                                                                                              | LOCALDATE                                                                                                                                           |
| TIME<br />TIME_WITH_TIME_ZONE<br />TIMETZ                                                                         | LOCALTIME                                                                                                                                           |
| TIMESTAMP<br />TIMESTAMP_WITH_OUT_TIME_ZONE<br />TIMESTAMPTZ                                                      | LOCALDATETIME                                                                                                                                       |
| GEOMETRY                                                                                                          | Not supported yet                                                                                                                                   |

## Options

|             name             |  type  | required |     default     |                                                                                                                            description                                                                                                                            |
|------------------------------|--------|----------|-----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                          | String | Yes      | -               | The URL of the JDBC connection. Refer to a case: jdbc:redshift://localhost:5439/database                                                                                                                                                                          |
| driver                       | String | Yes      | -               | The jdbc class name used to connect to the remote data source,<br/> if you use Redshift the value is `com.amazon.redshift.jdbc.Driver`.                                                                                                                           |
| user                         | String | No       | -               | Connection instance user name                                                                                                                                                                                                                                     |
| password                     | String | No       | -               | Connection instance password                                                                                                                                                                                                                                      |
| query                        | String | Yes      | -               | Query statement                                                                                                                                                                                                                                                   |
| connection_check_timeout_sec | Int    | No       | 30              | The time in seconds to wait for the database operation used to validate the connection to complete                                                                                                                                                                |
| partition_column             | String | No       | -               | The column name for parallelism's partition, only support numeric type,Only support numeric type primary key, and only can config one column.                                                                                                                     |
| partition_lower_bound        | Long   | No       | -               | The partition_column min value for scan, if not set SeaTunnel will query database get min value.                                                                                                                                                                  |
| partition_upper_bound        | Long   | No       | -               | The partition_column max value for scan, if not set SeaTunnel will query database get max value.                                                                                                                                                                  |
| partition_num                | Int    | No       | job parallelism | The number of partition count, only support positive integer. default value is job parallelism                                                                                                                                                                    |
| fetch_size                   | Int    | No       | 0               | For queries that return a large number of objects,you can configure<br/> the row fetch size used in the query toimprove performance by<br/> reducing the number database hits required to satisfy the selection criteria.<br/> Zero means use jdbc default value. |
| common-options               |        | No       | -               | Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details                                                                                                                                                           |

## tips

> If partition_column is not set, it will run in single concurrency, and if partition_column is set, it will be executed  in parallel according to the concurrency of tasks.

## Task Example

### simple:

> This example queries type_bin 'table' 16 data in your test "database" in single parallel and queries all of its fields. You can also specify which fields to query for final output to the console.

```
# Defining the runtime environment
env {
  execution.parallelism = 2
  job.mode = "BATCH"
}

source{
    Jdbc {
        url = "jdbc:redshift://localhost:5439/database?user=myuser&password=mypassword"
        driver = "com.amazon.redshift.jdbc.Driver"
        query = "SELECT * FROM mytable LIMIT 100"
        connectTimeout = 30
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
source {
  Jdbc {
    url = "jdbc:redshift://localhost:5439/database?user=myuser&password=mypassword"
    driver = "com.amazon.redshift.jdbc.Driver"
    # Define query logic as required
    query = "SELECT * FROM myTable"
    # Parallel sharding reads fields
    partition_column = "id"
    # Number of fragments
    partition_num = 10
  }
}
```

### parallel boundary:

> It is more efficient to specify the data within the upper and lower bounds of the query It is more efficient to read your data source according to the upper and lower boundaries you configured

```
source {
    Jdbc {
        url = "jdbc:redshift://localhost:5439/database?user=myuser&password=mypassword"
        driver = "com.amazon.redshift.jdbc.Driver"
        connectTimeout = 30
        # Define query logic as required
        query = "select * from mytable"
        partition_column = "id"
        # Read start boundary
        partition_lower_bound = 1
        # Read end boundary
        partition_upper_bound = 500
        partition_num = 10
    }
}
```

