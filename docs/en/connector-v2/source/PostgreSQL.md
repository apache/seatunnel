# PostgreSQL

> JDBC PostgreSQL Source Connector

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

| Datasource |                     Supported versions                     |        Driver         |                  Url                  |                                  Maven                                   |
|------------|------------------------------------------------------------|-----------------------|---------------------------------------|--------------------------------------------------------------------------|
| PostgreSQL | Different dependency version has different driver class.   | org.postgresql.Driver | jdbc:postgresql://localhost:5432/test | [Download](https://mvnrepository.com/artifact/org.postgresql/postgresql) |
| PostgreSQL | If you want to manipulate the GEOMETRY type in PostgreSQL. | org.postgresql.Driver | jdbc:postgresql://localhost:5432/test | [Download](https://mvnrepository.com/artifact/net.postgis/postgis-jdbc)  |

## Database Dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATNUNNEL_HOME/plugins/jdbc/lib/' working directory<br/>
> For example PostgreSQL datasource: cp postgresql-xxx.jar $SEATNUNNEL_HOME/plugins/jdbc/lib/<br/>
> If you want to manipulate the GEOMETRY type in PostgreSQL, add postgresql-xxx.jar and postgis-jdbc-xxx.jar to $SEATNUNNEL_HOME/plugins/jdbc/lib/

## Data Type Mapping

|                         PostgreSQL Data type                         |                                                              SeaTunnel Data type                                                               |
|----------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| BOOL<br/>                                                            | BOOLEAN                                                                                                                                        |
| _BOOL<br/>                                                           | ARRAY&LT;BOOLEAN&GT;                                                                                                                           |
| BYTEA<br/>                                                           | BYTES                                                                                                                                          |
| _BYTEA<br/>                                                          | ARRAY&LT;TINYINT&GT;                                                                                                                           |
| INT2<br/>SMALLSERIAL<br/>INT4<br/>SERIAL<br/>                        | INT                                                                                                                                            |
| _INT2<br/>_INT4<br/>                                                 | ARRAY&LT;INT&GT;                                                                                                                               |
| INT8<br/>BIGSERIAL<br/>                                              | BIGINT                                                                                                                                         |
| _INT8<br/>                                                           | ARRAY&LT;BIGINT&GT;                                                                                                                            |
| FLOAT4<br/>                                                          | FLOAT                                                                                                                                          |
| _FLOAT4<br/>                                                         | ARRAY&LT;FLOAT&GT;                                                                                                                             |
| FLOAT8<br/>                                                          | DOUBLE                                                                                                                                         |
| _FLOAT8<br/>                                                         | ARRAY&LT;DOUBLE&GT;                                                                                                                            |
| NUMERIC(Get the designated column's specified column size>0)         | DECIMAL(Get the designated column's specified column size,Gets the number of digits in the specified column to the right of the decimal point) |
| NUMERIC(Get the designated column's specified column size<0)         | DECIMAL(38, 18)                                                                                                                                |
| BPCHAR<br/>CHARACTER<br/>VARCHAR<br/>TEXT<br/>GEOMETRY<br/>GEOGRAPHY | STRING                                                                                                                                         |
| _BPCHAR<br/>_CHARACTER<br/>_VARCHAR<br/>_TEXT                        | ARRAY&LT;STRING&GT;                                                                                                                            |
| TIMESTAMP<br/>                                                       | TIMESTAMP                                                                                                                                      |
| TIME<br/>                                                            | TIME                                                                                                                                           |
| DATE<br/>                                                            | DATE                                                                                                                                           |
| OTHER DATA TYPES                                                     | NOT SUPPORTED YET                                                                                                                              |

## Options

|             Name             |    Type    | Required |     Default     |                                                                                                                            Description                                                                                                                            |
|------------------------------|------------|----------|-----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                          | String     | Yes      | -               | The URL of the JDBC connection. Refer to a case: jdbc:postgresql://localhost:5432/test                                                                                                                                                                            |
| driver                       | String     | Yes      | -               | The jdbc class name used to connect to the remote data source,<br/> if you use PostgreSQL the value is `org.postgresql.Driver`.                                                                                                                                   |
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
        url = "jdbc:postgresql://localhost:5432/test"
        driver = "org.postgresql.Driver"
        user = "root"
        password = "test"
        query = "select * from source limit 16"
    }
}

transform {
    # please go to https://seatunnel.apache.org/docs/transform-v2/sql
}

sink {
    Console {}
}
```

### Parallel:

> Read your query table in parallel with the shard field you configured and the shard data  You can do this if you want to read the whole table

```
source{
    jdbc{
        url = "jdbc:postgresql://localhost:5432/test"
        driver = "org.postgresql.Driver"
        user = "root"
        password = "test"
        query = "select * from source"
        partition_column= "id"
        partition_num = 5
    }
}
```

### Parallel Boundary:

> It is more efficient to specify the data within the upper and lower bounds of the query It is more efficient to read your data source according to the upper and lower boundaries you configured

```
source{
    jdbc{
        url = "jdbc:postgresql://localhost:5432/test"
        driver = "org.postgresql.Driver"
        user = "root"
        password = "test"
        query = "select * from source"
        partition_column= "id"
        
        # The name of the table returned
        result_table_name = "jdbc"
        partition_lower_bound = 1
        partition_upper_bound = 50
        partition_num = 5
    }
}
```

