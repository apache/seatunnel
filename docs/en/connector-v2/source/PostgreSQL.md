# PostgreSQL

> JDBC PostgreSQL Source Connector

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

| datasource |                    supported versions                    |        driver         |                  url                  |                                  maven                                   |
|------------|----------------------------------------------------------|-----------------------|---------------------------------------|--------------------------------------------------------------------------|
| PostgreSQL | Different dependency version has different driver class. | org.postgresql.Driver | jdbc:postgresql://localhost:5432/test | [Download](https://mvnrepository.com/artifact/org.postgresql/postgresql) |

## Database dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATNUNNEL_HOME/plugins/jdbc/lib/' working directory<br/>
> For example PostgreSQL datasource: cp postgresql-xxx.jar $SEATNUNNEL_HOME/plugins/jdbc/lib/

## Data Type Mapping

|                     PostgreSQL Data type                     |                                                          Seatunnel Data type                                                           |
|--------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| bool<br/>                                                    | BOOLEAN                                                                                                                                |
| _bool<br/>                                                   | ARRAY&lt;BOOLEAN&gt;                                                                                                                   |
| bytea<br/>                                                   | BYTES                                                                                                                                  |
| _bytea<br/>                                                  | ARRAY&lt;TINYINT&gt;                                                                                                                   |
| int2<br/>smallserial<br/>int4<br/>serial<br/>                | INT                                                                                                                                    |
| _int2<br/>_int4<br/>                                         | ARRAY&lt;INT&gt;                                                                                                                       |
| int8<br/>bigserial<br/>                                      | BIGINT                                                                                                                                 |
| _int8<br/>                                                   | ARRAY&lt;BIGINT&gt;                                                                                                                    |
| float4<br/>                                                  | FLOAT                                                                                                                                  |
| _float4<br/>                                                 | ARRAY&lt;FLOAT&gt;                                                                                                                     |
| float8<br/>                                                  | DOUBLE                                                                                                                                 |
| _float8<br/>                                                 | ARRAY&lt;DOUBLE&gt;                                                                                                                    |
| numeric(Get the designated column's specified column size>0) | DECIMAL(Get the designated column's specified column size,Gets the designated column's number of digits to right of the decimal point) |
| numeric(Get the designated column's specified column size<0) | DECIMAL(38, 18)                                                                                                                        |
| bpchar<br/>character<br/>varchar<br/>text                    | STRING                                                                                                                                 |
| _bpchar<br/>_character<br/>_varchar<br/>_text                | ARRAY&lt;STRING&gt;                                                                                                                    |
| timestamp<br/>                                               | TIMESTAMP                                                                                                                              |
| time<br/>                                                    | TIME                                                                                                                                   |
| date<br/>                                                    | DATE                                                                                                                                   |
| Other Data types                                             | Not supported yet                                                                                                                      |

## Options

|             name             |  type  | required |     default     |                                                                                                                            description                                                                                                                            |
|------------------------------|--------|----------|-----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                          | String | Yes      | -               | The URL of the JDBC connection. Refer to a case: jdbc:postgresql://localhost:5432/test                                                                                                                                                                            |
| driver                       | String | Yes      | -               | The jdbc class name used to connect to the remote data source,<br/> if you use PostgreSQL the value is `org.postgresql.Driver`.                                                                                                                                   |
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

### parallel boundary:

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

