# Redshift

> JDBC Redshift sink Connector

## Support those engines

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## Key features

- [ ] [batch](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

> Use `Xa transactions` to ensure `exactly-once`. So only support `exactly-once` for the database which is support `Xa transactions`. You can set `is_exactly_once=true` to enable it.

## Description

Write data through jdbc.

## Supported DataSource list

| datasource |                    supported versions                    |          driver          | url                                     |                                   maven                                   |
|------------|----------------------------------------------------------|--------------------------|-----------------------------------------|---------------------------------------------------------------------------|
| redshift   | Different dependency version has different driver class. | com.amazon.redshift.jdbc.Driver | jdbc:redshift://localhost:5439/database | [Download](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) |

## Database dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATNUNNEL_HOME/plugins/jdbc/lib/' working directory<br/>
> For example Redshift datasource: cp RedshiftJDBC42-xxx.jar $SEATNUNNEL_HOME/plugins/jdbc/lib/

## Data Type Mapping

| Redshift Data type                                           | Seatunnel Data type                                          |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| SMALLINT<br />INT2                                           | SHORT                                                        |
| INTEGER<br />INT<br />INT4                                   | INT                                                          |
| BIGINT<br />INT8<br />OID                                    | LONG                                                         |
| DECIMAL<br />NUMERIC                                         | DECIMAL((Get the designated column's specified column size)+1,<br/>(Gets the designated column's number of digits to right of the decimal point.))) |
| REAL<br />FLOAT4                                             | FLOAT                                                        |
| DOUBLE_PRECISION<br />FLOAT8<br />FLOAT                      | DOUBLE                                                       |
| BOOLEAN<br />BOOL                                            | BOOLEAN                                                      |
| CHAR<br />CHARACTER<br />NCHAR<br />BPCHAR<br />VARCHAR<br />CHARACTER_VARYING<br />NVARCHAR<br />TEXT<br />SUPER | STRING                                                       |
| DATE                                                         | LOCALDATE                                                    |
| TIME<br />TIME_WITH_TIME_ZONE<br />TIMETZ                    | LOCALTIME                                                    |
| TIMESTAMP<br />TIMESTAMP_WITH_OUT_TIME_ZONE<br />TIMESTAMPTZ | LOCALDATETIME                                                |
| GEOMETRY                                                     | Not supported yet                                            |

## Options

| name                         | type   | required | default         | description                                                  |
| ---------------------------- | ------ | -------- | --------------- | ------------------------------------------------------------ |
| url                          | String | Yes      | -               | The URL of the JDBC connection. Refer to a case: jdbc:redshift://localhost:5439/database |
| driver                       | String | Yes      | -               | The jdbc class name used to connect to the remote data source,<br/> if you use Redshift the value is `com.amazon.redshift.jdbc.Driver`. |
| user                         | String | No       | -               | Connection instance user name                                |
| password                     | String | No       | -               | Connection instance password                                 |
| query                        | String | Yes      | -               | Query statement                                              |
| connection_check_timeout_sec | Int    | No       | 30              | The time in seconds to wait for the database operation used to validate the connection to complete |
| partition_column             | String | No       | -               | The column name for parallelism's partition, only support numeric type,Only support numeric type primary key, and only can config one column. |
| partition_lower_bound        | Long   | No       | -               | The partition_column min value for scan, if not set SeaTunnel will query database get min value. |
| partition_upper_bound        | Long   | No       | -               | The partition_column max value for scan, if not set SeaTunnel will query database get max value. |
| partition_num                | Int    | No       | job parallelism | The number of partition count, only support positive integer. default value is job parallelism |
| fetch_size                   | Int    | No       | 0               | For queries that return a large number of objects,you can configure<br/> the row fetch size used in the query toimprove performance by<br/> reducing the number database hits required to satisfy the selection criteria.<br/> Zero means use jdbc default value. |
| common-options               |        | No       | -               | Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details |

## tips

> If partition_column is not set, it will run in single concurrency, and if partition_column is set, it will be executed  in parallel according to the concurrency of tasks.

## Task Example

### simple:

> This example defines a SeaTunnel synchronization task that automatically generates data through FakeSource and sends it to JDBC Sink. FakeSource generates a total of 16 rows of data (row.num=16), with each row having two fields, name (string type) and age (int type). The final target table is test_table will also be 16 rows of data in the table. Before run this job, you need create database mydatabase and table test_table in your Redshift. And if you have not yet installed and deployed SeaTunnel, you need to follow the instructions in [Install SeaTunnel](https://github.com/apache/incubator-seatunnel/blob/d013b5d2ac79f567077b62ceb4d247abf805ffdf/docs/en/start-v2/locally/deployment.md) to install and deploy SeaTunnel. And then follow the instructions in [Quick Start With SeaTunnel Engine](https://github.com/apache/incubator-seatunnel/blob/d013b5d2ac79f567077b62ceb4d247abf805ffdf/docs/en/start-v2/locally/quick-start-seatunnel-engine.md) to run this job.

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
        url = "jdbc:redshift://localhost:5439/mydatabase"
        driver = "com.mysql.cj.jdbc.Driver"
        user = "myUser"
        password = "myPassword"
        query = "insert into test_table(name,age) values(?,?)"
        }
  # If you would like to get more information about how to configure seatunnel and see full list of sink plugins,
  # please go to https://seatunnel.apache.org/docs/category/sink-v2
}
```

### Exactly-once :

> For accurate write scene we guarantee accurate once

```
sink {
  jdbc {
    url = "jdbc:redshift://localhost:5439/mydatabase"
    driver = "com.amazon.redshift.jdbc.Driver"
    user = "myUser"
    password = "myPassword"
    max_retries = 0
    query = "insert into mytable(name,age) values(?,?)"
    is_exactly_once = true
    xa_data_source_class_name = "com.amazon.redshift.jdbc.RedshiftXADataSource"
  }
}
```

### CDC(Change data capture) event

> CDC change data is also supported by us In this case, you need config database, table and primary_keys.

```
sink {
    jdbc {
        url = "jdbc:redshift://localhost:5439/mydatabase"
        driver = "com.amazon.redshift.jdbc.Driver"
        user = "myUser"
        password = "mypassword"
        
        generate_sink_sql = true
        # You need to configure both schema and table
        schema = "public"
        table = "sink_table"
        primary_keys = ["id","name"]
    }
}
```
