# Kingbase

> JDBC Kingbase Sink Connector

## Support Connector Version

- 8.6

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)

## Description

> Use `Xa transactions` to ensure `exactly-once`. So only support `exactly-once` for the database which is
> support `Xa transactions`. You can set `is_exactly_once=true` to enable it.

## Supported DataSource Info

| Datasource | Supported versions |        Driver        |                   Url                    |                                             Maven                                              |
|------------|--------------------|----------------------|------------------------------------------|------------------------------------------------------------------------------------------------|
| Kingbase   | 8.6                | com.kingbase8.Driver | jdbc:kingbase8://localhost:54321/db_test | [Download](https://repo1.maven.org/maven2/cn/com/kingbase/kingbase8/8.6.0/kingbase8-8.6.0.jar) |

## Database Dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATNUNNEL_HOME/plugins/jdbc/lib/' working directory<br/>
> For example: cp kingbase8-8.6.0.jar $SEATNUNNEL_HOME/plugins/jdbc/lib/

## Data Type Mapping

|             Kingbase Data type             |                                                                SeaTunnel Data type                                                                |
|--------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| BOOL                                       | BOOLEAN                                                                                                                                           |
| INT2                                       | SHORT                                                                                                                                             |
| SMALLSERIAL</br> SERIAL </br>INT4          | INT                                                                                                                                               |
| INT8 </br>   BIGSERIAL                     | BIGINT                                                                                                                                            |
| FLOAT4                                     | FLOAT                                                                                                                                             |
| FLOAT8                                     | DOUBLE                                                                                                                                            |
| NUMERIC                                    | DECIMAL((Get the designated column's specified column size),<br/>(Gets the designated column's number of digits to right of the decimal point.))) |
| BPCHAR<br/>CHARACTER<br/> VARCHAR<br/>TEXT | STRING                                                                                                                                            |
| TIMESTAMP                                  | LOCALDATETIME                                                                                                                                     |
| TIME                                       | LOCALTIME                                                                                                                                         |
| DATE                                       | LOCALDATE                                                                                                                                         |
| Other Data type                            | Not supported yet                                                                                                                                 |

## Source Options

|             Name             |    Type    | Required |     Default     |                                                                                                                              Description                                                                                                                              |
|------------------------------|------------|----------|-----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                          | String     | Yes      | -               | The URL of the JDBC connection. Refer to a case: jdbc:kingbase8://localhost:54321/test                                                                                                                                                                                |
| driver                       | String     | Yes      | -               | The jdbc class name used to connect to the remote data source, should be `com.kingbase8.Driver`.                                                                                                                                                                      |
| user                         | String     | No       | -               | Connection instance user name                                                                                                                                                                                                                                         |
| password                     | String     | No       | -               | Connection instance password                                                                                                                                                                                                                                          |
| query                        | String     | Yes      | -               | Query statement                                                                                                                                                                                                                                                       |
| connection_check_timeout_sec | Int        | No       | 30              | The time in seconds to wait for the database operation used to validate the connection to complete                                                                                                                                                                    |
| partition_column             | String     | No       | -               | The column name for parallelism's partition, only support numeric type column and string type column.                                                                                                                                                                 |
| partition_lower_bound        | BigDecimal | No       | -               | The partition_column min value for scan, if not set SeaTunnel will query database get min value.                                                                                                                                                                      |
| partition_upper_bound        | BigDecimal | No       | -               | The partition_column max value for scan, if not set SeaTunnel will query database get max value.                                                                                                                                                                      |
| partition_num                | Int        | No       | job parallelism | The number of partition count, only support positive integer. Default value is job parallelism.                                                                                                                                                                       |
| fetch_size                   | Int        | No       | 0               | For queries that return a large number of objects, you can configure <br/> the row fetch size used in the query to improve performance by <br/> reducing the number database hits required to satisfy the selection criteria.<br/> Zero means use jdbc default value. |
| common-options               |            | No       | -               | Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details                                                                                                                                                               |

### Tips

> If partition_column is not set, it will run in single concurrency, and if partition_column is set, it will be executed  in parallel according to the concurrency of tasks.

## Task Example

### Simple:

> This example defines a SeaTunnel synchronization task that automatically generates data through FakeSource and sends it to JDBC Sink. FakeSource generates a total of 16 rows of data (row.num=16), with each row having two fields, name (string type) and age (int type). The final target table is test_table will also be 16 rows of data in the table. Before run this job, you need create database test and table test_table in your DB2. And if you have not yet installed and deployed SeaTunnel, you need to follow the instructions in [Install SeaTunnel](../../start-v2/locally/deployment.md) to install and deploy SeaTunnel. And then follow the instructions in [Quick Start With SeaTunnel Engine](../../start-v2/locally/quick-start-seatunnel-engine.md) to run this job.

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
        url = "jdbc:kingbase8://127.0.0.1:54321/dbname"
        driver = "com.kingbase8.Driver"
        user = "root"
        password = "123456"
        query = "insert into test_table(name,age) values(?,?)"
        }
  # If you would like to get more information about how to configure seatunnel and see full list of sink plugins,
  # please go to https://seatunnel.apache.org/docs/category/sink-v2
}
```

### Generate Sink SQL

> This example  not need to write complex sql statements, you can configure the database name table name to automatically generate add statements for you

```
sink {
    jdbc {
        url = "jdbc:kingbase8://127.0.0.1:54321/dbname"
        driver = "com.kingbase8.Driver"
        user = "root"
        password = "123456"
        # Automatically generate sql statements based on database table names
        generate_sink_sql = true
        database = test
        table = test_table
    }
}
```

### Exactly-once :

> For accurate write scene we guarantee accurate once

```
sink {
    jdbc {
        url = "jdbc:kingbase8://127.0.0.1:54321/dbname"
        driver = "com.kingbase8.Driver"
    
        max_retries = 0
        user = "root"
        password = "123456"
        query = "insert into test_table(name,age) values(?,?)"
    
        is_exactly_once = "true"
    
        xa_data_source_class_name = "com.kingbase8.xa.KBXADataSource"
    }
}
```

