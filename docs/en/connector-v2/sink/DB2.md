# DB2

> JDBC DB2 Sink Connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Write data through jdbc. Support Batch mode and Streaming mode, support concurrent writing, support exactly-once
semantics (using XA transaction guarantee).

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Key Features

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)

> Use `Xa transactions` to ensure `exactly-once`. So only support `exactly-once` for the database which is
> support `Xa transactions`. You can set `is_exactly_once=true` to enable it.

## Supported DataSource Info

| Datasource |                    Supported Versions                    |             Driver             |                Url                |                                 Maven                                 |
|------------|----------------------------------------------------------|--------------------------------|-----------------------------------|-----------------------------------------------------------------------|
| DB2        | Different dependency version has different driver class. | com.ibm.db2.jdbc.app.DB2Driver | jdbc:db2://127.0.0.1:50000/dbname | [Download](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc) |

## Data Type Mapping

|                                            DB2 Data Type                                             | SeaTunnel Data Type |
|------------------------------------------------------------------------------------------------------|---------------------|
| BOOLEAN                                                                                              | BOOLEAN             |
| SMALLINT                                                                                             | SHORT               |
| INT<br/>INTEGER<br/>                                                                                 | INTEGER             |
| BIGINT                                                                                               | LONG                |
| DECIMAL<br/>DEC<br/>NUMERIC<br/>NUM                                                                  | DECIMAL(38,18)      |
| REAL                                                                                                 | FLOAT               |
| FLOAT<br/>DOUBLE<br/>DOUBLE PRECISION<br/>DECFLOAT                                                   | DOUBLE              |
| CHAR<br/>VARCHAR<br/>LONG VARCHAR<br/>CLOB<br/>GRAPHIC<br/>VARGRAPHIC<br/>LONG VARGRAPHIC<br/>DBCLOB | STRING              |
| BLOB                                                                                                 | BYTES               |
| DATE                                                                                                 | DATE                |
| TIME                                                                                                 | TIME                |
| TIMESTAMP                                                                                            | TIMESTAMP           |
| ROWID<br/>XML                                                                                        | Not supported yet   |

## Sink Options

|                   Name                    |  Type   | Required | Default |                                                                                                                  Description                                                                                                                   |
|-------------------------------------------|---------|----------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String  | Yes      | -       | The URL of the JDBC connection. Refer to a case: jdbc:db2://127.0.0.1:50000/dbname                                                                                                                                                             |
| driver                                    | String  | Yes      | -       | The jdbc class name used to connect to the remote data source,<br/> if you use DB2 the value is `com.ibm.db2.jdbc.app.DB2Driver`.                                                                                                              |
| user                                      | String  | No       | -       | Connection instance user name                                                                                                                                                                                                                  |
| password                                  | String  | No       | -       | Connection instance password                                                                                                                                                                                                                   |
| query                                     | String  | No       | -       | Use this sql write upstream input datas to database. e.g `INSERT ...`,`query` have the higher priority                                                                                                                                         |
| database                                  | String  | No       | -       | Use this `database` and `table-name` auto-generate sql and receive upstream input datas write to database.<br/>This option is mutually exclusive with `query` and has a higher priority.                                                       |
| table                                     | String  | No       | -       | Use database and this table-name auto-generate sql and receive upstream input datas write to database.<br/>This option is mutually exclusive with `query` and has a higher priority.                                                           |
| primary_keys                              | Array   | No       | -       | This option is used to support operations such as `insert`, `delete`, and `update` when automatically generate sql.                                                                                                                            |
| support_upsert_by_query_primary_key_exist | Boolean | No       | false   | Choose to use INSERT sql, UPDATE sql to process update events(INSERT, UPDATE_AFTER) based on query primary key exists. This configuration is only used when database unsupport upsert syntax. **Note**: that this method has low performance   |
| connection_check_timeout_sec              | Int     | No       | 30      | The time in seconds to wait for the database operation used to validate the connection to complete.                                                                                                                                            |
| max_retries                               | Int     | No       | 0       | The number of retries to submit failed (executeBatch)                                                                                                                                                                                          |
| batch_size                                | Int     | No       | 1000    | For batch writing, when the number of buffered records reaches the number of `batch_size` or the time reaches `checkpoint.interval`<br/>, the data will be flushed into the database                                                           |
| is_exactly_once                           | Boolean | No       | false   | Whether to enable exactly-once semantics, which will use Xa transactions. If on, you need to<br/>set `xa_data_source_class_name`.                                                                                                              |
| generate_sink_sql                         | Boolean | No       | false   | Generate sql statements based on the database table you want to write to                                                                                                                                                                       |
| xa_data_source_class_name                 | String  | No       | -       | The xa data source class name of the database Driver, for example, DB2 is `com.db2.cj.jdbc.Db2XADataSource`, and<br/>please refer to appendix for other data sources                                                                           |
| max_commit_attempts                       | Int     | No       | 3       | The number of retries for transaction commit failures                                                                                                                                                                                          |
| transaction_timeout_sec                   | Int     | No       | -1      | The timeout after the transaction is opened, the default is -1 (never timeout). Note that setting the timeout may affect<br/>exactly-once semantics                                                                                            |
| auto_commit                               | Boolean | No       | true    | Automatic transaction commit is enabled by default                                                                                                                                                                                             |
| properties                                | Map     | No       | -       | Additional connection configuration parameters,when properties and URL have the same parameters, the priority is determined by the <br/>specific implementation of the driver. For example, in MySQL, properties take precedence over the URL. |
| common-options                            |         | no       | -       | Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details                                                                                                                                    |

### Tips

> If partition_column is not set, it will run in single concurrency, and if partition_column is set, it will be executed  in parallel according to the concurrency of tasks.

## Task Example

### Simple:

> This example defines a SeaTunnel synchronization task that automatically generates data through FakeSource and sends it to JDBC Sink. FakeSource generates a total of 16 rows of data (row.num=16), with each row having two fields, name (string type) and age (int type). The final target table is test_table will also be 16 rows of data in the table. Before run this job, you need create database test and table test_table in your DB2. And if you have not yet installed and deployed SeaTunnel, you need to follow the instructions in [Install SeaTunnel](../../start-v2/locally/deployment.md) to install and deploy SeaTunnel. And then follow the instructions in [Quick Start With SeaTunnel Engine](../../start-v2/locally/quick-start-seatunnel-engine.md) to run this job.

```
# Defining the runtime environment
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  # This is a example source plugin **only for test and demonstrate the feature source plugin**
  FakeSource {
    parallelism = 1
    plugin_output = "fake"
    row.num = 16
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
  # If you would like to get more information about how to configure seatunnel and see full list of source plugins,
  # please go to https://seatunnel.apache.org/docs/connector-v2/source
}

transform {
  # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/transform-v2
}

sink {
    jdbc {
        url = "jdbc:db2://127.0.0.1:50000/dbname"
        driver = "com.ibm.db2.jdbc.app.DB2Driver"
        user = "root"
        password = "123456"
        query = "insert into test_table(name,age) values(?,?)"
        }
  # If you would like to get more information about how to configure seatunnel and see full list of sink plugins,
  # please go to https://seatunnel.apache.org/docs/connector-v2/sink
}
```

### Generate Sink SQL

> This example  not need to write complex sql statements, you can configure the database name table name to automatically generate add statements for you

```
sink {
    jdbc {
        url = "jdbc:db2://127.0.0.1:50000/dbname"
        driver = "com.ibm.db2.jdbc.app.DB2Driver"
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
        url = "jdbc:db2://127.0.0.1:50000/dbname"
        driver = "com.ibm.db2.jdbc.app.DB2Driver"
    
        max_retries = 0
        user = "root"
        password = "123456"
        query = "insert into test_table(name,age) values(?,?)"
    
        is_exactly_once = "true"
    
        xa_data_source_class_name = "com.db2.cj.jdbc.Db2XADataSource"
    }
}
```

