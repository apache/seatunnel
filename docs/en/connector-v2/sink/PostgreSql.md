# PostgreSql

> JDBC PostgreSql Sink Connector

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

|                   name                    |  type   | required | default value |                                                                                                                 description                                                                                                                  |
|-------------------------------------------|---------|----------|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String  | Yes      | -             | The URL of the JDBC connection. Refer to a case: jdbc:postgresql://localhost:5432/test                                                                                                                                                       |
| driver                                    | String  | Yes      | -             | The jdbc class name used to connect to the remote data source,<br/> if you use PostgreSQL the value is `org.postgresql.Driver`.                                                                                                              |
| user                                      | String  | No       | -             | Connection instance user name                                                                                                                                                                                                                |
| password                                  | String  | No       | -             | Connection instance password                                                                                                                                                                                                                 |
| query                                     | String  | No       | -             | Use this sql write upstream input datas to database. e.g `INSERT ...`,`query` have the higher priority                                                                                                                                       |
| database                                  | String  | No       | -             | Use this `database` and `table-name` auto-generate sql and receive upstream input datas write to database.<br/>This option is mutually exclusive with `query` and has a higher priority.                                                     |
| table                                     | String  | No       | -             | Use database and this table-name auto-generate sql and receive upstream input datas write to database.<br/>This option is mutually exclusive with `query` and has a higher priority.                                                         |
| primary_keys                              | Array   | No       | -             | This option is used to support operations such as `insert`, `delete`, and `update` when automatically generate sql.                                                                                                                          |
| support_upsert_by_query_primary_key_exist | Boolean | No       | false         | Choose to use INSERT sql, UPDATE sql to process update events(INSERT, UPDATE_AFTER) based on query primary key exists. This configuration is only used when database unsupport upsert syntax. **Note**: that this method has low performance |
| connection_check_timeout_sec              | Int     | No       | 30            | The time in seconds to wait for the database operation used to validate the connection to complete.                                                                                                                                          |
| max_retries                               | Int     | No       | 0             | The number of retries to submit failed (executeBatch)                                                                                                                                                                                        |
| batch_size                                | Int     | No       | 1000          | For batch writing, when the number of buffered records reaches the number of `batch_size` or the time reaches `batch_interval_ms`<br/>, the data will be flushed into the database                                                           |
| batch_interval_ms                         | Int     | No       | 1000          | For batch writing, when the number of buffers reaches the number of `batch_size` or the time reaches `batch_interval_ms`, the data will be flushed into the database                                                                         |
| is_exactly_once                           | Boolean | No       | false         | Whether to enable exactly-once semantics, which will use Xa transactions. If on, you need to<br/>set `xa_data_source_class_name`.                                                                                                            |
| xa_data_source_class_name                 | String  | No       | -             | The xa data source class name of the database Driver, for example, PostgreSQL is `org.postgresql.xa.PGXADataSource`, and<br/>please refer to appendix for other data sources                                                                 |
| max_commit_attempts                       | Int     | No       | 3             | The number of retries for transaction commit failures                                                                                                                                                                                        |
| transaction_timeout_sec                   | Int     | No       | -1            | The timeout after the transaction is opened, the default is -1 (never timeout). Note that setting the timeout may affect<br/>exactly-once semantics                                                                                          |
| auto_commit                               | Boolean | No       | true          | Automatic transaction commit is enabled by default                                                                                                                                                                                           |
| common-options                            |         | no       | -             | Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details                                                                                                                                          |

## tips

> If partition_column is not set, it will run in single concurrency, and if partition_column is set, it will be executed  in parallel according to the concurrency of tasks.

## Task Example

### simple:

> This example defines a SeaTunnel synchronization task that automatically generates data through FakeSource and sends it to JDBC Sink. FakeSource generates a total of 16 rows of data (row.num=16), with each row having two fields, name (string type) and age (int type). The final target table is test_table will also be 16 rows of data in the table. Before run this job, you need create database test and table test_table in your PostgreSQL. And if you have not yet installed and deployed SeaTunnel, you need to follow the instructions in [Install SeaTunnel](../../start-v2/locally/deployment.md) to install and deploy SeaTunnel. And then follow the instructions in [Quick Start With SeaTunnel Engine](../../start-v2/locally/quick-start-seatunnel-engine.md) to run this job.

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
        url = "jdbc:postgresql://localhost:5432/test"
        driver = "org.postgresql.Driver"
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
sink {
    jdbc {
        url = "jdbc:postgresql://localhost:5432/test"
        driver = "org.postgresql.Driver"
    
        max_retries = 0
        user = "root"
        password = "123456"
        query = "insert into test_table(name,age) values(?,?)"
    
        is_exactly_once = "true"
    
        xa_data_source_class_name = "org.postgresql.xa.PGXADataSource"
    }
}
```

### CDC(Change data capture) event

> CDC change data is also supported by us In this case, you need config database, table and primary_keys.

```
sink {
    jdbc {
        url = "jdbc:postgresql://localhost:5432/test"
        driver = "org.postgresql.Driver"
        user = "root"
        password = "123456"
        
        generate_sink_sql = true
        # You need to configure both database and table
        database = test
        table = sink_table
        primary_keys = ["id","name"]
    }
}
```

