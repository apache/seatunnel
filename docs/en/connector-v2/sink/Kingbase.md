# Kingbase

> JDBC Kingbase Sink Connector

## Support Connector Version

- 8.6

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)

## Description

> Use `Xa transactions` to ensure `exactly-once`. So only support `exactly-once` for the database which is
> support `Xa transactions`. You can set `is_exactly_once=true` to enable it.Kingbase currently does not support

## Supported DataSource Info

| Datasource | Supported versions |        Driver        |                   Url                    |                                             Maven                                              |
|------------|--------------------|----------------------|------------------------------------------|------------------------------------------------------------------------------------------------|
| Kingbase   | 8.6                | com.kingbase8.Driver | jdbc:kingbase8://localhost:54321/db_test | [Download](https://repo1.maven.org/maven2/cn/com/kingbase/kingbase8/8.6.0/kingbase8-8.6.0.jar) |

## Database Dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATUNNEL_HOME/plugins/jdbc/lib/'
> working directory<br/>
> For example: cp kingbase8-8.6.0.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## Data Type Mapping

|              Kingbase Data Type              |                                                                SeaTunnel Data Type                                                                |
|----------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| BOOL                                         | BOOLEAN                                                                                                                                           |
| INT2                                         | SHORT                                                                                                                                             |
| SMALLSERIAL <br/>SERIAL <br/>INT4            | INT                                                                                                                                               |
| INT8 <br/>BIGSERIAL                          | BIGINT                                                                                                                                            |
| FLOAT4                                       | FLOAT                                                                                                                                             |
| FLOAT8                                       | DOUBLE                                                                                                                                            |
| NUMERIC                                      | DECIMAL((Get the designated column's specified column size),<br/>(Gets the designated column's number of digits to right of the decimal point.))) |
| BPCHAR <br/>CHARACTER <br/>VARCHAR <br/>TEXT | STRING                                                                                                                                            |
| TIMESTAMP                                    | LOCALDATETIME                                                                                                                                     |
| TIME                                         | LOCALTIME                                                                                                                                         |
| DATE                                         | LOCALDATE                                                                                                                                         |
| Other data type                              | Not supported yet                                                                                                                                 |

## Sink Options

|                   Name                    |  Type   | Required | Default |                                                                                                                 Description                                                                                                                  |
|-------------------------------------------|---------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String  | Yes      | -       | The URL of the JDBC connection. Refer to a case: jdbc:db2://127.0.0.1:50000/dbname                                                                                                                                                           |
| driver                                    | String  | Yes      | -       | The jdbc class name used to connect to the remote data source,<br/> if you use DB2 the value is `com.ibm.db2.jdbc.app.DB2Driver`.                                                                                                            |
| user                                      | String  | No       | -       | Connection instance user name                                                                                                                                                                                                                |
| password                                  | String  | No       | -       | Connection instance password                                                                                                                                                                                                                 |
| query                                     | String  | No       | -       | Use this sql write upstream input datas to database. e.g `INSERT ...`,`query` have the higher priority                                                                                                                                       |
| database                                  | String  | No       | -       | Use this `database` and `table-name` auto-generate sql and receive upstream input datas write to database.<br/>This option is mutually exclusive with `query` and has a higher priority.                                                     |
| table                                     | String  | No       | -       | Use database and this table-name auto-generate sql and receive upstream input datas write to database.<br/>This option is mutually exclusive with `query` and has a higher priority.                                                         |
| primary_keys                              | Array   | No       | -       | This option is used to support operations such as `insert`, `delete`, and `update` when automatically generate sql.                                                                                                                          |
| support_upsert_by_query_primary_key_exist | Boolean | No       | false   | Choose to use INSERT sql, UPDATE sql to process update events(INSERT, UPDATE_AFTER) based on query primary key exists. This configuration is only used when database unsupport upsert syntax. **Note**: that this method has low performance |
| connection_check_timeout_sec              | Int     | No       | 30      | The time in seconds to wait for the database operation used to validate the connection to complete.                                                                                                                                          |
| max_retries                               | Int     | No       | 0       | The number of retries to submit failed (executeBatch)                                                                                                                                                                                        |
| batch_size                                | Int     | No       | 1000    | For batch writing, when the number of buffered records reaches the number of `batch_size` or the time reaches `checkpoint.interval`<br/>, the data will be flushed into the database                                                         |
| is_exactly_once                           | Boolean | No       | false   | Whether to enable exactly-once semantics, which will use Xa transactions. If on, you need to<br/>set `xa_data_source_class_name`. Kingbase currently does not support                                                                        |
| generate_sink_sql                         | Boolean | No       | false   | Generate sql statements based on the database table you want to write to                                                                                                                                                                     |
| xa_data_source_class_name                 | String  | No       | -       | The xa data source class name of the database Driver，Kingbase currently does not support                                                                                                                                                     |
| max_commit_attempts                       | Int     | No       | 3       | The number of retries for transaction commit failures                                                                                                                                                                                        |
| transaction_timeout_sec                   | Int     | No       | -1      | The timeout after the transaction is opened, the default is -1 (never timeout). Note that setting the timeout may affect<br/>exactly-once semantics                                                                                          |
| auto_commit                               | Boolean | No       | true    | Automatic transaction commit is enabled by default                                                                                                                                                                                           |
| common-options                            |         | no       | -       | Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details                                                                                                                                  |
| enable_upsert                             | Boolean | No       | true    | Enable upsert by primary_keys exist, If the task has no key duplicate data, setting this parameter to `false` can speed up data import                                                                                                       |

### Tips

> If partition_column is not set, it will run in single concurrency, and if partition_column is set, it will be executed
> in parallel according to the concurrency of tasks.

## Task Example

### Simple:

> This example defines a SeaTunnel synchronization task that automatically generates data through FakeSource and sends
> it to JDBC Sink. FakeSource generates a total of 16 rows of data (row.num=16), with each row having 12 fields. The final target table is test_table will also be 16 rows of data in the table.
> Before
> run this job, you need create database test and table test_table in your Kingbase. And if you have not yet installed and
> deployed SeaTunnel, you need to follow the instructions in [Install SeaTunnel](../../start-v2/locally/deployment.md)
> to
> install and deploy SeaTunnel. And then follow the instructions
> in [Quick Start With SeaTunnel Engine](../../start-v2/locally/quick-start-seatunnel-engine.md) to run this job.

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
            c_string = string
            c_boolean = boolean
            c_tinyint = tinyint
            c_smallint = smallint
            c_int = int
            c_bigint = bigint
            c_float = float
            c_double = double
            c_decimal = "decimal(30, 8)"
            c_date = date
            c_time = time 
            c_timestamp = timestamp
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
        url = "jdbc:kingbase8://127.0.0.1:54321/dbname"
        driver = "com.kingbase8.Driver"
        user = "root"
        password = "123456"
        query = "insert into test_table(c_string,c_boolean,c_tinyint,c_smallint,c_int,c_bigint,c_float,c_double,c_decimal,c_date,c_time,c_timestamp) values(?,?,?,?,?,?,?,?,?,?,?,?)"
        }
  # If you would like to get more information about how to configure seatunnel and see full list of sink plugins,
  # please go to https://seatunnel.apache.org/docs/connector-v2/sink
}
```

### Generate Sink SQL

> This example not need to write complex sql statements, you can configure the database name table name to automatically
> generate add statements for you

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

