# Snowflake

> JDBC Snowflake Sink Connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

## Description

Write data through jdbc. Support Batch mode and Streaming mode, support concurrent writing.

## Supported DataSource list

| Datasource |                    Supported Versions                    |                  Driver                   |                            Url                             |                                    Maven                                    |
|------------|----------------------------------------------------------|-------------------------------------------|------------------------------------------------------------|-----------------------------------------------------------------------------|
| snowflake  | Different dependency version has different driver class. | net.snowflake.client.jdbc.SnowflakeDriver | jdbc&#58;snowflake://<account_name>.snowflakecomputing.com | [Download](https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc) |

## Database dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATUNNEL_HOME/plugins/jdbc/lib/' working directory<br/>
> For example Snowflake datasource: cp snowflake-connector-java-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## Data Type Mapping

|                             Snowflake Data Type                             | SeaTunnel Data Type |
|-----------------------------------------------------------------------------|---------------------|
| BOOLEAN                                                                     | BOOLEAN             |
| TINYINT<br/>SMALLINT<br/>BYTEINT<br/>                                       | SHORT_TYPE          |
| INT<br/>INTEGER<br/>                                                        | INT                 |
| BIGINT                                                                      | LONG                |
| DECIMAL<br/>NUMERIC<br/>NUMBER<br/>                                         | DECIMAL(x,y)        |
| DECIMAL(x,y)(Get the designated column's specified column size.>38)         | DECIMAL(38,18)      |
| REAL<br/>FLOAT4                                                             | FLOAT               |
| DOUBLE<br/>DOUBLE PRECISION<br/>FLOAT8<br/>FLOAT<br/>                       | DOUBLE              |
| CHAR<br/>CHARACTER<br/>VARCHAR<br/>STRING<br/>TEXT<br/>VARIANT<br/>OBJECT   | STRING              |
| DATE                                                                        | DATE                |
| TIME                                                                        | TIME                |
| DATETIME<br/>TIMESTAMP<br/>TIMESTAMP_LTZ<br/>TIMESTAMP_NTZ<br/>TIMESTAMP_TZ | TIMESTAMP           |
| BINARY<br/>VARBINARY<br/>GEOGRAPHY<br/>GEOMETRY                             | BYTES               |

## Options

|                   Name                    |  Type   | Required | Default |                                                                                                                  Description                                                                                                                   |
|-------------------------------------------|---------|----------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String  | Yes      | -       | The URL of the JDBC connection. Refer to a case: jdbc&#58;snowflake://<account_name>.snowflakecomputing.com                                                                                                                                    |
| driver                                    | String  | Yes      | -       | The jdbc class name used to connect to the remote data source,<br/> if you use Snowflake the value is `net.snowflake.client.jdbc.SnowflakeDriver`.                                                                                             |
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
| max_commit_attempts                       | Int     | No       | 3       | The number of retries for transaction commit failures                                                                                                                                                                                          |
| transaction_timeout_sec                   | Int     | No       | -1      | The timeout after the transaction is opened, the default is -1 (never timeout). Note that setting the timeout may affect<br/>exactly-once semantics                                                                                            |
| auto_commit                               | Boolean | No       | true    | Automatic transaction commit is enabled by default                                                                                                                                                                                             |
| properties                                | Map     | No       | -       | Additional connection configuration parameters,when properties and URL have the same parameters, the priority is determined by the <br/>specific implementation of the driver. For example, in MySQL, properties take precedence over the URL. |
| common-options                            |         | No       | -       | Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details                                                                                                                                    |
| enable_upsert                             | Boolean | No       | true    | Enable upsert by primary_keys exist, If the task has no key duplicate data, setting this parameter to `false` can speed up data import                                                                                                         |

## tips

> If partition_column is not set, it will run in single concurrency, and if partition_column is set, it will be executed  in parallel according to the concurrency of tasks.
>
  ## Task Example

### simple:

> This example defines a SeaTunnel synchronization task that automatically generates data through FakeSource and sends it to JDBC Sink. FakeSource generates a total of 16 rows of data (row.num=16), with each row having two fields, name (string type) and age (int type). The final target table is test_table will also be 16 rows of data in the table. Before run this job, you need create database test and table test_table in your snowflake database. And if you have not yet installed and deployed SeaTunnel, you need to follow the instructions in [Install SeaTunnel](../../start-v2/locally/deployment.md) to install and deploy SeaTunnel. And then follow the instructions in [Quick Start With SeaTunnel Engine](../../start-v2/locally/quick-start-seatunnel-engine.md) to run this job.

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
        url = "jdbc:snowflake://<account_name>.snowflakecomputing.com"
        driver = "net.snowflake.client.jdbc.SnowflakeDriver"
        user = "root"
        password = "123456"
        query = "insert into test_table(name,age) values(?,?)"
    }
    # If you would like to get more information about how to configure seatunnel and see full list of sink plugins,
    # please go to https://seatunnel.apache.org/docs/connector-v2/sink
}
```

### CDC(Change data capture) event

> CDC change data is also supported by us In this case, you need config database, table and primary_keys.

```
sink {
   jdbc {
   url = "jdbc:snowflake://<account_name>.snowflakecomputing.com"
   driver = "net.snowflake.client.jdbc.SnowflakeDriver"
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

