import ChangeLog from '../changelog/connector-jdbc.md';

# Oracle

> JDBC Oracle Sink Connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Write data through jdbc. Support Batch mode and Streaming mode, support concurrent writing, support exactly-once
semantics (using XA transaction guarantee).

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Key Features

- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [x] [timer flush](../../introduction/concepts/connector-v2-features.md)

> Use `Xa transactions` to ensure `exactly-once`. So only support `exactly-once` for the database which is
> support `Xa transactions`. You can set `is_exactly_once=true` and `max_retries=0` to enable it.

## Supported DataSource Info

| Datasource |                    Supported Versions                    |          Driver          |                  Url                   |                               Maven                                |
|------------|----------------------------------------------------------|--------------------------|----------------------------------------|--------------------------------------------------------------------|
| Oracle     | Different dependency version has different driver class. | oracle.jdbc.OracleDriver | jdbc:oracle:thin:@datasource01:1523:xe | https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8 |

## Database Dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATUNNEL_HOME/plugins/jdbc/lib/' working directory<br/>
> For example Oracle datasource: cp ojdbc8-xxxxxx.jar $SEATUNNEL_HOME/lib/<br/>
> To support the i18n character set, copy the orai18n.jar to the $SEATUNNEL_HOME/lib/ directory.

## Data Type Mapping

|                                   Oracle Data Type                                   | SeaTunnel Data Type |
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

## Options

|                   Name                    |  Type   | Required |           Default            |                                                                                                                  Description                                                                                                                   |
|-------------------------------------------|---------|----------|------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String  | Yes      | -                            | The URL of the JDBC connection. Refer to a case: jdbc:oracle:thin:@datasource01:1523:xe                                                                                                                                                        |
| driver                                    | String  | Yes      | -                            | The jdbc class name used to connect to the remote data source,<br/> if you use Oracle the value is `oracle.jdbc.OracleDriver`.                                                                                                                 |
| username                                      | String  | No       | -                            | Connection instance user name                                                                                                                                                                                                                  |
| password                                  | String  | No       | -                            | Connection instance password                                                                                                                                                                                                                   |
| query                                     | String  | No       | -                            | Use this sql write upstream input datas to database. e.g `INSERT ...`,`query` have the higher priority                                                                                                                                         |
| database                                  | String  | No       | -                            | Use this `database` and `table-name` auto-generate sql and receive upstream input datas write to database.<br/>This option is mutually exclusive with `query` and has a higher priority.                                                       |
| table                                     | String  | No       | -                            | Use database and this table-name auto-generate sql and receive upstream input datas write to database.<br/>This option is mutually exclusive with `query` and has a higher priority.                                                           |
| primary_keys                              | Array   | No       | -                            | This option is used to support operations such as `insert`, `delete`, and `update` when automatically generate sql.                                                                                                                            |
| connection_check_timeout_sec              | Int     | No       | 30                           | The time in seconds to wait for the database operation used to validate the connection to complete.                                                                                                                                            |
| max_retries                               | Int     | No       | 0                            | The number of retries to submit failed (executeBatch)                                                                                                                                                                                          |
| batch_size                                | Int     | No       | 1000                         | For batch writing, when the number of buffered records reaches `batch_size`, the data will be flushed into the database. If `batch_interval_ms` is greater than 0, elapsed time can also trigger a flush.                                      |
| batch_interval_ms                         | Long    | No       | 0                            | Write-triggered flush interval in milliseconds. `0` disables time-based flushing. When greater than 0, the writer checks elapsed time on each record and flushes synchronously when the interval is reached.                                    |
| is_exactly_once                           | Boolean | No       | false                        | Whether to enable exactly-once semantics, which will use Xa transactions. If on, you need to<br/>set `xa_data_source_class_name`.                                                                                                              |
| generate_sink_sql                         | Boolean | No       | false                        | Generate sql statements based on the database table you want to write to.                                                                                                                                                                      |
| xa_data_source_class_name                 | String  | No       | -                            | The xa data source class name of the database Driver, for example, Oracle is `oracle.jdbc.xa.client.OracleXADataSource`, and<br/>please refer to appendix for other data sources                                                               |
| max_commit_attempts                       | Int     | No       | 3                            | The number of retries for transaction commit failures                                                                                                                                                                                          |
| transaction_timeout_sec                   | Int     | No       | -1                           | The timeout after the transaction is opened, the default is -1 (never timeout). Note that setting the timeout may affect<br/>exactly-once semantics                                                                                            |
| auto_commit                               | Boolean | No       | true                         | Automatic transaction commit is enabled by default                                                                                                                                                                                             |
| properties                                | Map     | No       | -                            | Additional connection configuration parameters,when properties and URL have the same parameters, the priority is determined by the <br/>specific implementation of the driver. For example, in MySQL, properties take precedence over the URL. |
| common-options                            |         | No       | -                            | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details                                                                                                                                    |
| schema_save_mode                          | Enum    | No       | CREATE_SCHEMA_WHEN_NOT_EXIST | Before the synchronous task is turned on, different treatment schemes are selected for the existing surface structure of the target side.                                                                                                      |
| data_save_mode                            | Enum    | No       | APPEND_DATA                  | Before the synchronous task is turned on, different processing schemes are selected for data existing data on the target side.                                                                                                                 |
| custom_sql                                | String  | No       | -                            | When data_save_mode selects CUSTOM_PROCESSING, you should fill in the CUSTOM_SQL parameter. This parameter usually fills in a SQL that can be executed. SQL will be executed before synchronization tasks.                                     |
| enable_upsert                             | Boolean | No       | true                         | Enable upsert by primary_keys exist, If the task has no key duplicate data, setting this parameter to `false` can speed up data import                                                                                                         |
| multi_table_sink_replica                  | Int     | No       | 1                            | The number of sink writer replicas used when writing multiple tables.                                                                                                                                                                          |

### Tips

> If partition_column is not set, it will run in single concurrency, and if partition_column is set, it will be executed  in parallel according to the concurrency of tasks.

## Task Example

### Simple

> This example defines a SeaTunnel synchronization task that automatically generates data through FakeSource and sends it to JDBC Sink. FakeSource generates a total of 16 rows of data (row.num=16), with each row having two fields, name (string type) and age (int type). The final target table is test_table will also be 16 rows of data in the table. Before run this job, you need create database test and table test_table in your Oracle. And if you have not yet installed and deployed SeaTunnel, you need to follow the instructions in [Install SeaTunnel](../../getting-started/locally/deployment.md) to install and deploy SeaTunnel. And then follow the instructions in [Quick Start With SeaTunnel Engine](../../getting-started/locally/quick-start-seatunnel-engine.md) to run this job.

```
# Defining the runtime environment
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
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
  # please go to https://seatunnel.apache.org/docs/connectors/source
}

transform {
  # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/transforms
}

sink {
    jdbc {
        url = "jdbc:oracle:thin:@datasource01:1523:xe"
        driver = "oracle.jdbc.OracleDriver"
        username = root
        password = 123456
        query = "INSERT INTO TEST.TEST_TABLE(NAME,AGE) VALUES(?,?)"
     }
  # If you would like to get more information about how to configure seatunnel and see full list of sink plugins,
  # please go to https://seatunnel.apache.org/docs/connectors/sink
}
```

### Generate Sink SQL

> This example  not need to write complex sql statements, you can configure the database name table name to automatically generate add statements for you

```
sink {
    Jdbc {
        url = "jdbc:oracle:thin:@datasource01:1523:xe"
        driver = "oracle.jdbc.OracleDriver"
        username = root
        password = 123456
        
        generate_sink_sql = true
        database = XE
        table = "TEST.TEST_TABLE"
    }
}
```

### Exactly-once

> For accurate write scene we guarantee accurate once

```
sink {
    jdbc {
        url = "jdbc:oracle:thin:@datasource01:1523:xe"
        driver = "oracle.jdbc.OracleDriver"
    
        max_retries = 0
        username = root
        password = 123456
        query = "INSERT INTO TEST.TEST_TABLE(NAME,AGE) VALUES(?,?)"
    
        is_exactly_once = "true"
    
        xa_data_source_class_name = "oracle.jdbc.xa.client.OracleXADataSource"
    }
}
```

### CDC(Change Data Capture) Event

> CDC change data is also supported by us In this case, you need config database, table and primary_keys.

```
sink {
    jdbc {
        url = "jdbc:oracle:thin:@datasource01:1523:xe"
        driver = "oracle.jdbc.OracleDriver"
        username = root
        password = 123456

        generate_sink_sql = true
        # You need to configure both database and table
        database = XE
        table = "TEST.TEST_TABLE"
        primary_keys = ["ID"]
        schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
        data_save_mode="APPEND_DATA"
    }
}
```

### Timer Flush + Batch Combined

For streaming jobs that should not wait for the buffer to fill before flushing, set both `batch_size` and `batch_interval_ms`. The flush is **write-triggered**: each incoming write checks the buffered row count and elapsed time and flushes synchronously when either threshold is reached. There is no background scheduler, so during idle periods (no incoming rows) buffered rows are held until the next row arrives or a checkpoint completes — `batch_interval_ms` does not by itself guarantee a strict wall-clock latency bound.

```hocon
sink {
  Jdbc {
    url = "jdbc:oracle:thin:@datasource01:1523:xe"
    driver = "oracle.jdbc.OracleDriver"
    username = "root"
    password = "123456"
    generate_sink_sql = true
    database = "XE"
    table = "TEST.TEST_TABLE"
    primary_keys = ["ID"]
    batch_size = 2000
    batch_interval_ms = 5000
  }
}
```

### Multi-Table Write With Placeholder

Use `${schema_name}` and `${table_name}` placeholders in `table` to route each row to the matching target table based on metadata carried by the upstream row. Set `multi_table_sink_replica` to control how many writer replicas the sink uses for multi-table writes.

```hocon
sink {
  Jdbc {
    url = "jdbc:oracle:thin:@datasource01:1523:xe"
    driver = "oracle.jdbc.OracleDriver"
    username = "root"
    password = "123456"
    generate_sink_sql = true
    database = "XE"
    table = "${schema_name}.${table_name}_SINK"
    primary_keys = ["ID"]
    multi_table_sink_replica = 2
  }
}
```

## Changelog

<ChangeLog />
