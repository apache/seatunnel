import ChangeLog from '../changelog/connector-jdbc.md';

# OceanBase

> JDBC OceanBase Sink Connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [x] [timer flush](../../introduction/concepts/connector-v2-features.md)

> Exactly-once uses XA transactions. Enable it with `is_exactly_once = true`, `max_retries = 0`, and a valid OceanBase XA data source class from the OceanBase JDBC driver.

## Description

Write data to OceanBase through JDBC. The sink supports batch and streaming jobs, CDC row kinds, generated SQL, save modes, multi-table writes, and exactly-once semantics when XA transactions are configured.

## Supported DataSource Info

| Datasource |       Supported versions       |          Driver           |                 Url                  |                                     Maven                                     |
|------------|--------------------------------|---------------------------|--------------------------------------|-------------------------------------------------------------------------------|
| OceanBase  | All OceanBase server versions. | com.oceanbase.jdbc.Driver | jdbc:oceanbase://localhost:2883/test | [Download](https://mvnrepository.com/artifact/com.oceanbase/oceanbase-client) |

## Database Dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATUNNEL_HOME/plugins/jdbc/lib/' working directory<br/>
> For example: cp oceanbase-client-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## Data Type Mapping

### Mysql Mode

|                                                          Mysql Data type                                                          |                                                                 SeaTunnel Data type                                                                 |
|-----------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| BIT(1)<br/>INT UNSIGNED                                                                                                           | BOOLEAN                                                                                                                                             |
| TINYINT<br/>TINYINT UNSIGNED<br/>SMALLINT<br/>SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED<br/>INT<br/>INTEGER<br/>YEAR | INT                                                                                                                                                 |
| INT UNSIGNED<br/>INTEGER UNSIGNED<br/>BIGINT                                                                                      | BIGINT                                                                                                                                              |
| BIGINT UNSIGNED                                                                                                                   | DECIMAL(20,0)                                                                                                                                       |
| DECIMAL(x,y)(Get the designated column's specified column size.<38)                                                               | DECIMAL(x,y)                                                                                                                                        |
| DECIMAL(x,y)(Get the designated column's specified column size.>38)                                                               | DECIMAL(38,18)                                                                                                                                      |
| DECIMAL UNSIGNED                                                                                                                  | DECIMAL((Get the designated column's specified column size)+1,<br/>(Gets the designated column's number of digits to right of the decimal point.))) |
| FLOAT<br/>FLOAT UNSIGNED                                                                                                          | FLOAT                                                                                                                                               |
| DOUBLE<br/>DOUBLE UNSIGNED                                                                                                        | DOUBLE                                                                                                                                              |
| CHAR<br/>VARCHAR<br/>TINYTEXT<br/>MEDIUMTEXT<br/>TEXT<br/>LONGTEXT<br/>JSON                                                       | STRING                                                                                                                                              |
| DATE                                                                                                                              | DATE                                                                                                                                                |
| TIME                                                                                                                              | TIME                                                                                                                                                |
| DATETIME<br/>TIMESTAMP                                                                                                            | TIMESTAMP                                                                                                                                           |
| TINYBLOB<br/>MEDIUMBLOB<br/>BLOB<br/>LONGBLOB<br/>BINARY<br/>VARBINAR<br/>BIT(n)                                                  | BYTES                                                                                                                                               |
| GEOMETRY<br/>UNKNOWN                                                                                                              | Not supported yet                                                                                                                                   |

### Oracle Mode

|                     Oracle Data type                      | SeaTunnel Data type |
|-----------------------------------------------------------|---------------------|
| Number(p), p <= 9                                         | INT                 |
| Number(p), p <= 18                                        | BIGINT              |
| Number(p), p > 18                                         | DECIMAL(38,18)      |
| REAL<br/> BINARY_FLOAT                                    | FLOAT               |
| BINARY_DOUBLE                                             | DOUBLE              |
| CHAR<br/>NCHAR<br/>NVARCHAR2<br/>NCLOB<br/>CLOB<br/>ROWID | STRING              |
| DATE                                                      | DATE                |
| TIMESTAMP<br/>TIMESTAMP WITH LOCAL TIME ZONE              | TIMESTAMP           |
| BLOB<br/>RAW<br/>LONG RAW<br/>BFILE                       | BYTES               |
| UNKNOWN                                                   | Not supported yet   |

## Sink Options

|                   Name                    |  Type   | Required | Default |                                                                                                                  Description                                                                                                                   |
|-------------------------------------------|---------|----------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String  | Yes      | -       | The URL of the JDBC connection. Refer to a case: jdbc:oceanbase://localhost:2883/test                                                                                                                                                          |
| driver                                    | String  | Yes      | -       | The jdbc class name used to connect to the remote data source, should be `com.oceanbase.jdbc.Driver`.                                                                                                                                          |
| username                                      | String  | No       | -       | Connection instance user name                                                                                                                                                                                                                  |
| password                                  | String  | No       | -       | Connection instance password                                                                                                                                                                                                                   |
| query                                     | String  | No       | -       | Use this SQL to write upstream data to OceanBase, for example `INSERT ...`. When `generate_sink_sql = false`, `query` is required. Save mode options do not run in custom `query` mode.                                                        |
| compatible_mode                           | String  | Yes      | -       | The compatible mode of OceanBase, can be 'mysql' or 'oracle'.                                                                                                                                                                                  |
| database                                  | String  | No       | -       | Database used when `generate_sink_sql = true`. This option is required when generated SQL is enabled.                                                                                                                                          |
| table                                     | String  | No       | -       | Target table used when `generate_sink_sql = true`. It supports placeholders such as `${schema_name}` and `${table_name}` for multi-table writes.                                                                                               |
| primary_keys                              | Array   | No       | -       | This option is used to support operations such as `insert`, `delete`, and `update` when automatically generate sql.                                                                                                                            |
| connection_check_timeout_sec              | Int     | No       | 30      | The time in seconds to wait for the database operation used to validate the connection to complete.                                                                                                                                            |
| max_retries                               | Int     | No       | 0       | The number of retries to submit failed (executeBatch)                                                                                                                                                                                          |
| batch_size                                | Int     | No       | 1000    | For batch writing, when buffered record count reaches `batch_size`, data is flushed to OceanBase. If `batch_interval_ms` is greater than 0, elapsed time can also trigger a flush.                                                             |
| batch_interval_ms                         | Long    | No       | 0       | Write-triggered flush interval in milliseconds. `0` disables time-based flushing. When greater than 0, the writer checks elapsed time on each record and flushes synchronously when the interval is reached.                                  |
| is_exactly_once                           | Boolean | No       | false   | Whether to enable exactly-once semantics through XA transactions. If enabled, set `xa_data_source_class_name` and keep `max_retries = 0`.                                                                                                     |
| generate_sink_sql                         | Boolean | No       | false   | Generate sql statements based on the database table you want to write to                                                                                                                                                                       |
| xa_data_source_class_name                 | String  | No       | -       | XA data source class name from the OceanBase JDBC driver. Required when `is_exactly_once = true`.                                                                                                                                              |
| max_commit_attempts                       | Int     | No       | 3       | The number of retries for transaction commit failures                                                                                                                                                                                          |
| transaction_timeout_sec                   | Int     | No       | -1      | The timeout after the transaction is opened, the default is -1 (never timeout). Note that setting the timeout may affect<br/>exactly-once semantics                                                                                            |
| auto_commit                               | Boolean | No       | true    | Automatic transaction commit is enabled by default                                                                                                                                                                                             |
| field_ide                                 | String  | No       | -       | Controls field name case conversion. Available values are `ORIGINAL`, `UPPERCASE`, and `LOWERCASE`.                                                                                                                                           |
| properties                                | Map     | No       | -       | Additional connection configuration parameters,when properties and URL have the same parameters, the priority is determined by the <br/>specific implementation of the driver. For example, in MySQL, properties take precedence over the URL. |
| schema_save_mode                          | Enum    | No       | CREATE_SCHEMA_WHEN_NOT_EXIST | Controls how target table structure is handled before the job starts.                                                                                                                                                           |
| data_save_mode                            | Enum    | No       | APPEND_DATA | Controls how existing target table data is handled before the job starts.                                                                                                                                                                      |
| custom_sql                                | String  | No       | -       | SQL executed before synchronization when `data_save_mode = CUSTOM_PROCESSING`. This option is not executed in custom `query` mode.                                                                                                             |
| common-options                            |         | No       | -       | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details                                                                                                                                    |
| enable_upsert                             | Boolean | No       | true    | Enable upsert by primary_keys exist, If the task has no key duplicate data, setting this parameter to `false` can speed up data import                                                                                                         |
| is_primary_key_updated                    | Boolean | No       | true    | Whether primary key fields are included when generated update statements are built.                                                                                                                                                             |
| support_upsert_by_insert_only             | Boolean | No       | false   | Whether to support upsert behavior through insert-only statements for compatible dialects.                                                                                                                                                      |
| multi_table_sink_replica                  | Int     | No       | 1       | Number of sink writer replicas used when writing multiple tables.                                                                                                                                                                              |

### Tips

> Configure `compatible_mode = "mysql"` for OceanBase MySQL mode and `compatible_mode = "oracle"` for OceanBase Oracle mode.
>
> Use `query` when you want to provide the full write SQL yourself. Use `generate_sink_sql = true` with `database` and `table` when you want SeaTunnel to generate INSERT/UPSERT SQL and apply save mode settings.
>
> To consume CDC data, use generated SQL and configure `primary_keys`; otherwise UPDATE and DELETE events cannot be mapped safely.

## Task Example

### Simple

> This example defines a SeaTunnel synchronization task that automatically generates data through FakeSource and sends it to JDBC Sink. FakeSource generates a total of 16 rows of data (row.num=16), with each row having two fields, name (string type) and age (int type). The final target table is test_table will also be 16 rows of data in the table. Before run this job, you need create database test and table test_table in your mysql. And if you have not yet installed and deployed SeaTunnel, you need to follow the instructions in [Install SeaTunnel](../../getting-started/locally/deployment.md) to install and deploy SeaTunnel. And then follow the instructions in [Quick Start With SeaTunnel Engine](../../getting-started/locally/quick-start-seatunnel-engine.md) to run this job.

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
  # please go to https://seatunnel.apache.org/docs/connectors/source
}

transform {
  # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/transforms
}

sink {
    jdbc {
        url = "jdbc:oceanbase://localhost:2883/test"
        driver = "com.oceanbase.jdbc.Driver"
        username = "root"
        password = "123456"
        compatible_mode = "mysql"
        query = "insert into test_table(name,age) values(?,?)"
    }
  # If you would like to get more information about how to configure seatunnel and see full list of sink plugins,
  # please go to https://seatunnel.apache.org/docs/connectors/sink
}
```

### Generate Sink SQL

> This example  not need to write complex sql statements, you can configure the database name table name to automatically generate add statements for you

```
sink {
    jdbc {
        url = "jdbc:oceanbase://localhost:2883/test"
        driver = "com.oceanbase.jdbc.Driver"
        username = "root"
        password = "123456"
        compatible_mode = "mysql"
        # Automatically generate sql statements based on database table names
        generate_sink_sql = true
        database = test
        table = test_table
    }
}
```

### Generated SQL With Save Mode

```
sink {
  jdbc {
    url = "jdbc:oceanbase://localhost:2883/test"
    driver = "com.oceanbase.jdbc.Driver"
    username = "root@test"
    password = ""
    compatible_mode = "mysql"
    generate_sink_sql = true
    database = "test"
    table = "sink_table"
    primary_keys = ["id"]
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

### CDC(Change Data Capture) Event

> CDC change data is also supported by us In this case, you need config database, table and primary_keys.

```
sink {
    jdbc {
        url = "jdbc:oceanbase://localhost:3306/test"
        driver = "com.oceanbase.jdbc.Driver"
        username = "root"
        password = "123456"
        compatible_mode = "mysql"
        generate_sink_sql = true
        # You need to configure both database and table
        database = test
        table = sink_table
        primary_keys = ["id","name"]
    }
}
```

### Oracle-Compatible Mode

```
sink {
  jdbc {
    url = "jdbc:oceanbase://localhost:2883/TESTUSER"
    driver = "com.oceanbase.jdbc.Driver"
    username = "TESTUSER@test"
    password = ""
    compatible_mode = "oracle"
    query = "INSERT INTO SINK_TABLE (ID, NAME, CREATE_TIME) VALUES (?, ?, ?)"
  }
}
```

### Multiple Table Write

Use placeholders in `table` when rows from upstream carry table identity.

```
sink {
  jdbc {
    url = "jdbc:oceanbase://localhost:2883/test"
    driver = "com.oceanbase.jdbc.Driver"
    username = "root@test"
    password = ""
    compatible_mode = "mysql"
    generate_sink_sql = true
    database = "test"
    table = "${table_name}_sink"
    primary_keys = ["id"]
    multi_table_sink_replica = 2
  }
}
```

### EXACTLY_ONCE With XA Transactions

Enable XA-backed exactly-once semantics by setting `is_exactly_once = true`, providing `xa_data_source_class_name` from the OceanBase JDBC driver, and keeping `max_retries = 0`. The writer wraps each checkpoint batch in an XA transaction so the write either commits atomically with the source checkpoint or is rolled back on failure.

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

sink {
  Jdbc {
    url = "jdbc:oceanbase://localhost:2883/test"
    driver = "com.oceanbase.jdbc.Driver"
    username = "root"
    password = "123456"
    compatible_mode = "mysql"
    generate_sink_sql = true
    database = "test"
    table = "sink_table"
    primary_keys = ["id"]
    is_exactly_once = true
    xa_data_source_class_name = "com.oceanbase.jdbc.OceanBaseXADataSource"
    max_retries = 0
    batch_size = 1000
  }
}
```

### Timer Flush + Batch Combined

For streaming jobs, set `batch_interval_ms` to flush buffered rows based on elapsed time since the previous flush. The flush is **write-triggered**: each incoming write checks the elapsed time and flushes synchronously when the interval is reached. There is no background scheduler, so during idle periods (no incoming rows) buffered rows are held until the next row arrives or a checkpoint completes — `batch_interval_ms` does not by itself guarantee a wall-clock latency bound for low-throughput streams. Combined with `batch_size`, it gives both throughput and reasonable per-record latency, but do not treat it as a strict real-time timer.

```hocon
sink {
  Jdbc {
    url = "jdbc:oceanbase://localhost:2883/test"
    driver = "com.oceanbase.jdbc.Driver"
    username = "root"
    password = "123456"
    compatible_mode = "mysql"
    generate_sink_sql = true
    database = "test"
    table = "sink_table"
    primary_keys = ["id"]
    batch_size = 2000
    batch_interval_ms = 5000
  }
}
```

## Changelog

<ChangeLog />
