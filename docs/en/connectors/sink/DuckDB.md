import ChangeLog from '../changelog/connector-jdbc.md';

# DuckDB

> JDBC DuckDB Sink Connector

## Support DuckDB Version

- 0.8.x/0.9.x/0.10.x/1.x

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Write data through jdbc. Support Batch mode and Streaming mode, support concurrent writing, support exactly-once
semantics (using XA transaction guarantee).

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/org.duckdb/duckdb_jdbc) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/org.duckdb/duckdb_jdbc) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Key Features

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

> Use `Xa transactions` to ensure `exactly-once`. So only support `exactly-once` for the database which is
> support `Xa transactions`. You can set `is_exactly_once=true` to enable it.

## Supported DataSource Info

| Datasource | Supported Versions                                       | Driver                  | Url                              | Maven                                                                 |
|------------|----------------------------------------------------------|-------------------------|----------------------------------|-----------------------------------------------------------------------|
| DuckDB     | Different dependency version has different driver class. | org.duckdb.DuckDBDriver | jdbc:duckdb:/path/to/database.db | [Download](https://mvnrepository.com/artifact/org.duckdb/duckdb_jdbc) |

## Data Type Mapping

| SeaTunnel Data Type                                                 | DuckDB Data Type |
|---------------------------------------------------------------------|------------------|
| BOOLEAN                                                             | BOOLEAN          |
| TINYINT<br/>SMALLINT<br/>INT                                        | INTEGER          |
| BIGINT                                                              | BIGINT           |
| DECIMAL(x,y)(Get the designated column's specified column size.<38) | DECIMAL(x,y)     |
| DECIMAL(x,y)(Get the designated column's specified column size.>38) | DECIMAL(38,18)   |
| FLOAT                                                               | FLOAT            |
| DOUBLE                                                              | DOUBLE           |
| STRING                                                              | VARCHAR          |
| DATE                                                                | DATE             |
| TIME                                                                | TIME             |
| TIMESTAMP                                                           | TIMESTAMP        |
| BYTES<br/>ARRAY<br/>ROW<br/>MAP                                     | BLOB             |

## Sink Options

| url                                       | String  | Yes      | -                            | The URL of the JDBC connection. Refer to a case: jdbc:duckdb:/path/to/database.db                                                                                                                                                         |
| driver                                    | String  | Yes      | -                            | The jdbc class name used to connect to the remote data source,<br/> if you use DuckDB the value is `org.duckdb.DuckDBDriver`.                                                                                                                  |
| username                                      | String  | No       | -                            | Connection instance user name                                                                                                                                                                                                                  |
| password                                  | String  | No       | -                            | Connection instance password                                                                                                                                                                                                                   |
| query                                     | String  | No       | -                            | Use this sql write upstream input datas to database. e.g `INSERT ...`,`query` have the higher priority                                                                                                                                         |
| database                                  | String  | No       | main                         | Use this `database` and `table-name` auto-generate sql and receive upstream input datas write to database.<br/>This option is mutually exclusive with `query` and has a higher priority.                                                       |
| table                                     | String  | No       | -                            | Use database and this table-name auto-generate sql and receive upstream input datas write to database.<br/>This option is mutually exclusive with `query` and has a higher priority.                                                           |
| primary_keys                              | Array   | No       | -                            | This option is used to support operations such as `insert`, `delete`, and `update` when automatically generate sql.                                                                                                                            |
| connection_check_timeout_sec              | Int     | No       | 30                           | The time in seconds to wait for the database operation used to validate the connection to complete.                                                                                                                                            |
| max_retries                               | Int     | No       | 0                            | The number of retries to submit failed (executeBatch)                                                                                                                                                                                          |
| batch_size                                | Int     | No       | 1000                         | For batch writing, when the number of buffered records reaches the number of `batch_size` or the time reaches `checkpoint.interval`<br/>, the data will be flushed into the database                                                           |
| is_exactly_once                           | Boolean | No       | false                        | Whether to enable exactly-once semantics, which will use Xa transactions. If on, you need to<br/>set `xa_data_source_class_name`.                                                                                                              |
| generate_sink_sql                         | Boolean | No       | false                        | Generate sql statements based on the database table you want to write to                                                                                                                                                                       |
| xa_data_source_class_name                 | String  | No       | -                            | The xa data source class name of the database Driver, for example, DuckDB is `org.duckdb.DuckDBXADataSource`, and<br/>please refer to appendix for other data sources                                                                     |
| max_commit_attempts                       | Int     | No       | 3                            | The number of retries for transaction commit failures                                                                                                                                                                                          |
| transaction_timeout_sec                   | Int     | No       | -1                           | The timeout after the transaction is opened, the default is -1 (never timeout). Note that setting the timeout may affect<br/>exactly-once semantics                                                                                            |
| auto_commit                               | Boolean | No       | true                         | Automatic transaction commit is enabled by default                                                                                                                                                                                             |
| field_ide                                 | String  | No       | -                            | Identify whether the field needs to be converted when synchronizing from the source to the sink. `ORIGINAL` indicates no conversion is needed; `UPPERCASE` indicates conversion to uppercase; `LOWERCASE` indicates conversion to lowercase.     |
| properties                                | Map     | No       | -                            | Additional connection configuration parameters, when properties and URL have the same parameters, the priority is determined by the <br/>specific implementation of the driver. For example, in DuckDB, properties take precedence over the URL. |
| common-options                            |         | No       | -                            | Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details                                                                                                                                    |
| schema_save_mode                          | Enum    | No       | CREATE_SCHEMA_WHEN_NOT_EXIST | Before the synchronous task is turned on, different treatment schemes are selected for the existing surface structure of the target side.                                                                                                      |
| data_save_mode                            | Enum    | No       | APPEND_DATA                  | Before the synchronous task is turned on, different processing schemes are selected for data existing data on the target side.                                                                                                                 |
| custom_sql                                | String  | No       | -                            | When data_save_mode selects CUSTOM_PROCESSING, you should fill in the CUSTOM_SQL parameter. This parameter usually fills in a SQL that can be executed. SQL will be executed before synchronization tasks.                                     |
| enable_upsert                             | Boolean | No       | true                         | Enable upsert by primary_keys exist, If the task only has `insert`, setting this parameter to `false` can speed up data import                                                                                                                 |

### Tips

> If partition_column is not set, it will run in single concurrency, and if partition_column is set, it will be executed  in parallel according to the concurrency of tasks.

## Task Example

### Simple

```
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    parallelism = 1
    row_num = 1000
    schema = {
      fields {
        id = "int"
        name = "string"
        age = "int"
        email = "string"
      }
    }
  }
}

sink {
  Jdbc {
    url = "jdbc:duckdb:/tmp/test.db"
    driver = "org.duckdb.DuckDBDriver"
    table = "sink_table"
    username = ""
    password = ""
  }
}
```

### CDC(Change data capture) event

```
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MySQL-CDC {
    base-url = "jdbc:mysql://localhost:3306/test"
    username = "root"
    password = "123456"
    table-names = ["test.user"]
  }
}

sink {
  Jdbc {
    url = "jdbc:duckdb:/tmp/test.db"
    driver = "org.duckdb.DuckDBDriver"
    table = "sink_table"
    username = ""
    password = ""
    generate_sink_sql = true
    # You need to configure both database and table
    database = main
    table = "sink_table"
    primary_keys = ["id"]
  }
}
```

### Exactly-once

```
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    parallelism = 1
    row_num = 1000
    schema = {
      fields {
        id = "int"
        name = "string"
        age = "int"
        email = "string"
      }
    }
  }
}

sink {
  Jdbc {
    url = "jdbc:duckdb:/tmp/test.db"
    driver = "org.duckdb.DuckDBDriver"
    table = "sink_table"
    username = ""
    password = ""

    is_exactly_once = "true"

    xa_data_source_class_name = "org.duckdb.DuckDBXADataSource"
  }
}
```

## Changelog

<ChangeLog />