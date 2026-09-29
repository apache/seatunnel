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

Write data to a DuckDB database file through JDBC. Supports batch and streaming modes, supports concurrent
writing, and supports exactly-once semantics when the underlying JDBC driver exposes an XA datasource
(set `is_exactly_once = true` and provide `xa_data_source_class_name`). DuckDB runs in-process, so the connector
works against a local database file path (`jdbc:duckdb:/path/to/database.db`) or an in-memory database.

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/org.duckdb/duckdb_jdbc) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/org.duckdb/duckdb_jdbc) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Key Features

- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)

> Use `Xa transactions` to ensure `exactly-once`. So only support `exactly-once` for the database which is
> support `Xa transactions`. You can set `is_exactly_once=true` to enable it.
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

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

|                   Name                    |  Type   | Required |           Default            |                                                                                                                  Description                                                                                                                   |
|-------------------------------------------|---------|----------|------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String  | Yes      | -                            | The URL of the JDBC connection. Example: `jdbc:duckdb:/path/to/database.db`. For an in-memory DuckDB, use `jdbc:duckdb:`.                                                                                                                       |
| driver                                    | String  | Yes      | -                            | The jdbc class name used to connect to the remote data source. For DuckDB, the value is `org.duckdb.DuckDBDriver`.                                                                                                                              |
| username                                  | String  | No       | -                            | Connection instance user name. DuckDB does not require authentication for local files; leave empty unless you wrap it with a custom authenticator.                                                                                            |
| password                                  | String  | No       | -                            | Connection instance password. DuckDB does not require authentication for local files; leave empty unless you wrap it with a custom authenticator.                                                                                             |
| query                                     | String  | No       | -                            | Use this SQL to write upstream input data to the database, for example `INSERT ...`. When `query` is set, it has higher priority than `database`/`table`/`table_list`.                                                                       |
| database                                  | String  | No       | main                         | Use this `database` and `table` to auto-generate SQL and write upstream input data to the database. This option is mutually exclusive with `query` and has a higher priority.                                                                 |
| table                                     | String  | No       | -                            | Use database and this table name to auto-generate SQL and write upstream input data to the database. This option is mutually exclusive with `query` and has a higher priority.                                                                |
| primary_keys                              | Array   | No       | -                            | This option is used to support operations such as `insert`, `delete`, and `update` when automatically generating SQL.                                                                                                                          |
| connection_check_timeout_sec              | Int     | No       | 30                           | The time in seconds to wait for the database operation used to validate the connection to complete.                                                                                                                                            |
| max_retries                               | Int     | No       | 0                            | The number of retries to submit a failed `executeBatch` call.                                                                                                                                                                                  |
| batch_size                                | Int     | No       | 1000                         | For batch writing, when the number of buffered records reaches `batch_size` or the time reaches `checkpoint.interval`, the data is flushed into the database.                                                                                  |
| is_exactly_once                           | Boolean | No       | false                        | Whether to enable exactly-once semantics, which uses XA transactions. When enabled, you must also set `xa_data_source_class_name`.                                                                                                              |
| generate_sink_sql                         | Boolean | No       | false                        | Generate SQL statements based on the database table you want to write to. Requires `database` and `table` (or `table_list`) to be configured.                                                                                                  |
| xa_data_source_class_name                 | String  | No       | -                            | The XA datasource class name of the database driver. For DuckDB, use `org.duckdb.DuckDBXADataSource`.                                                                                                                                          |
| max_commit_attempts                       | Int     | No       | 3                            | The number of retries for transaction commit failures.                                                                                                                                                                                        |
| transaction_timeout_sec                   | Int     | No       | -1                           | The timeout after the transaction is opened, the default is `-1` (never timeout). Note that setting the timeout may affect exactly-once semantics.                                                                                             |
| auto_commit                               | Boolean | No       | true                         | Whether to enable automatic transaction commit. Set to `false` when `is_exactly_once = true`.                                                                                                                                                 |
| field_ide                                 | String  | No       | -                            | Identify whether the field needs to be converted when synchronizing from the source to the sink. `ORIGINAL` indicates no conversion is needed; `UPPERCASE` indicates conversion to uppercase; `LOWERCASE` indicates conversion to lowercase.     |
| properties                                | Map     | No       | -                            | Additional connection configuration parameters. When properties and URL have the same parameters, the priority is determined by the specific driver implementation. For DuckDB, properties take precedence over the URL.                         |
| common-options                            |         | No       | -                            | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.                                                                                                                    |
| schema_save_mode                          | Enum    | No       | CREATE_SCHEMA_WHEN_NOT_EXIST | How to handle the existing table schema on the target side before the sync task starts. Supported values: `RECREATE_SCHEMA`, `CREATE_SCHEMA_WHEN_NOT_EXIST`, `ERROR_WHEN_SCHEMA_NOT_EXIST`.                                                     |
| data_save_mode                            | Enum    | No       | APPEND_DATA                  | How to handle existing data on the target side before the sync task starts. Supported values: `DROP_DATA`, `APPEND_DATA`, `CUSTOM_PROCESSING`, `ERROR_WHEN_DATA_EXISTS`.                                                                      |
| custom_sql                                | String  | No       | -                            | When `data_save_mode = CUSTOM_PROCESSING`, fill in the CUSTOM_SQL parameter. This is a SQL statement that runs before the synchronization task.                                                                                               |
| enable_upsert                             | Boolean | No       | true                         | Enable upsert by `primary_keys`. If the task only has `insert`, setting this parameter to `false` can speed up data import.                                                                                                                   |
| multi_table_sink_replica                  | Int     | No       | 1                            | The number of replicas for multi-table write. When `multi_table_sink_replica > 1`, the data is written to multiple tables in parallel.                                                                                                       |

### Tips

> If partition_column is not set, it will run in single concurrency, and if partition_column is set, it will be executed  in parallel according to the concurrency of tasks.

## Task Example

### Simple

```hocon
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

### CDC (Change Data Capture) Event

```hocon
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

### Exactly-Once

```hocon
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
