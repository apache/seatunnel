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

Write data to DuckDB through JDBC in batch or streaming jobs. DuckDB runs in-process, so a normal
DuckDB connection uses a local database file (`jdbc:duckdb:/path/to/database.db`) or an in-memory
database. DuckDB JDBC 1.3.1 does not provide an XA datasource; do not configure
`is_exactly_once = true` with this driver.

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/org.duckdb/duckdb_jdbc) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/org.duckdb/duckdb_jdbc) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

> The JDBC sink requires an XA datasource for exactly-once writes. DuckDB JDBC 1.3.1 does not
> provide one, so exactly-once is unavailable with this driver.

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
| database                                  | String  | No       | -                            | Use this `database` and `table` to auto-generate SQL and write upstream input data to the database. This option is only used to auto-generate SQL when `generate_sink_sql = true`; when `query` is set, `query` takes precedence.                                                                 |
| table                                     | String  | No       | -                            | Use database and this table name to auto-generate SQL and write upstream input data to the database. This option is only used to auto-generate SQL when `generate_sink_sql = true`; when `query` is set, `query` takes precedence.                                                                |
| primary_keys                              | Array   | No       | -                            | This option is used to support operations such as `insert`, `delete`, and `update` when automatically generating SQL.                                                                                                                          |
| connection_check_timeout_sec              | Int     | No       | 30                           | The time in seconds to wait for the database operation used to validate the connection to complete.                                                                                                                                            |
| max_retries                               | Int     | No       | 0                            | The number of retries to submit a failed `executeBatch` call.                                                                                                                                                                                  |
| batch_size                                | Int     | No       | 1000                         | For batch writing, when the number of buffered records reaches `batch_size` or the time reaches `checkpoint.interval`, the data is flushed into the database.                                                                                  |
| ducklake_bulk_write                       | Boolean | No       | false                        | For an existing DuckLake table, stage each batch in a DuckDB temporary table and write it to the lake with one `INSERT ... SELECT`. See below.                                                                                                  |
| is_exactly_once                           | Boolean | No       | false                        | Whether to enable exactly-once semantics, which uses XA transactions. When enabled, you must also set `xa_data_source_class_name`.                                                                                                              |
| generate_sink_sql                         | Boolean | No       | false                        | Generate SQL statements based on the database table you want to write to. Requires `database` and `table` (or `table_list`) to be configured.                                                                                                  |
| xa_data_source_class_name                 | String  | No       | -                            | XA datasource class name, if the selected driver supplies one. DuckDB JDBC 1.3.1 does not supply one.                                                                                                                                         |
| max_commit_attempts                       | Int     | No       | 3                            | The number of retries for transaction commit failures.                                                                                                                                                                                        |
| transaction_timeout_sec                   | Int     | No       | -1                           | The timeout after the transaction is opened, the default is `-1` (never timeout). Note that setting the timeout may affect exactly-once semantics.                                                                                             |
| auto_commit                               | Boolean | No       | true                         | Whether to enable automatic transaction commit. Set to `false` when `is_exactly_once = true`.                                                                                                                                                 |
| field_ide                                 | String  | No       | -                            | Identify whether the field needs to be converted when synchronizing from the source to the sink. `ORIGINAL` indicates no conversion is needed; `UPPERCASE` indicates conversion to uppercase; `LOWERCASE` indicates conversion to lowercase.     |
| properties                                | Map     | No       | -                            | Additional connection configuration parameters. When properties and URL have the same parameters, the priority is determined by the specific driver implementation. For DuckDB, properties take precedence over the URL.                         |
| common-options                            |         | No       | -                            | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.                                                                                                                    |
| schema_save_mode                          | Enum    | No       | CREATE_SCHEMA_WHEN_NOT_EXIST | How to handle the existing table schema on the target side before the sync task starts. Supported values: `RECREATE_SCHEMA`, `CREATE_SCHEMA_WHEN_NOT_EXIST`, `ERROR_WHEN_SCHEMA_NOT_EXIST`, `IGNORE`.                                                     |
| data_save_mode                            | Enum    | No       | APPEND_DATA                  | How to handle existing data on the target side before the sync task starts. Supported values: `DROP_DATA`, `APPEND_DATA`, `CUSTOM_PROCESSING`, `ERROR_WHEN_DATA_EXISTS`.                                                                      |
| custom_sql                                | String  | No       | -                            | When `data_save_mode = CUSTOM_PROCESSING`, fill in the CUSTOM_SQL parameter. This is a SQL statement that runs before the synchronization task.                                                                                               |
| enable_upsert                             | Boolean | No       | true                         | Enable upsert by `primary_keys`. If the task only has `insert`, setting this parameter to `false` can speed up data import.                                                                                                                   |
| multi_table_sink_replica                  | Int     | No       | 1                            | The number of replicas for multi-table write. When `multi_table_sink_replica > 1`, the data is written to multiple tables in parallel.                                                                                                       |

### Tips

> If partition_column is not set, it will run in single concurrency, and if partition_column is set, it will be executed  in parallel according to the concurrency of tasks.

## Task Example

### DuckLake bulk append

DuckLake tables can be written through the existing JDBC sink. With the DuckDB JDBC 1.3.1 driver,
`executeBatch` against a DuckLake table can produce one Parquet file per input row. Enable
`ducklake_bulk_write` to stage at most `batch_size` rows in a connection-local temporary table and
insert the batch into DuckLake with one SQL statement. The target table must already exist.

For an attached lake, initialize **every writer connection**, including reconnects, with a DuckDB
session-init SQL file. For example, `/etc/seatunnel/ducklake-init.sql` can load the matching DuckDB
extensions, configure the metadata and object-store credentials, and attach the lake:

```sql
LOAD ducklake;
LOAD postgres_scanner;
LOAD httpfs;
-- Configure PostgreSQL and S3 credentials for this worker without putting them in the job file.
ATTACH 'ducklake:postgres:dbname=lake_metadata host=metadata.example.com port=5432'
  AS lake (METADATA_SCHEMA 'lake_catalog', DATA_PATH 's3://my-bucket/ducklake/');
```

`lake_metadata` is the PostgreSQL **database**; `lake_catalog` is its metadata **schema**. The
DuckLake table schema, such as `main`, is separate. Use the actual names and credentials for your
deployment, and make the SQL file and matching extensions available on each worker.

```hocon
sink {
  Jdbc {
    url = "jdbc:duckdb:;session_init_sql_file=/etc/seatunnel/ducklake-init.sql"
    driver = "org.duckdb.DuckDBDriver"
    database = "lake"
    table = "main.events"
    generate_sink_sql = true
    ducklake_bulk_write = true
    schema_save_mode = "IGNORE"
    data_save_mode = "APPEND_DATA"
    batch_size = 1000
    auto_commit = true
    max_retries = 0
  }
}
```

This mode accepts INSERT rows only. It does not support `query`, primary keys/upserts, COPY, XA,
automatic table creation, or JDBC batch retries. Each successful flush commits one lake insert;
replaying a job after an uncertain commit can still duplicate rows. The number of Parquet files
also depends on DuckLake partitioning and file-size policies, so one file per flush is not a
general guarantee. The regular DuckDB sink behavior is unchanged when the option is false.

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

## Changelog

<ChangeLog />
