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

> Exactly-once uses XA transactions. Enable it with `is_exactly_once = true`,
> `max_retries = 0`, and a valid OceanBase XA data source class from the
> OceanBase JDBC driver.

## Description

Write data to OceanBase through JDBC. The sink supports batch and streaming
jobs, CDC row kinds, generated SQL, save modes, multi-table writes, and
exactly-once semantics when XA transactions are configured.

:::tip

The connector uses the `Jdbc` plugin under the hood with the
`com.oceanbase.jdbc.Driver` driver. The `compatible_mode` option is required
to switch between the MySQL and Oracle dialect implementations.

:::

## Supported DataSource Info

| Datasource | Supported versions       | Driver                   | Url                                  | Maven                                                                       |
|------------|--------------------------|--------------------------|--------------------------------------|-----------------------------------------------------------------------------|
| OceanBase  | All OceanBase server versions. | com.oceanbase.jdbc.Driver | jdbc:oceanbase://localhost:2883/test | [Download](https://mvnrepository.com/artifact/com.oceanbase/oceanbase-client) |

## Database Dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATUNNEL_HOME/plugins/jdbc/lib/' working directory<br/>
> For example: cp oceanbase-client-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## Data Type Mapping

### MySQL Mode

| Mysql Data type                                                                                | SeaTunnel Data type                                                                                                                |
|------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| BIT(1)<br/>INT UNSIGNED                                                                        | BOOLEAN                                                                                                                            |
| TINYINT<br/>TINYINT UNSIGNED<br/>SMALLINT<br/>SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED<br/>INT<br/>INTEGER<br/>YEAR | INT |
| INT UNSIGNED<br/>INTEGER UNSIGNED<br/>BIGINT                                                   | BIGINT                                                                                                                             |
| BIGINT UNSIGNED                                                                                | DECIMAL(20,0)                                                                                                                      |
| DECIMAL(x,y) (column size < 38)                                                                | DECIMAL(x,y)                                                                                                                       |
| DECIMAL(x,y) (column size >= 38)                                                               | DECIMAL(38,18)                                                                                                                     |
| DECIMAL UNSIGNED                                                                               | DECIMAL((column size + 1), (right-of-decimal digits))                                                                             |
| FLOAT<br/>FLOAT UNSIGNED                                                                       | FLOAT                                                                                                                              |
| DOUBLE<br/>DOUBLE UNSIGNED                                                                     | DOUBLE                                                                                                                             |
| CHAR<br/>VARCHAR<br/>TINYTEXT<br/>MEDIUMTEXT<br/>TEXT<br/>LONGTEXT<br/>JSON                     | STRING                                                                                                                             |
| DATE                                                                                           | DATE                                                                                                                               |
| TIME                                                                                           | TIME                                                                                                                               |
| DATETIME<br/>TIMESTAMP                                                                         | TIMESTAMP                                                                                                                          |
| TINYBLOB<br/>MEDIUMBLOB<br/>BLOB<br/>LONGBLOB<br/>BINARY<br/>VARBINARY<br/>BIT(n)              | BYTES                                                                                                                              |
| GEOMETRY<br/>UNKNOWN                                                                           | Not supported yet                                                                                                                  |

### Oracle Mode

| Oracle Data type                                              | SeaTunnel Data type |
|---------------------------------------------------------------|---------------------|
| Number(p), p <= 9                                             | INT                 |
| Number(p), p <= 18                                            | BIGINT              |
| Number(p), p > 18                                             | DECIMAL(38,18)      |
| REAL<br/>BINARY_FLOAT                                         | FLOAT               |
| BINARY_DOUBLE                                                 | DOUBLE              |
| CHAR<br/>NCHAR<br/>NVARCHAR2<br/>NCLOB<br/>CLOB<br/>ROWID     | STRING              |
| DATE                                                          | DATE                |
| TIMESTAMP<br/>TIMESTAMP WITH LOCAL TIME ZONE                  | TIMESTAMP           |
| BLOB<br/>RAW<br/>LONG RAW<br/>BFILE                           | BYTES               |
| UNKNOWN                                                       | Not supported yet   |

## Sink Options

| Name                              | Type    | Required | Default                          | Description                                                                                                                                                                                                                                                          |
|-----------------------------------|---------|----------|----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                               | String  | Yes      | -                                | The URL of the JDBC connection, for example `jdbc:oceanbase://localhost:2883/test`.                                                                                                                                                                                  |
| driver                            | String  | Yes      | -                                | The JDBC class name used to connect to the remote data source. Must be `com.oceanbase.jdbc.Driver`.                                                                                                                                                                  |
| username                          | String  | No       | -                                | Connection instance username.                                                                                                                                                                                                                                        |
| password                          | String  | No       | -                                | Connection instance password.                                                                                                                                                                                                                                         |
| query                             | String  | No       | -                                | SQL used to write upstream data to OceanBase, for example `INSERT ...`. When `generate_sink_sql = false`, `query` is required. Save mode options do not run in custom `query` mode.                                                                                  |
| compatible_mode                   | String  | Yes      | -                                | The compatible mode of OceanBase. Must be `mysql` or `oracle`.                                                                                                                                                                                                       |
| database                          | String  | No       | -                                | Database used when `generate_sink_sql = true`. This option is required when generated SQL is enabled.                                                                                                                                                                |
| table                             | String  | No       | -                                | Target table used when `generate_sink_sql = true`. Supports placeholders such as `${schema_name}` and `${table_name}` for multi-table writes.                                                                                                                       |
| primary_keys                      | Array   | No       | -                                | Used to support operations such as `insert`, `delete`, and `update` when SQL is generated automatically.                                                                                                                                                              |
| connection_check_timeout_sec      | Int     | No       | 30                               | Time in seconds to wait for the database operation used to validate the connection to complete.                                                                                                                                                                      |
| max_retries                       | Int     | No       | 0                                | The number of retries to submit a failed batch (`executeBatch`).                                                                                                                                                                                                    |
| batch_size                        | Int     | No       | 1000                             | For batch writing, when buffered record count reaches `batch_size`, data is flushed to OceanBase. If `batch_interval_ms > 0`, elapsed time can also trigger a flush.                                                                                                |
| batch_interval_ms                 | Long    | No       | 0                                | Write-triggered flush interval in milliseconds. `0` disables time-based flushing. When greater than 0, the writer checks elapsed time on each record and flushes synchronously when the interval is reached.                                                       |
| is_exactly_once                   | Boolean | No       | false                            | Whether to enable exactly-once semantics through XA transactions. If enabled, set `xa_data_source_class_name` and keep `max_retries = 0`.                                                                                                                            |
| generate_sink_sql                 | Boolean | No       | false                            | Generate SQL statements based on the database table you want to write to.                                                                                                                                                                                            |
| xa_data_source_class_name         | String  | No       | -                                | XA data source class name from the OceanBase JDBC driver. Required when `is_exactly_once = true`.                                                                                                                                                                    |
| max_commit_attempts               | Int     | No       | 3                                | The number of retries for transaction commit failures.                                                                                                                                                                                                               |
| transaction_timeout_sec           | Int     | No       | -1                               | The timeout after the transaction is opened. `-1` means never time out. Note that setting a timeout may affect exactly-once semantics.                                                                                                                                |
| auto_commit                       | Boolean | No       | true                             | Whether automatic transaction commit is enabled.                                                                                                                                                                                                                     |
| field_ide                         | String  | No       | -                                | Controls field name case conversion. Available values: `ORIGINAL`, `UPPERCASE`, `LOWERCASE`.                                                                                                                                                                          |
| properties                        | Map     | No       | -                                | Additional connection configuration parameters. When properties and the URL have the same parameters, the priority depends on the driver implementation; for example, MySQL drivers give `properties` priority over the URL.                                        |
| schema_save_mode                  | Enum    | No       | CREATE_SCHEMA_WHEN_NOT_EXIST      | Controls how the target table structure is handled before the job starts. See [schema_save_mode](#schema_save_mode) below.                                                                                                                                          |
| data_save_mode                    | Enum    | No       | APPEND_DATA                      | Controls how existing target table data is handled before the job starts. See [data_save_mode](#data_save_mode) below.                                                                                                                                               |
| custom_sql                        | String  | No       | -                                | SQL executed before synchronization when `data_save_mode = CUSTOM_PROCESSING`. Not executed in custom `query` mode.                                                                                                                                                 |
| enable_upsert                     | Boolean | No       | true                             | Enable upsert by `primary_keys`. If the task has no key duplicate data, setting this to `false` can speed up data import.                                                                                                                                           |
| is_primary_key_updated            | Boolean | No       | true                             | Whether primary key fields are included when generated update statements are built.                                                                                                                                                                                  |
| support_upsert_by_insert_only     | Boolean | No       | false                            | Whether to support upsert behavior through insert-only statements for compatible dialects.                                                                                                                                                                          |
| multi_table_sink_replica          | Int     | No       | 1                                | Number of sink writer replicas used when writing multiple tables.                                                                                                                                                                                                   |
| common-options                    |         | No       | -                                | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.                                                                                                                                          |

### schema_save_mode

Controls how the target table structure is handled before the job starts.

- `RECREATE_SCHEMA`: Drop the table and recreate it.
- `CREATE_SCHEMA_WHEN_NOT_EXIST` (default): Create the table when it does not
  exist, skip when it exists.
- `ERROR_WHEN_SCHEMA_NOT_EXIST`: Report an error when the table does not exist.
- `IGNORE`: Skip table handling.

### data_save_mode

Controls how existing target table data is handled before the job starts.

- `DROP_DATA`: Preserve the database structure and delete the data.
- `APPEND_DATA` (default): Preserve both the database structure and the data.
- `CUSTOM_PROCESSING`: Run a user-provided `custom_sql` before the job starts.
- `ERROR_WHEN_DATA_EXISTS`: Report an error when data already exists.

### Tips

> Configure `compatible_mode = "mysql"` for OceanBase MySQL mode and
> `compatible_mode = "oracle"` for OceanBase Oracle mode.
>
> Use `query` when you want to provide the full write SQL yourself. Use
> `generate_sink_sql = true` together with `database` and `table` when you want
> SeaTunnel to generate INSERT/UPSERT SQL and apply save mode settings.
>
> To consume CDC data, use generated SQL and configure `primary_keys`;
> otherwise UPDATE and DELETE events cannot be mapped safely.

## Task Example

### Simple

> This example defines a SeaTunnel job that automatically generates data through
> FakeSource and sends it to the OceanBase sink. `FakeSource` generates 16 rows
> in total (row.num=16), each with two fields: `name` (string) and `age` (int).
> The target table `test_table` will also contain 16 rows. Before running this
> job, create the `test` database and `test_table` in your OceanBase cluster.
> If you have not yet installed and deployed SeaTunnel, follow the
> [Install SeaTunnel](../../getting-started/locally/deployment.md) and
> [Quick Start With SeaTunnel Engine](../../getting-started/locally/quick-start-seatunnel-engine.md)
> guides.

```hocon
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
}

transform {
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
}
```

### Generate Sink SQL

> Use this option when you do not want to write complex SQL by hand; SeaTunnel
> generates the INSERT statement for you.

```hocon
sink {
  jdbc {
    url = "jdbc:oceanbase://localhost:2883/test"
    driver = "com.oceanbase.jdbc.Driver"
    username = "root"
    password = "123456"
    compatible_mode = "mysql"
    generate_sink_sql = true
    database = test
    table = test_table
  }
}
```

### Generated SQL With Save Mode

```hocon
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

### CDC (Change Data Capture) Event

> CDC change data is supported. Configure `database`, `table`, and
> `primary_keys`.

```hocon
sink {
  jdbc {
    url = "jdbc:oceanbase://localhost:3306/test"
    driver = "com.oceanbase.jdbc.Driver"
    username = "root"
    password = "123456"
    compatible_mode = "mysql"
    generate_sink_sql = true
    database = test
    table = sink_table
    primary_keys = ["id", "name"]
  }
}
```

### Oracle-Compatible Mode

```hocon
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

Use placeholders in `table` when upstream rows carry table identity.

```hocon
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

## Changelog

<ChangeLog />