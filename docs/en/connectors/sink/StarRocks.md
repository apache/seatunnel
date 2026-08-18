import ChangeLog from '../changelog/connector-starrocks.md';

# StarRocks

> StarRocks sink connector

## Support These Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [x] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Description

Used to send data to StarRocks. Both support streaming and batch mode.
The internal implementation of StarRocks sink connector is cached and imported by stream load in batches.

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Sink Options

|            Name             |  Type   | Required |           Default            |                                                                                                    Description                                                                                                    |
|-----------------------------|---------|----------|------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| nodeUrls                    | list    | yes      | -                            | `StarRocks` cluster address, the format is `["fe_ip:fe_http_port", ...]`                                                                                                                                          |
| base-url                    | string  | yes      | -                            | The JDBC URL like `jdbc:mysql://localhost:9030/` or `jdbc:mysql://localhost:9030` or `jdbc:mysql://localhost:9030/db`                                                                                             |
| username                    | string  | yes      | -                            | `StarRocks` user username                                                                                                                                                                                         |
| password                    | string  | yes      | -                            | `StarRocks` user password                                                                                                                                                                                         |
| database                    | string  | yes      | -                            | The name of StarRocks database                                                                                                                                                                                    |
| table                       | string  | no       | -                            | The name of StarRocks table, If not set, the table name will be the name of the upstream table                                                                                                                    |
| labelPrefix                 | string  | no       | -                            | The prefix of StarRocks stream load label                                                                                                                                                                         |
| batch_max_rows              | long    | no       | 1024                         | For batch writing, when the number of buffers reaches the number of `batch_max_rows` or the byte size of `batch_max_bytes` or the time reaches `checkpoint.interval`, the data will be flushed into the StarRocks |
| batch_max_bytes             | int     | no       | 5 * 1024 * 1024              | For batch writing, when the number of buffers reaches the number of `batch_max_rows` or the byte size of `batch_max_bytes` or the time reaches `checkpoint.interval`, the data will be flushed into the StarRocks |
| max_retries                 | int     | no       | -                            | The number of retries to flush failed                                                                                                                                                                             |
| retry_backoff_multiplier_ms | int     | no       | -                            | Using as a multiplier for generating the next delay for backoff                                                                                                                                                   |
| max_retry_backoff_ms        | int     | no       | -                            | The amount of time to wait before attempting to retry a request to `StarRocks`                                                                                                                                    |
| enable_upsert_delete        | boolean | no       | false                        | Whether to enable upsert/delete, only supports PrimaryKey model.                                                                                                                                                  |
| save_mode_create_template   | string  | no       | see below                    | see below                                                                                                                                                                                                         |
| starrocks.config            | map     | no       | -                            | The parameter of the stream load `data_desc`                                                                                                                                                                      |
| http_socket_timeout_ms      | int     | no       | 180000                       | Set http socket timeout, default is 3 minutes.                                                                                                                                                                    |
| schema_save_mode            | Enum    | no       | CREATE_SCHEMA_WHEN_NOT_EXIST | Before the synchronous task is turned on, different treatment schemes are selected for the existing surface structure of the target side.                                                                         |
| data_save_mode              | Enum    | no       | APPEND_DATA                  | Before the synchronous task is turned on, different processing schemes are selected for data existing data on the target side.                                                                                    |
| table_options               | Map     | no       | -                            | Sink-specific table properties merged into CREATE TABLE PROPERTIES during SaveMode auto-create. See below.                                                                                                          |
| custom_sql                  | String  | no       | -                            | When data_save_mode selects CUSTOM_PROCESSING, you should fill in the CUSTOM_SQL parameter. This parameter usually fills in a SQL that can be executed. SQL will be executed before synchronization tasks.        |

### save_mode_create_template

We use templates to automatically create starrocks tables,
which will create corresponding table creation statements based on the type of upstream data and schema type,
and the default template can be modified according to the situation. Only work on multi-table mode at now.

Default template:

```sql
CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (
${rowtype_primary_key},
${rowtype_fields}
) ENGINE=OLAP
PRIMARY KEY (${rowtype_primary_key})
COMMENT '${comment}'
DISTRIBUTED BY HASH (${rowtype_primary_key})PROPERTIES (
"replication_num" = "1"
)
```

If a custom field is filled in the template, such as adding an `id` field

```sql
CREATE TABLE IF NOT EXISTS `${database}`.`${table}`
(   
    id,
    ${rowtype_fields}
) ENGINE = OLAP 
    COMMENT '${comment}'
    DISTRIBUTED BY HASH (${rowtype_primary_key})
    PROPERTIES
(
    "replication_num" = "1"
);
```

The connector will automatically obtain the corresponding type from the upstream to complete the filling,
and remove the id field from `rowtype_fields`. This method can be used to customize the modification of field types and attributes.

You can use the following placeholders

- database: Used to get the database in the upstream schema
- table_name: Used to get the table name in the upstream schema
- rowtype_fields: Used to get all the fields in the upstream schema, we will automatically map to the field
  description of StarRocks
- rowtype_primary_key: Used to get the primary key in the upstream schema (maybe a list)
- rowtype_unique_key: Used to get the unique key in the upstream schema (maybe a list)
- comment: Used to get the table comment in the upstream schema

### table [string]

Use `database` and this `table-name` auto-generate sql and receive upstream input datas write to database.

This option is mutually exclusive with `query` and has a higher priority.

The table parameter can fill in the name of an unwilling table, which will eventually be used as the table name of the creation table, and supports variables (`${table_name}`, `${schema_name}`). Replacement rules: `${schema_name}` will replace the SCHEMA name passed to the target side, and `${table_name}` will replace the name of the table passed to the table at the target side.

for example:
1. test_${schema_name}_${table_name}_test
2. sink_sinktable
3. ss_${table_name}

### schema_save_mode [Enum]

Before the synchronous task is turned on, different treatment schemes are selected for the existing surface structure of the target side.  
Option introduction：  
`RECREATE_SCHEMA` ：Will create when the table does not exist, delete and rebuild when the table is saved        
`CREATE_SCHEMA_WHEN_NOT_EXIST` ：Will Created when the table does not exist, skipped when the table is saved        
`ERROR_WHEN_SCHEMA_NOT_EXIST` ：Error will be reported when the table does not exist  
`IGNORE` ：Ignore the treatment of the table

### data_save_mode [Enum]

Before the synchronous task is turned on, different processing schemes are selected for data existing data on the target side.  
Option introduction：  
`DROP_DATA`： Preserve database structure and delete data  
`APPEND_DATA`：Preserve database structure, preserve data  
`CUSTOM_PROCESSING`：User defined processing  
`ERROR_WHEN_DATA_EXISTS`：When there is data, an error is reported

### custom_sql [String]

When data_save_mode selects CUSTOM_PROCESSING, you should fill in the CUSTOM_SQL parameter. This parameter usually fills in a SQL that can be executed. SQL will be executed before synchronization tasks.

### table_options [Map]

Sink-specific table options applied when SaveMode auto-creates the target table (DDL phase). They take effect only when `schema_save_mode` triggers table creation, such as `CREATE_SCHEMA_WHEN_NOT_EXIST` or `RECREATE_SCHEMA`. They do **not** affect Stream Load writes at runtime and do **not** run `ALTER TABLE` on existing tables.

When used with the default `save_mode_create_template` (option omitted, or configured with the same content as the built-in default), `table_options` are merged into the template `PROPERTIES` clause. **Duplicate keys are overridden by `table_options`.** Use property names from the [StarRocks CREATE TABLE documentation](https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE/#properties); SeaTunnel does not maintain an allowlist—invalid properties fail when StarRocks executes the CREATE TABLE statement.

If you configure a `save_mode_create_template` that **differs from the built-in default**, `table_options` cannot be used together (validation fails at job submission). Put properties directly in the template instead.

Invalid combinations are validated early via `StarRocksSinkFactory` option rules (`--check` and job submission), not only when StarRocks executes CREATE TABLE.

Example:

```hocon
sink {
  StarRocks {
    base-url = "jdbc:mysql://127.0.0.1:9030"
    nodeUrls = ["127.0.0.1:8030"]
    username = "root"
    password = ""
    database = "test"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    table_options = {
      replication_num = "3"
      storage_format = "V2"
    }
  }
}
```

### Zeta Timer Flush

This engine-level capability is available only in Zeta. Configure `sink.flush.interval` in `env` to periodically write buffered rows through StarRocks Stream Load even when `batch_max_rows` and `batch_max_bytes` have not been reached. Spark and Flink do not trigger this scheduled flush.

:::tip

StarRocks timer flush does not provide 2PC exactly-once semantics. The StarRocks Sink remains at-least-once, and a task restart may submit rows again. When appropriate for the workload, a Primary Key table with deterministic keys can absorb duplicate writes.

:::

```hocon
env {
  job.mode = "STREAMING"
  checkpoint.interval = 300000
  sink.flush.interval = 5000
}

sink {
  StarRocks {
    nodeUrls = ["starrocks-fe:8030"]
    base-url = "jdbc:mysql://starrocks-fe:9030/mydb"
    username = root
    password = ""
    database = "mydb"
    table = "mytable"
    batch_max_rows = 10000
    batch_max_bytes = 104857600
  }
}
```

## Data Type Mapping

| StarRocks Data type | SeaTunnel Data type |
|---------------------|---------------------|
| BOOLEAN             | BOOLEAN             |
| TINYINT             | TINYINT             |
| SMALLINT            | SMALLINT            |
| INT                 | INT                 |
| BIGINT              | BIGINT              |
| FLOAT               | FLOAT               |
| DOUBLE              | DOUBLE              |
| DECIMAL             | DECIMAL             |
| DATE                | STRING              |
| TIME                | STRING              |
| DATETIME            | STRING              |
| STRING              | STRING              |
| ARRAY               | STRING              |
| MAP                 | STRING              |
| BYTES               | STRING              |

#### Supported import data formats

The supported formats include CSV and JSON

## Task Example

### Simple

> The following example describes writing multiple data types to StarRocks, and users need to create corresponding tables downstream

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
  checkpoint.interval = 10000
}

source {
  FakeSource {
    row.num = 10
    map.size = 10
    array.size = 10
    bytes.length = 10
    string.length = 10
    schema = {
      fields {
        c_map = "map<string, array<int>>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(16, 1)"
        c_null = "null"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
    }
}

sink {
  StarRocks {
    nodeUrls = ["e2e_starRocksdb:8030"]
    username = root
    password = ""
    database = "test"
    table = "e2e_table_sink"
    batch_max_rows = 10
    starrocks.config = {
      format = "JSON"
      strip_outer_array = true
    }
  }
}
```

### Support write cdc changelog event(INSERT/UPDATE/DELETE)

```hocon
sink {
  StarRocks {
    nodeUrls = ["e2e_starRocksdb:8030"]
    username = root
    password = ""
    database = "test"
    table = "e2e_table_sink"
    ...
    
    // Support upsert/delete event synchronization (enable_upsert_delete=true), only supports PrimaryKey model.
    enable_upsert_delete = true
  }
}
```

### Use JSON format to import data

```hocon
sink {
  StarRocks {
    nodeUrls = ["e2e_starRocksdb:8030"]
    base-url = "jdbc:mysql://e2e_starRocksdb:9030/"
    username = root
    password = ""
    database = "test"
    table = "e2e_table_sink"
    batch_max_rows = 10
    starrocks.config = {
      format = "JSON"
      strip_outer_array = true
    }
  }
}
```

### Use CSV format to import data

```hocon
sink {
  StarRocks {
    nodeUrls = ["e2e_starRocksdb:8030"]
    base-url = "jdbc:mysql://e2e_starRocksdb:9030/"
    username = root
    password = ""
    database = "test"
    table = "e2e_table_sink"
    batch_max_rows = 10
    starrocks.config = {
      format = "CSV"
      column_separator = "\\x01"
      row_delimiter = "\\x02"
    }
  }
}
```

### Use save_mode function

```hocon
sink {
  StarRocks {
    nodeUrls = ["e2e_starRocksdb:8030"]
    base-url = "jdbc:mysql://e2e_starRocksdb:9030/"
    username = root
    password = ""
    database = "test"
    table = "test_${schema_name}_${table_name}"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
    batch_max_rows = 10
    starrocks.config = {
      format = "CSV"
      column_separator = "\\x01"
      row_delimiter = "\\x02"
    }
  }
}
```

### CDC With Schema Change

This example shows MySQL-CDC streaming into StarRocks with `schema-changes.enabled = true` so that
upstream MySQL DDL changes (column additions, type widening, etc.) are applied to the target
StarRocks Primary Key table.

```hocon
env {
  job.mode = "STREAMING"
  checkpoint.interval = 2000
}

source {
  MySQL-CDC {
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["shop.products", "shop.orders", "shop.customers"]
    url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"

    schema-changes.enabled = true
  }
}

sink {
  StarRocks {
    nodeUrls = ["starrocks_cdc_e2e:8040"]
    base-url = "jdbc:mysql://starrocks_cdc_e2e:9030/shop"
    username = "root"
    password = ""
    database = "shop"
    table = "${table_name}"
    max_retries = 3
    enable_upsert_delete = true
    schema_save_mode = "RECREATE_SCHEMA"
    data_save_mode = "DROP_DATA"
    save_mode_create_template = """
    CREATE TABLE IF NOT EXISTS shop.`${table_name}` (
        ${rowtype_primary_key},
        ${rowtype_fields}
    ) ENGINE=OLAP
    PRIMARY KEY (${rowtype_primary_key})
    DISTRIBUTED BY HASH (${rowtype_primary_key})
    PROPERTIES (
        "replication_num" = "1",
        "in_memory" = "false",
        "enable_persistent_index" = "true",
        "replicated_storage" = "true",
        "compression" = "LZ4"
    )
    """
  }
}
```

### Timer Flush With MySQL-CDC

This example wires `sink.flush.interval` into the streaming job so that the StarRocks sink flushes
its buffer every 500 ms, independent of `batch_max_rows` and `batch_max_bytes`.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 300000
  sink.flush.interval = 500
}

source {
  MySQL-CDC {
    server-id = 5670
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["shop.products"]
    url = "jdbc:mysql://mysql_starrocks_timer_flush_e2e:3306/shop"
  }
}

sink {
  StarRocks {
    nodeUrls = ["starrocks_timer_flush_e2e:8030"]
    base-url = "jdbc:mysql://starrocks_timer_flush_e2e:9030/timer_flush"
    username = root
    password = ""
    database = "timer_flush"
    table = "starrocks_timer_flush"
    labelPrefix = "timer-flush-"
    batch_max_rows = 100000
    batch_max_bytes = 104857600
    schema_save_mode = "IGNORE"
    data_save_mode = "APPEND_DATA"
    starrocks.config = {
      format = "JSON"
    }
  }
}
```

### Multiple table

#### example1

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Mysql-CDC {
    base-url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
    username = "root"
    password = "******"
    
    table-names = ["seatunnel.role","seatunnel.user","galileo.Bucket"]
  }
}

transform {
}

sink {
  StarRocks {
    nodeUrls = ["e2e_starRocksdb:8030"]
    base-url = "jdbc:mysql://e2e_starRocksdb:9030/"
    username = root
    password = ""
    database = "${database_name}_test"
    table = "${table_name}_test"
    ...

    // Support upsert/delete event synchronization (enable_upsert_delete=true), only supports PrimaryKey model.
    enable_upsert_delete = true
  }
}
```

#### example2

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = oracle.jdbc.driver.OracleDriver
    url = "jdbc:oracle:thin:@localhost:1521/XE"
    user = testUser
    password = testPassword

    table_list = [
      {
        table_path = "TESTSCHEMA.TABLE_1"
      },
      {
        table_path = "TESTSCHEMA.TABLE_2"
      }
    ]
  }
}

transform {
}

sink {
  StarRocks {
    nodeUrls = ["e2e_starRocksdb:8030"]
    base-url = "jdbc:mysql://e2e_starRocksdb:9030/"
    username = root
    password = ""
    database = "${schema_name}_test"
    table = "${table_name}_test"
    ...

    // Support upsert/delete event synchronization (enable_upsert_delete=true), only supports PrimaryKey model.
    enable_upsert_delete = true
  }
}
```

## FAQ

### Does StarRocks Sink support automatic table creation?

Yes. Use `schema_save_mode` to control table creation behavior:

- `CREATE_SCHEMA_WHEN_NOT_EXIST`: Creates the table only if it does not exist.
- `RECREATE_SCHEMA`: Drops and recreates the table on every job start.
- `ERROR_WHEN_SCHEMA_NOT_EXIST`: Throws an error if the table is missing.
- `IGNORE`: Skips all table creation logic.

SeaTunnel infers StarRocks column types from the upstream schema automatically.

### Does StarRocks Sink support upsert and DELETE operations?

Yes. Enable upsert and DELETE propagation by setting `enable_upsert_delete = true`. This requires the target StarRocks table to use the **Primary Key** model. DELETE events from CDC sources are propagated correctly when this option is enabled.

### What is `labelPrefix` used for in StarRocks Sink?

`labelPrefix` controls the prefix of the Stream Load labels generated by the sink. StarRocks uses
these labels to deduplicate ingestion requests, so keeping this prefix stable and unique per job
helps avoid spurious "label already exists" errors across retries or restarts:

```hocon
sink {
  StarRocks {
    nodeUrls = ["starrocks-fe:8030"]
    base-url = "jdbc:mysql://starrocks-fe:9030/"
    username = root
    password = ""
    database = "mydb"
    table = "mytable"
    labelPrefix = "unique-job-label"
  }
}
```

Note that StarRocks Sink does not currently provide exactly-once delivery (see the **Key Features**
matrix at the top of this page). Using a stable `labelPrefix` reduces label collisions but does not
by itself give end-to-end exactly-once guarantees.

### Are StarRocks column names case-sensitive?

StarRocks column names are case-insensitive by default. Verify that upstream field names align with the target StarRocks column names to avoid unintended mapping issues.

### What is the difference between `nodeUrls` and `base-url`?

- `nodeUrls`: HTTP addresses of the StarRocks FE nodes, used for Stream Load data ingestion.
- `base-url`: JDBC URL pointing to a StarRocks FE node, used for DDL operations such as table creation and schema inspection.

Both parameters are required when automatic table creation is enabled.

## Changelog

<ChangeLog />
