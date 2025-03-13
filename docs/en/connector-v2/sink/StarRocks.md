# StarRocks

> StarRocks sink connector

## Support These Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)
- [x] [support multiple table write](../../concept/connector-v2-features.md)

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

### schema_save_mode[Enum]

Before the synchronous task is turned on, different treatment schemes are selected for the existing surface structure of the target side.  
Option introduction：  
`RECREATE_SCHEMA` ：Will create when the table does not exist, delete and rebuild when the table is saved        
`CREATE_SCHEMA_WHEN_NOT_EXIST` ：Will Created when the table does not exist, skipped when the table is saved        
`ERROR_WHEN_SCHEMA_NOT_EXIST` ：Error will be reported when the table does not exist  
`IGNORE` ：Ignore the treatment of the table

### data_save_mode[Enum]

Before the synchronous task is turned on, different processing schemes are selected for data existing data on the target side.  
Option introduction：  
`DROP_DATA`： Preserve database structure and delete data  
`APPEND_DATA`：Preserve database structure, preserve data  
`CUSTOM_PROCESSING`：User defined processing  
`ERROR_WHEN_DATA_EXISTS`：When there is data, an error is reported

### custom_sql[String]

When data_save_mode selects CUSTOM_PROCESSING, you should fill in the CUSTOM_SQL parameter. This parameter usually fills in a SQL that can be executed. SQL will be executed before synchronization tasks.

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

### Simple:

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

```
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

```
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

```
sink {
  StarRocks {
    nodeUrls = ["e2e_starRocksdb:8030"]
    base-url = "jdbc:mysql://e2e_starRocksdb:9030/"
    username = root
    password = ""
    database = "test"
    table = "test_${schema_name}_${table_name}"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode="APPEND_DATA"
    batch_max_rows = 10
    starrocks.config = {
      format = "CSV"
      column_separator = "\\x01"
      row_delimiter = "\\x02"
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

## Changelog

### next version

- Add StarRocks Sink Connector
- [Improve] Change Connector Custom Config Prefix To Map [3719](https://github.com/apache/seatunnel/pull/3719)
- [Feature] Support write cdc changelog event(INSERT/UPDATE/DELETE) [3865](https://github.com/apache/seatunnel/pull/3865)

