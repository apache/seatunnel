# StarRocks

> StarRocks sink connector

## Support These Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

## Description

Used to send data to StarRocks. Both support streaming and batch mode.
The internal implementation of StarRocks sink connector is cached and imported by stream load in batches.

## Sink Options

|            Name             |  Type   | Required |     Default     |                                                                                                    Description                                                                                                    |
|-----------------------------|---------|----------|-----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| nodeUrls                    | list    | yes      | -               | `StarRocks` cluster address, the format is `["fe_ip:fe_http_port", ...]`                                                                                                                                          |
| base-url                    | string  | yes      | -               | The JDBC URL like `jdbc:mysql://localhost:9030/` or `jdbc:mysql://localhost:9030` or `jdbc:mysql://localhost:9030/db`                                                                                             |
| username                    | string  | yes      | -               | `StarRocks` user username                                                                                                                                                                                         |
| password                    | string  | yes      | -               | `StarRocks` user password                                                                                                                                                                                         |
| database                    | string  | yes      | -               | The name of StarRocks database                                                                                                                                                                                    |
| table                       | string  | no       | -               | The name of StarRocks table, If not set, the table name will be the name of the upstream table                                                                                                                    |
| labelPrefix                 | string  | no       | -               | The prefix of StarRocks stream load label                                                                                                                                                                         |
| batch_max_rows              | long    | no       | 1024            | For batch writing, when the number of buffers reaches the number of `batch_max_rows` or the byte size of `batch_max_bytes` or the time reaches `checkpoint.interval`, the data will be flushed into the StarRocks |
| batch_max_bytes             | int     | no       | 5 * 1024 * 1024 | For batch writing, when the number of buffers reaches the number of `batch_max_rows` or the byte size of `batch_max_bytes` or the time reaches `checkpoint.interval`, the data will be flushed into the StarRocks |
| max_retries                 | int     | no       | -               | The number of retries to flush failed                                                                                                                                                                             |
| retry_backoff_multiplier_ms | int     | no       | -               | Using as a multiplier for generating the next delay for backoff                                                                                                                                                   |
| max_retry_backoff_ms        | int     | no       | -               | The amount of time to wait before attempting to retry a request to `StarRocks`                                                                                                                                    |
| enable_upsert_delete        | boolean | no       | false           | Whether to enable upsert/delete, only supports PrimaryKey model.                                                                                                                                                  |
| save_mode_create_template   | string  | no       | see below       | see below                                                                                                                                                                                                         |
| starrocks.config            | map     | no       | -               | The parameter of the stream load `data_desc`                                                                                                                                                                      |

### save_mode_create_template

We use templates to automatically create starrocks tables,
which will create corresponding table creation statements based on the type of upstream data and schema type,
and the default template can be modified according to the situation. Only work on multi-table mode at now.

```sql
CREATE TABLE IF NOT EXISTS `${database}`.`${table_name}`
(
    ${rowtype_fields}
) ENGINE = OLAP DISTRIBUTED BY HASH (${rowtype_primary_key})
    PROPERTIES
(
    "replication_num" = "1"
);
```

If a custom field is filled in the template, such as adding an `id` field

```sql
CREATE TABLE IF NOT EXISTS `${database}`.`${table_name}`
(   
    id,
    ${rowtype_fields}
) ENGINE = OLAP DISTRIBUTED BY HASH (${rowtype_primary_key})
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

## Changelog

### next version

- Add StarRocks Sink Connector
- [Improve] Change Connector Custom Config Prefix To Map [3719](https://github.com/apache/seatunnel/pull/3719)
- [Feature] Support write cdc changelog event(INSERT/UPDATE/DELETE) [3865](https://github.com/apache/seatunnel/pull/3865)

