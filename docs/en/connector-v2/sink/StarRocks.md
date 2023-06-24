# StarRocks

> StarRocks sink connector

## Description

Used to send data to StarRocks. Both support streaming and batch mode.
The internal implementation of StarRocks sink connector is cached and imported by stream load in batches.

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

## Options

|            name             |  type   | required |  default value  |
|-----------------------------|---------|----------|-----------------|
| nodeUrls                    | list    | yes      | -               |
| base-url                    | string  | yes      | -               |
| username                    | string  | yes      | -               |
| password                    | string  | yes      | -               |
| database                    | string  | yes      | -               |
| table                       | string  | no       | -               |
| labelPrefix                 | string  | no       | -               |
| batch_max_rows              | long    | no       | 1024            |
| batch_max_bytes             | int     | no       | 5 * 1024 * 1024 |
| batch_interval_ms           | int     | no       | -               |
| max_retries                 | int     | no       | -               |
| retry_backoff_multiplier_ms | int     | no       | -               |
| max_retry_backoff_ms        | int     | no       | -               |
| enable_upsert_delete        | boolean | no       | false           |
| save_mode_create_template   | string  | no       | see below       |
| starrocks.config            | map     | no       | -               |

### nodeUrls [list]

`StarRocks` cluster address, the format is `["fe_ip:fe_http_port", ...]`

### base-url [string]

The JDBC URL like `jdbc:mysql://localhost:9030/` or `jdbc:mysql://localhost:9030` or `jdbc:mysql://localhost:9030/db`

### username [string]

`StarRocks` user username

### password [string]

`StarRocks` user password

### database [string]

The name of StarRocks database

### table [string]

The name of StarRocks table, If not set, the table name will be the name of the upstream table

### labelPrefix [string]

The prefix of StarRocks stream load label

### batch_max_rows [long]

For batch writing, when the number of buffers reaches the number of `batch_max_rows` or the byte size of `batch_max_bytes` or the time reaches `batch_interval_ms`, the data will be flushed into the StarRocks

### batch_max_bytes [int]

For batch writing, when the number of buffers reaches the number of `batch_max_rows` or the byte size of `batch_max_bytes` or the time reaches `batch_interval_ms`, the data will be flushed into the StarRocks

### batch_interval_ms [int]

For batch writing, when the number of buffers reaches the number of `batch_max_rows` or the byte size of `batch_max_bytes` or the time reaches `batch_interval_ms`, the data will be flushed into the StarRocks

### max_retries [int]

The number of retries to flush failed

### retry_backoff_multiplier_ms [int]

Using as a multiplier for generating the next delay for backoff

### max_retry_backoff_ms [int]

The amount of time to wait before attempting to retry a request to `StarRocks`

### enable_upsert_delete [boolean]

Whether to enable upsert/delete, only supports PrimaryKey model.

### save_mode_create_template [string]

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

### starrocks.config  [map]

The parameter of the stream load `data_desc`

#### Supported import data formats

The supported formats include CSV and JSON. Default value: JSON

## Example

Use JSON format to import data

```hocon
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

Use CSV format to import data

```hocon
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

Support write cdc changelog event(INSERT/UPDATE/DELETE)

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

## Changelog

### next version

- Add StarRocks Sink Connector
- [Improve] Change Connector Custom Config Prefix To Map [3719](https://github.com/apache/seatunnel/pull/3719)
- [Feature] Support write cdc changelog event(INSERT/UPDATE/DELETE) [3865](https://github.com/apache/seatunnel/pull/3865)

