import ChangeLog from '../changelog/connector-selectdb-cloud.md';

# SelectDB Cloud

> SelectDB Cloud sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Description

Used to send data to SelectDB Cloud. Both support streaming and batch mode.
The internal implementation of SelectDB Cloud sink connector upload after batch caching and commit the CopyInto sql to load data into the table.

## Supported DataSource Info

:::tip

Version Supported

* supported  `SelectDB Cloud version is >= 2.2.x`

:::

## Sink Options

|        Name        |  Type  | Required |        Default         |                                                                                                                                                                    Description                                                                                                                                                                    |
|--------------------|--------|----------|------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| load-url           | String | Yes      | -                      | `SelectDB Cloud` warehouse HTTP address, in the format `warehouse_ip:http_port`. Must not be empty or whitespace-only.                                                                                                                                                                                                                            |
| jdbc-url           | String | Yes      | -                      | `SelectDB Cloud` warehouse JDBC address, in the format `warehouse_ip:mysql_port`. Must not be empty or whitespace-only.                                                                                                                                                                                                                           |
| cluster-name       | String | Yes      | -                      | `SelectDB Cloud` cluster name. Must not be empty or whitespace-only.                                                                                                                                                                                                                                                                               |
| username           | String | Yes      | -                      | `SelectDB Cloud` username. Must not be empty or whitespace-only.                                                                                                                                                                                                                                                                                   |
| password           | String | No       | -                      | Optional `SelectDB Cloud` user password.                                                                                                                                                                                                                                                                                                          |
| sink.enable-2pc    | bool   | No       | true                   | Whether to enable two-phase commit (2pc), the default is true, to ensure Exactly-Once semantics. SelectDB uses cache files to load data. When the amount of data is large, cached data may become invalid (the default expiration time is 1 hour). If you encounter a large amount of data write loss, please configure sink.enable-2pc to false. |
| table.identifier   | String | Yes      | -                      | The name of the `SelectDB Cloud` table, in the format `database.table`. Must not be empty or whitespace-only.                                                                                                                                                                                                                                     |
| sink.enable-delete | bool   | No       | false                  | Whether to enable deletion. This option requires SelectDB Cloud table to enable batch delete function, and only supports Unique model.                                                                                                                                                                                                            |
| sink.max-retries   | int    | No       | 3                      | the max retry times if writing records to database failed                                                                                                                                                                                                                                                                                         |
| sink.buffer-size   | int    | No       | 10 * 1024 * 1024 (1MB) | the buffer size to cache data for stream load.                                                                                                                                                                                                                                                                                                    |
| sink.buffer-count  | int    | No       | 10000                  | the buffer count to cache data for stream load.                                                                                                                                                                                                                                                                                                   |
| selectdb.config    | map    | yes      | -                      | This option is used to support operations such as `insert`, `delete`, and `update` when automatically generate sql,and supported formats.                                                                                                                                                                                                         |

## Data Type Mapping

| SelectDB Cloud Data type |           SeaTunnel Data type           |
|--------------------------|-----------------------------------------|
| BOOLEAN                  | BOOLEAN                                 |
| TINYINT                  | TINYINT                                 |
| SMALLINT                 | SMALLINT<br/>TINYINT                    |
| INT                      | INT<br/>SMALLINT<br/>TINYINT            |
| BIGINT                   | BIGINT<br/>INT<br/>SMALLINT<br/>TINYINT |
| LARGEINT                 | BIGINT<br/>INT<br/>SMALLINT<br/>TINYINT |
| FLOAT                    | FLOAT                                   |
| DOUBLE                   | DOUBLE<br/>FLOAT                        |
| DECIMAL                  | DECIMAL<br/>DOUBLE<br/>FLOAT            |
| DATE                     | DATE                                    |
| DATETIME                 | TIMESTAMP                               |
| CHAR                     | STRING                                  |
| VARCHAR                  | STRING                                  |
| STRING                   | STRING                                  |
| ARRAY                    | ARRAY                                   |
| MAP                      | MAP                                     |
| JSON                     | STRING                                  |
| HLL                      | Not supported yet                       |
| BITMAP                   | Not supported yet                       |
| QUANTILE_STATE           | Not supported yet                       |
| STRUCT                   | Not supported yet                       |

#### Supported import data formats

The supported formats include CSV and JSON

## Task Example

### Simple

> The following example describes writing multiple data types to SelectDBCloud, and users need to create corresponding tables downstream

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
  SelectDBCloud {
    load-url = "warehouse_ip:http_port"
    jdbc-url = "warehouse_ip:mysql_port"
    cluster-name = "Cluster"
    table.identifier = "test.test"
    username = "admin"
    password = "******"
    selectdb.config {
        file.type = "json"
    }
  }
}
```

### Use JSON format to import data

```
sink {
  SelectDBCloud {
    load-url = "warehouse_ip:http_port"
    jdbc-url = "warehouse_ip:mysql_port"
    cluster-name = "Cluster"
    table.identifier = "test.test"
    username = "admin"
    password = "******"
    selectdb.config {
        file.type = "json"
    }
  }
}

```

### Use CSV format to import data

```
sink {
  SelectDBCloud {
    load-url = "warehouse_ip:http_port"
    jdbc-url = "warehouse_ip:mysql_port"
    cluster-name = "Cluster"
    table.identifier = "test.test"
    username = "admin"
    password = "******"
    selectdb.config {
        file.type = "csv"
        file.column_separator = "," 
        file.line_delimiter = "\n" 
    }
  }
}
```

## Changelog

<ChangeLog />
