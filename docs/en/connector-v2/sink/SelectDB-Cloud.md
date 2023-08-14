# SelectDB Cloud

> SelectDB Cloud sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

## Description

Used to send data to SelectDB Cloud. Both support streaming and batch mode.
The internal implementation of SelectDB Cloud sink connector upload after batch caching and commit the CopyInto sql to load data into the table.

## Supported DataSource Info

:::tip

Version Supported

* supported  `SelectDB Cloud version is >= 2.2.x`

:::

## Sink Options

|        Name        |  Type  | Required |        Default         |                                                                Description                                                                |
|--------------------|--------|----------|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| load-url           | String | Yes      | -                      | `SelectDB Cloud` warehouse http address, the format is `warehouse_ip:http_port`                                                           |
| jdbc-url           | String | Yes      | -                      | `SelectDB Cloud` warehouse jdbc address, the format is `warehouse_ip:mysql_port`                                                          |
| cluster-name       | String | Yes      | -                      | `SelectDB Cloud` cluster name                                                                                                             |
| username           | String | Yes      | -                      | `SelectDB Cloud` user username                                                                                                            |
| password           | String | Yes      | -                      | `SelectDB Cloud` user password                                                                                                            |
| table.identifier   | String | Yes      | -                      | The name of `SelectDB Cloud` table, the format is `database.table`                                                                        |
| sink.enable-delete | bool   | No       | false                  | Whether to enable deletion. This option requires SelectDB Cloud table to enable batch delete function, and only supports Unique model.    |
| sink.max-retries   | int    | No       | 3                      | the max retry times if writing records to database failed                                                                                 |
| sink.buffer-size   | int    | No       | 10 * 1024 * 1024 (1MB) | the buffer size to cache data for stream load.                                                                                            |
| sink.buffer-count  | int    | No       | 10000                  | the buffer count to cache data for stream load.                                                                                           |
| selectdb.config    | map    | yes      | -                      | This option is used to support operations such as `insert`, `delete`, and `update` when automatically generate sql,and supported formats. |

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

### Simple:

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

### next version

- [Feature] Support SelectDB Cloud Sink Connector [3958](https://github.com/apache/seatunnel/pull/3958)
- [Improve] Refactor some SelectDB Cloud Sink code as well as support copy into batch and async flush and cdc [4312](https://github.com/apache/seatunnel/pull/4312)

