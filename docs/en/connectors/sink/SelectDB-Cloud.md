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

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| load-url | String | Yes | - | SelectDB Cloud warehouse HTTP address. The format is `warehouse_ip:http_port`. |
| jdbc-url | String | Yes | - | SelectDB Cloud warehouse JDBC address. The format is `warehouse_ip:mysql_port`. |
| cluster-name | String | Yes | - | SelectDB Cloud cluster name. |
| username | String | Yes | - | SelectDB Cloud username. |
| password | String | No | - | SelectDB Cloud password. |
| table.identifier | String | Yes | - | SelectDB Cloud table name. The format is `database.table`. |
| sink.enable-2pc | Boolean | No | true | Whether to enable two-phase commit. When enabled, the connector can provide exactly-once semantics through the checkpoint commit path. |
| sink.enable-delete | Boolean | No | false | Whether to write delete events. The target SelectDB Cloud table must enable batch delete and use the Unique model. |
| sink.max-retries | Int | No | 3 | Maximum retry times when writing records fails. |
| sink.buffer-size | Int | No | 10485760 | Buffer size in bytes before uploading cached data. The default is 10 MB. |
| sink.buffer-count | Int | No | 10000 | Number of rows to cache before uploading data. |
| sink.label-prefix | String | No | Random UUID | Unique label prefix used by load jobs. Configure a stable value when you need easier load-label tracing. |
| sink.flush.queue-size | Int | No | 1 | Queue length for asynchronous upload to object storage. |
| selectdb.config | Map | No | - | Extra Copy Into data description parameters. Add the `selectdb.config` prefix to the original load parameter name, for example `selectdb.config.file.type = "json"`. |

### CDC and exactly-once notes

SelectDB Cloud Sink can consume insert, update, and delete row kinds. Delete handling only takes effect when `sink.enable-delete = true`, and the target table must meet the SelectDB Cloud delete requirements.

`sink.enable-2pc = true` is the default and is the recommended setting for exactly-once delivery. If a very large write keeps cached files longer than the SelectDB Cloud expiration window, set `sink.enable-2pc = false` and accept at-least-once behavior.

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

The supported formats include CSV and JSON. Configure the format through `selectdb.config`, for example `selectdb.config.file.type = "json"`.

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
  Jdbc {
    driver = com.mysql.cj.jdbc.Driver
    url = "jdbc:mysql://selectdb_e2e:9030"
    username = admin
    password = ""
    query = "select BIGINT_COL, LARGEINT_COL, SMALLINT_COL, TINYINT_COL, BOOLEAN_COL, DECIMAL_COL, DOUBLE_COL, FLOAT_COL, INT_COL, CHAR_COL, VARCHAR_11_COL, STRING_COL, DATETIME_COL, DATE_COL from `test`.`e2e_table_source`"
  }
}

sink {
  SelectDBCloud {
    load-url = "warehouse_ip:http_port"
    jdbc-url = "warehouse_ip:mysql_port"
    cluster-name = "Cluster"
    table.identifier = "test.e2e_table_sink"
    username = "admin"
    password = "******"
    sink.enable-2pc = true
    selectdb.config {
      file.type = "json"
      file.strip_outer_array = "false"
    }
  }
}
```

### Write FakeSource data

```hocon
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
