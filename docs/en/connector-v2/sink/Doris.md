# Doris

> Doris sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

## Description

Used to send data to Doris. Both support streaming and batch mode.
The internal implementation of Doris sink connector is cached and imported by stream load in batches.

## Supported DataSource Info

:::tip

Version Supported

* exactly-once & cdc supported  `Doris version is >= 1.1.x`
* Array data type supported  `Doris version is >= 1.2.x`
* Map data type will be support in `Doris version is 2.x`

:::

## Sink Options

|        Name         |  Type  | Required |  Default   |                                                                                                                                         Description                                                                                                                                          |
|---------------------|--------|----------|------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| fenodes             | String | Yes      | -          | `Doris` cluster fenodes address, the format is `"fe_ip:fe_http_port, ..."`                                                                                                                                                                                                                   |
| username            | String | Yes      | -          | `Doris` user username                                                                                                                                                                                                                                                                        |
| password            | String | Yes      | -          | `Doris` user password                                                                                                                                                                                                                                                                        |
| table.identifier    | String | Yes      | -          | The name of `Doris` table                                                                                                                                                                                                                                                                    |
| sink.label-prefix   | String | Yes      | -          | The label prefix used by stream load imports. In the 2pc scenario, global uniqueness is required to ensure the EOS semantics of SeaTunnel.                                                                                                                                                   |
| sink.enable-2pc     | bool   | No       | -          | Whether to enable two-phase commit (2pc), the default is true, to ensure Exactly-Once semantics. For two-phase commit, please refer to [here](https://doris.apache.org/docs/dev/sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD).                                     |
| sink.enable-delete  | bool   | No       | -          | Whether to enable deletion. This option requires Doris table to enable batch delete function (0.15+ version is enabled by default), and only supports Unique model. you can get more detail at this [link](https://doris.apache.org/docs/dev/data-operate/update-delete/batch-delete-manual) |
| sink.check-interval | int    | No       | 10000      | check exception with the interval while loading                                                                                                                                                                                                                                              |
| sink.max-retries    | int    | No       | 3          | the max retry times if writing records to database failed                                                                                                                                                                                                                                    |
| sink.buffer-size    | int    | No       | 256 * 1024 | the buffer size to cache data for stream load.                                                                                                                                                                                                                                               |
| sink.buffer-count   | int    | No       | 3          | the buffer count to cache data for stream load.                                                                                                                                                                                                                                              |
| doris.config        | map    | yes      | -          | This option is used to support operations such as `insert`, `delete`, and `update` when automatically generate sql,and supported formats.                                                                                                                                                    |

## Data Type Mapping

| Doris Data type |           SeaTunnel Data type           |
|-----------------|-----------------------------------------|
| BOOLEAN         | BOOLEAN                                 |
| TINYINT         | TINYINT                                 |
| SMALLINT        | SMALLINT<br/>TINYINT                    |
| INT             | INT<br/>SMALLINT<br/>TINYINT            |
| BIGINT          | BIGINT<br/>INT<br/>SMALLINT<br/>TINYINT |
| LARGEINT        | BIGINT<br/>INT<br/>SMALLINT<br/>TINYINT |
| FLOAT           | FLOAT                                   |
| DOUBLE          | DOUBLE<br/>FLOAT                        |
| DECIMAL         | DECIMAL<br/>DOUBLE<br/>FLOAT            |
| DATE            | DATE                                    |
| DATETIME        | TIMESTAMP                               |
| CHAR            | STRING                                  |
| VARCHAR         | STRING                                  |
| STRING          | STRING                                  |
| ARRAY           | ARRAY                                   |
| MAP             | MAP                                     |
| JSON            | STRING                                  |
| HLL             | Not supported yet                       |
| BITMAP          | Not supported yet                       |
| QUANTILE_STATE  | Not supported yet                       |
| STRUCT          | Not supported yet                       |

#### Supported import data formats

The supported formats include CSV and JSON

## Task Example

### Simple:

> The following example describes writing multiple data types to Doris, and users need to create corresponding tables downstream

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
  Doris {
    fenodes = "doris_cdc_e2e:8030"
    username = root
    password = ""
    table.identifier = "test.e2e_table_sink"
    sink.label-prefix = "test-cdc"
    sink.enable-2pc = "true"
    sink.enable-delete = "true"
    doris.config {
      format = "json"
      read_json_by_line = "true"
    }
  }
}
```

### CDC(Change Data Capture) Event:

> This example defines a SeaTunnel synchronization task that automatically generates data through FakeSource and sends it to Doris Sink,FakeSource simulates CDC data with schema, score (int type),Doris needs to create a table sink named test.e2e_table_sink and a corresponding table for it.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
  checkpoint.interval = 10000
}

source {
  FakeSource {
    schema = {
      fields {
        pk_id = bigint
        name = string
        score = int
        sex = boolean
        number = tinyint
        height = float
        sight = double
        create_time = date
        update_time = timestamp
      }
    }
    rows = [
      {
        kind = INSERT
        fields = [1, "A", 100, true, 1, 170.0, 4.3, "2020-02-02", "2020-02-02T02:02:02"]
      },
      {
        kind = INSERT
        fields = [2, "B", 100, true, 1, 170.0, 4.3, "2020-02-02", "2020-02-02T02:02:02"]
      },
      {
        kind = INSERT
        fields = [3, "C", 100, true, 1, 170.0, 4.3, "2020-02-02", "2020-02-02T02:02:02"]
      },
      {
        kind = UPDATE_BEFORE
        fields = [1, "A", 100, true, 1, 170.0, 4.3, "2020-02-02", "2020-02-02T02:02:02"]
      },
      {
        kind = UPDATE_AFTER
        fields = [1, "A_1", 100, true, 1, 170.0, 4.3, "2020-02-02", "2020-02-02T02:02:02"]
      },
      {
        kind = DELETE
        fields = [2, "B", 100, true, 1, 170.0, 4.3, "2020-02-02", "2020-02-02T02:02:02"]
      }
    ]
  }
}

sink {
  Doris {
    fenodes = "doris_cdc_e2e:8030"
    username = root
    password = ""
    table.identifier = "test.e2e_table_sink"
    sink.label-prefix = "test-cdc"
    sink.enable-2pc = "true"
    sink.enable-delete = "true"
    doris.config {
      format = "json"
      read_json_by_line = "true"
    }
  }
}

```

### Use JSON format to import data

```
sink {
    Doris {
        fenodes = "e2e_dorisdb:8030"
        username = root
        password = ""
        table.identifier = "test.e2e_table_sink"
        sink.enable-2pc = "true"
        sink.label-prefix = "test_json"
        doris.config = {
            format="json"
            read_json_by_line="true"
        }
    }
}

```

### Use CSV format to import data

```
sink {
    Doris {
        fenodes = "e2e_dorisdb:8030"
        username = root
        password = ""
        table.identifier = "test.e2e_table_sink"
        sink.enable-2pc = "true"
        sink.label-prefix = "test_csv"
        doris.config = {
          format = "csv"
          column_separator = ","
        }
    }
}
```

## Changelog

### 2.3.0-beta 2022-10-20

- Add Doris Sink Connector

### Next version

- [Improve] Change Doris Config Prefix [3856](https://github.com/apache/seatunnel/pull/3856)

- [Improve] Refactor some Doris Sink code as well as support 2pc and cdc [4235](https://github.com/apache/seatunnel/pull/4235)

:::tip

PR 4235 is an incompatible modification to PR 3856. Please refer to PR 4235 to use the new Doris connector

:::
