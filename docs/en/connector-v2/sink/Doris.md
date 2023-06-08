# Doris

> Doris sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## Key features

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

| Name                | Type   | Required | Default    | Description                                                                                                                                                                                                                                                                                  |
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

#### Supported import data formats

The supported formats include CSV and JSON

## Task Example

### Simple:

> This example defines a SeaTunnel synchronization task that automatically generates data through FakeSource and sends it to Doris Sink,FakeSource simulates CDC data with schema pk_id (bigint type), name (string type), and score (int type),Doris needs to create a table sink named test.e2e_table_sink and a corresponding table for it.

```hocon
env {
  execution.parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        pk_id = bigint
        name = string
        score = int
      }
    }
    rows = [
      {
        kind = INSERT
        fields = [1, "A", 100]
      },
      {
        kind = INSERT
        fields = [2, "B", 100]
      },
      {
        kind = INSERT
        fields = [3, "C", 100]
      },
      {
        kind = UPDATE_BEFORE
        fields = [1, "A", 100]
      },
      {
        kind = UPDATE_AFTER
        fields = [1, "A_1", 100]
      },
      {
        kind = DELETE
        fields = [2, "B", 100]
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
    sink.enable-2pc = "false"
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

- [Improve] Change Doris Config Prefix [3856](https://github.com/apache/incubator-seatunnel/pull/3856)

- [Improve] Refactor some Doris Sink code as well as support 2pc and cdc [4235](https://github.com/apache/incubator-seatunnel/pull/4235)

:::tip

PR 4235 is an incompatible modification to PR 3856. Please refer to PR 4235 to use the new Doris connector

:::
