# Doris

> Doris sink connector

## Description

Used to send data to Doris. Both support streaming and batch mode.
The internal implementation of Doris sink connector is cached and imported by stream load in batches.

:::tip

Version Supported

* exactly-once & cdc supported  `Doris version is >= 1.1.x`
* Array data type supported  `Doris version is >= 1.2.x`
* Map data type will be support in `Doris version is 2.x`

:::

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

## Options

|        name        |  type  | required | default value |
|--------------------|--------|----------|---------------|
| fenodes            | string | yes      | -             |
| username           | string | yes      | -             |
| password           | string | yes      | -             |
| table.identifier   | string | yes      | -             |
| sink.label-prefix  | string | yes      | -             |
| sink.enable-2pc    | bool   | no       | true          |
| sink.enable-delete | bool   | no       | false         |
| doris.config       | map    | yes      | -             |

### fenodes [string]

`Doris` cluster fenodes address, the format is `"fe_ip:fe_http_port, ..."`

### username [string]

`Doris` user username

### password [string]

`Doris` user password

### table.identifier [string]

The name of `Doris` table

### sink.label-prefix [string]

The label prefix used by stream load imports. In the 2pc scenario, global uniqueness is required to ensure the EOS semantics of SeaTunnel.

### sink.enable-2pc [bool]

Whether to enable two-phase commit (2pc), the default is true, to ensure Exactly-Once semantics. For two-phase commit, please refer to [here](https://doris.apache.org/docs/dev/sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD).

### sink.enable-delete [bool]

Whether to enable deletion. This option requires Doris table to enable batch delete function (0.15+ version is enabled by default), and only supports Unique model. you can get more detail at this link:

https://doris.apache.org/docs/dev/data-operate/update-delete/batch-delete-manual

### doris.config [map]

The parameter of the stream load `data_desc`, you can get more detail at this link:

https://doris.apache.org/docs/dev/sql-manual/sql-reference/Data-Manipulation-Statements/Load/STREAM-LOAD

#### Supported import data formats

The supported formats include CSV and JSON. Default value: CSV

## Example

Use JSON format to import data

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

Use CSV format to import data

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
