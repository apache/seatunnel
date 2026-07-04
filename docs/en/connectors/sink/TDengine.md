import ChangeLog from '../changelog/connector-tdengine.md';

# TDengine

> TDengine sink connector

## Description

Write data to TDengine.

Create the target database and super table before running the SeaTunnel job. The
sink can write one input table to one super table, or use placeholders such as
`${table_name}` in `stable` for multi-table writes.

The input row must follow TDengine's super table write shape: the first field is
the target sub table name, the middle fields are normal columns, and the last
fields are TAGS values. The connector reads the number of TAGS fields from the
target super table metadata.

## Key features

- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)

## Options

| name         | type   | required | default value |
|--------------|--------|----------|---------------|
| url          | string | yes      | -             |
| username     | string | yes      | -             |
| password     | string | yes      | -             |
| database     | string | yes      | -             |
| stable       | string | yes      | -             |
| timezone     | string | no       | UTC           |
| write_columns | list   | no       | -             |
| common-options |       | no       | -             |

### url [string]

The TDengine REST JDBC URL.

e.g.

```
jdbc:TAOS-RS://localhost:6041/
```

### username [string]

The username used to connect to TDengine.

### password [string]

The password used to connect to TDengine.

### database [string]

The TDengine database name.

### stable [string]

The TDengine super table name. For multi-table writes, this value can contain
placeholders, for example `${table_name}`.

### timezone [string]

The TDengine server timezone used for timestamp conversion. The default value is
`UTC`.

### write_columns [list]

The normal TDengine column names to insert. If it is not set, TDengine uses the
column order of the target super table. Do not include the first input field
that contains the sub table name, and do not include TAGS columns; the connector
adds TAGS values from the end of the input row automatically.

### common options

Sink plugin common parameters, please refer to
[Sink Common Options](../common-options/sink-common-options.md) for details.
For multi-table writes, `multi_table_sink_replica` can be used with the common
sink options.

## Examples

### Write to one super table

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

sink {
  TDengine {
    url = "jdbc:TAOS-RS://localhost:6041/"
    username = "root"
    password = "taosdata"
    database = "power2"
    stable = "meters2"
    timezone = "UTC"
    write_columns = ["ts", "voltage", "current", "power"]
  }
}
```

### Write multiple input tables to matching super tables

```hocon
source {
  FakeSource {
    plugin_output = "fake"
    tables_configs = [
      {
        schema = {
          table = "meters3"
          fields {
            device_id = "string"
            event_time = "timestamp"
            metric1 = "float"
            metric2 = "int"
            metric3 = "float"
            status_flag = "boolean"
            notes = "string"
            location_tag = "string"
            group_tag = "int"
          }
        }
        rows = [
          {
            kind = INSERT
            fields = ["d2001", "2023-04-22T14:38:05", 10.3, 219, 0.31, true, "nc", "California.SanFrancisco", 2]
          }
        ]
      },
      {
        schema = {
          table = "meters4"
          fields {
            device_id = "string"
            event_time = "timestamp"
            metric1 = "float"
            metric2 = "int"
            metric3 = "float"
            status_flag = "boolean"
            notes = "string"
            location_tag = "string"
            group_tag = "int"
          }
        }
        rows = [
          {
            kind = INSERT
            fields = ["d1005", "2023-04-22T14:38:05", 110.3, 219, 0.31, true, "nc", "California.SanFrancisco", 2]
          }
        ]
      }
    ]
  }
}

sink {
  TDengine {
    url = "jdbc:TAOS-RS://localhost:6041/"
    username = "root"
    password = "taosdata"
    database = "power2"
    stable = "${table_name}"
    timezone = "UTC"
  }
}
```

## Changelog

<ChangeLog />
