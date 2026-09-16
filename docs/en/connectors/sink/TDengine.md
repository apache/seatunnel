import ChangeLog from '../changelog/connector-tdengine.md';

# TDengine

> TDengine sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Write data to TDengine.

Create the target database and super table before running the SeaTunnel job. The
sink can write one input table to one super table, or use placeholders such as
`${table_name}` in `stable` for multi-table writes.

The input row must follow TDengine's super table write shape: the first field is
the target sub table name, the middle fields are normal columns, and the last
fields are TAGS values. The connector reads the number of TAGS fields from the
target super table metadata.

For example, if the target super table has two TAGS fields, the last two input
fields are treated as TAGS values, and the first input field is treated as the
sub table name.

## Key features

- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Options

| Name          | Type   | Required | Default | Description |
|---------------|--------|----------|---------|-------------|
| url           | String | Yes      | -       | TDengine REST JDBC URL. For example `jdbc:TAOS-RS://localhost:6041/`. |
| username      | String | Yes      | -       | Username used to connect to TDengine. |
| password      | String | Yes      | -       | Password used to connect to TDengine. |
| database      | String | Yes      | -       | TDengine database name. |
| stable        | String | Yes      | -       | TDengine super table name. For multi-table writes, this value can contain placeholders such as `${table_name}`. |
| timezone      | String | No       | UTC     | TDengine server timezone used for timestamp conversion. |
| write_columns | List   | No       | -       | Normal TDengine column names to insert. When unset, the target super table column order is used. Do not include TAGS columns or the sub-table-name field. |
| common-options |        | No       | -       | Sink plugin common parameters. See [Sink Common Options](../common-options/sink-common-options.md). |

### url [String]

The TDengine REST JDBC URL.

For example:

```
jdbc:TAOS-RS://localhost:6041/
```

### username [String]

The username used to connect to TDengine.

### password [String]

The password used to connect to TDengine.

### database [String]

The TDengine database name. The database must already exist on the server.

### stable [String]

The TDengine super table name. The value is used verbatim by the sink writer;
the TDengine connector itself does not perform placeholder substitution, so
`${table_name}` and similar tokens are not rewritten at runtime. For multi-table
writes, SeaTunnel's upstream framework (`TablePlaceholderProcessor`) may
substitute `stable` once during job setup based on the upstream `CatalogTable`
identifier, but this depends on the upstream framework wiring and is not a
TDengine-specific feature.

### timezone [String]

The TDengine server timezone used for timestamp conversion. The default value is
`UTC`. Set this to match your TDengine server if it is not running in UTC.

### write_columns [List]

The normal TDengine column names to insert. If it is not set, TDengine uses the
column order of the target super table. Do not include the first input field
that contains the sub table name, and do not include TAGS columns; the connector
adds TAGS values from the end of the input row automatically.

### common options

Sink plugin common parameters, please refer to
[Sink Common Options](../common-options/sink-common-options.md) for details.
For multi-table writes, `multi_table_sink_replica` can be used with the common
sink options.

## Input Row Shape

The connector expects every input row to follow the super-table write shape:

1. The first field is the target sub table name (string). The sink creates the
   sub table on demand if it does not exist.
2. The next fields are the normal columns declared in `write_columns` (or in the
   target super table column order when `write_columns` is unset).
3. The last fields are TAGS values. The number of TAGS fields is read from the
   target super table metadata.

If the target super table has two TAGS fields, the last two input fields are
TAGS values and the first input field is the sub table name.

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

Here `${table_name}` is treated as a literal string by the TDengine sink writer
(the connector does not substitute it per row), so this example only works if
the upstream framework substitutes `stable` once at job setup with the
`CatalogTable` identifier from the upstream source. The target super tables
must already exist with matching TAGS columns.

## Changelog

<ChangeLog />
