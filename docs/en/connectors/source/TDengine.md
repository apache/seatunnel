import ChangeLog from '../changelog/connector-tdengine.md';

# TDengine

> TDengine source connector

## Support Those Engines

> SeaTunnel Zeta<br/>

## Description

Read data from TDengine super tables.

The source reads data in batch mode by querying a time range from one super
table. You can read all sub tables under the super table, limit the read to
specific sub tables, and select only part of the columns.

Each source split reads one TDengine sub table. The output schema always adds
`subtable_name` as the first field so that downstream sinks can keep the
original TDengine sub table name. This is also the field consumed by the
TDengine sink when writing rows back to TDengine.

:::tip

The connector uses the TDengine REST endpoint (`jdbc:TAOS-RS://...`). Make sure
the TDengine REST service is enabled on the server. Tested against TDengine
3.x.

:::

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Source Options

| Name         | Type   | Required | Default Value | Description                                                                                                                                                                                       |
|--------------|--------|----------|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url          | String | Yes      | -             | The TDengine REST JDBC URL, for example `jdbc:TAOS-RS://localhost:6041/`.                                                                                                                          |
| username     | String | Yes      | -             | The username used to connect to TDengine.                                                                                                                                                         |
| password     | String | Yes      | -             | The password used to connect to TDengine.                                                                                                                                                         |
| database     | String | Yes      | -             | The TDengine database name.                                                                                                                                                                       |
| stable       | String | Yes      | -             | The TDengine super table name to read from.                                                                                                                                                      |
| sub_tables   | List   | No       | -             | A list of sub table names to read. If it is not configured, all sub tables under the configured super table are read. If it is configured, only the listed sub tables are read.                   |
| lower_bound  | String | Yes      | -             | The inclusive lower bound of the query time range. The connector adds `timestamp_column >= lower_bound` to each sub table query. Use a TDengine-compatible timestamp string, e.g. `2018-10-03 14:38:05.000`. |
| upper_bound  | String | Yes      | -             | The exclusive upper bound of the query time range. The connector adds `timestamp_column < upper_bound` to each sub table query. Use a TDengine-compatible timestamp string, e.g. `2018-10-03 14:38:16.801`. |
| read_columns | List   | No       | -             | A list of column names to read. If it is not configured, all columns are read. When reading from a super table, put TAGS columns at the end of the list. Do not include `subtable_name`; the connector adds it automatically as the first output field. |
| common-options |     | No       | -             | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.                                                                  |

:::tip

`lower_bound` and `upper_bound` together define the query window. Each
parallel reader scans its own sub table within that window, so `parallelism`
maps to the number of sub tables being read. Make sure the time range covers
all the rows you want to read.

:::

### url [String]

The TDengine REST JDBC URL.

```
jdbc:TAOS-RS://localhost:6041/
```

### username [String]

The username used to connect to TDengine.

### password [String]

The password used to connect to TDengine.

### database [String]

The TDengine database name.

### stable [String]

The TDengine super table name. Each source split reads one sub table under this
super table.

### sub_tables [List]

A list of sub table names. If it is not configured, all sub tables under the
configured super table are read. If it is configured, only the listed sub tables
are read.

### lower_bound [String]

The inclusive lower bound of the query time range. The connector adds
`timestamp_column >= lower_bound` to each sub table query. Use a
TDengine-compatible timestamp string, for example `2018-10-03 14:38:05.000`.

### upper_bound [String]

The exclusive upper bound of the query time range. The connector adds
`timestamp_column < upper_bound` to each sub table query. Use a
TDengine-compatible timestamp string, for example `2018-10-03 14:38:16.801`.

### read_columns [List]

A list of column names to read. If it is not configured, all columns are read.
When reading from a super table, put TAGS columns at the end of the list. Do not
include `subtable_name`; the connector adds it automatically as the first output
field.

The order of `read_columns` decides the output field order after
`subtable_name`. If the result is written to a TDengine sink, keep normal columns
before TAGS columns so the sink can split column values and TAGS values
correctly.

### common options

Source plugin common parameters, please refer to
[Source Common Options](../common-options/source-common-options.md) for details.

## Task Example

### Read all sub tables in a time range

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  TDengine {
    url = "jdbc:TAOS-RS://localhost:6041/"
    username = "root"
    password = "taosdata"
    database = "power"
    stable = "meters"
    lower_bound = "2018-10-03 14:38:05.000"
    upper_bound = "2018-10-03 14:38:16.801"
    plugin_output = "tdengine_result"
  }
}

sink {
  Console {}
}
```

### Read selected sub tables and columns

```hocon
source {
  TDengine {
    url = "jdbc:TAOS-RS://localhost:6041/"
    username = "root"
    password = "taosdata"
    database = "power"
    stable = "meters"
    lower_bound = "2018-10-03 14:38:05.000"
    upper_bound = "2018-10-03 14:38:16.801"
    sub_tables = ["d1001", "d1002"]
    read_columns = ["ts", "current", "voltage", "phase", "off", "nc", "location", "groupid"]
  }
}
```

### Read from TDengine and write back to TDengine

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  TDengine {
    url = "jdbc:TAOS-RS://tdengine-src:6041/"
    username = "root"
    password = "taosdata"
    database = "power"
    stable = "meters"
    lower_bound = "2018-10-03 14:38:05.000"
    upper_bound = "2018-10-03 14:38:16.801"
    plugin_output = "tdengine_result"
  }
}

sink {
  TDengine {
    url = "jdbc:TAOS-RS://tdengine-sink:6041/"
    username = "root"
    password = "taosdata"
    database = "power2"
    stable = "meters2"
    timezone = "UTC"
  }
}
```

## Changelog

<ChangeLog />