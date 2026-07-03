import ChangeLog from '../changelog/connector-tdengine.md';

# TDengine

> TDengine source connector

## Description

Read data from TDengine super tables.

The source reads data in batch mode by querying a time range from one super
table. You can read all sub tables under the super table, limit the read to
specific sub tables, and select only part of the columns.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)

- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Options

| name         | type   | required | default value |
|--------------|--------|----------|---------------|
| url          | string | yes      | -             |
| username     | string | yes      | -             |
| password     | string | yes      | -             |
| database     | string | yes      | -             |
| stable       | string | yes      | -             |
| sub_tables   | list   | no       | -             |
| lower_bound  | string | yes      | -             |
| upper_bound  | string | yes      | -             |
| read_columns | list   | no       | -             |

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

The TDengine super table name.

### sub_tables [list]
A list of sub table names. If it is not configured, all sub tables under the
configured super table are read. If it is configured, only the listed sub tables
are read.

### lower_bound [string]

The inclusive lower bound of the query time range. Use a TDengine-compatible
timestamp string, for example `2018-10-03 14:38:05.000`.

### upper_bound [string]

The upper bound of the query time range. Use a TDengine-compatible timestamp
string, for example `2018-10-03 14:38:16.801`.

### read_columns [list]
A list of column names to read. If it is not configured, all columns are read.
When reading from a super table, put TAGS columns at the end of the list.

## Examples

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

## Changelog

<ChangeLog />
