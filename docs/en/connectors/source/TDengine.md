import ChangeLog from '../changelog/connector-tdengine.md';

# TDengine

> TDengine source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
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

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Options

| Name         | Type   | Required | Default | Description |
|--------------|--------|----------|---------|-------------|
| url          | String | Yes      | -       | TDengine REST JDBC URL. For example `jdbc:TAOS-RS://localhost:6041/`. |
| username     | String | Yes      | -       | Username used to connect to TDengine. |
| password     | String | Yes      | -       | Password used to connect to TDengine. |
| database     | String | Yes      | -       | TDengine database name. |
| stable       | String | Yes      | -       | TDengine super table name. |
| sub_tables   | List   | No       | -       | Sub table names to read. When unset, all sub tables under `stable` are read. |
| lower_bound  | String | Yes      | -       | Inclusive lower bound of the query time range. Use a TDengine-compatible timestamp such as `2018-10-03 14:38:05.000`. |
| upper_bound  | String | Yes      | -       | Exclusive upper bound of the query time range. Use a TDengine-compatible timestamp such as `2018-10-03 14:38:16.801`. |
| read_columns | List   | No       | -       | Columns to read. When unset, all columns are read. Put TAGS columns at the end of the list; do not include `subtable_name`. |
| common-options |        | No       | -       | Source plugin common parameters. See [Source Common Options](../common-options/source-common-options.md). |

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

The TDengine super table name. The connector splits the read into one source
split per sub table under this super table.

### sub_tables [List]

A list of sub table names. If it is not configured, all sub tables under the
configured super table are read. If it is configured, only the listed sub tables
are read. Sub table names must match the server exactly.

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

## Output Schema

The output table always starts with the reserved `subtable_name` column, which
carries the TDengine sub table name each row came from. Subsequent columns are
the columns declared in `read_columns` (or all columns when `read_columns` is
unset), in the order specified by `read_columns`. TAGS columns follow the
normal columns in the same order you list them.

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

This reads only sub tables `d1001` and `d1002` under super table `meters`. The
TAGS columns (`location`, `groupid`) are placed at the end of `read_columns` so
that a downstream TDengine sink can correctly split normal columns and TAGS
values.

### Read from TDengine and write to TDengine

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

The sink relies on `subtable_name` from the source as the target sub table name
when writing back. The number of TAGS values is read from the target super
table metadata, so the target super table must already exist.

## Changelog

<ChangeLog />
