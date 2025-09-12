import ChangeLog from '../changelog/connector-tdengine.md';

# TDengine

> TDengine source connector

## Description

Read external data source data through TDengine.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)

supports query SQL and can achieve projection effect.

- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name         | type   | required | default value |
|--------------|--------|----------|---------------|
| url          | string | yes      | -             |
| username     | string | yes      | -             |
| password     | string | yes      | -             |
| database     | string | yes      |               |
| stable       | string | yes      | -             |
| sub_tables   | list   | no       | -             |
| lower_bound  | long   | yes      | -             |
| upper_bound  | long   | yes      | -             |
| read_columns | list   | no       | -             |

### url [string]

the url of the TDengine when you select the TDengine

e.g.

```
jdbc:TAOS-RS://localhost:6041/
```

### username [string]

the username of the TDengine when you select

### password [string]

the password of the TDengine when you select

### database [string]

the database of the TDengine when you select

### stable [string]

the stable of the TDengine when you select

### sub_tables [list]
A list of sub_table names. If not specified, all sub-tables will be selected. If specified, only the specified sub-tables will be selected.

### lower_bound [long]

the lower_bound of the migration period

### upper_bound [long]

the upper_bound of the migration period

### read_columns [list]
A list of column names to read. If not specified, all columns will be selected. 
When reading from a super table, please make sure to put the TAGS columns at the end of the list.

## Example

### source

```hocon
source {
        TDengine {
          url : "jdbc:TAOS-RS://localhost:6041/"
          username : "root"
          password : "taosdata"
          database : "power"
          stable : "meters"
          sub_tables : ["meter_1","meter_2"]
          lower_bound : "2018-10-03 14:38:05.000"
          upper_bound : "2018-10-03 14:38:16.800"
          plugin_output : "tdengine_result"
          read_columns : ["ts","voltage","current","power"]
        }
}
```

## Changelog

<ChangeLog />