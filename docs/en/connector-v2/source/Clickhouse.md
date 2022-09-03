# Clickhouse

> Clickhouse source connector

## Description

Used to read data from Clickhouse.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)

supports query SQL and can achieve projection effect.

- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

:::tip

Reading data from Clickhouse can also be done using JDBC

:::

## Options

| name           | type   | required | default value |
|----------------|--------|----------|---------------|
| host           | string | yes      | -             |
| database       | string | yes      | -             |
| sql            | string | yes      | -             |
| username       | string | yes      | -             |
| password       | string | yes      | -             |
| common-options | string | yes      | -             |

### host [string]

`ClickHouse` cluster address, the format is `host:port` , allowing multiple `hosts` to be specified. Such as `"host1:8123,host2:8123"` .

### database [string]

The `ClickHouse` database

### sql [string]

The query sql used to search data though Clickhouse server

### username [string]

`ClickHouse` user username

### password [string]

`ClickHouse` user password

### common options [string]

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details

## Examples

```hocon
source {
  
  Clickhouse {
    host = "localhost:8123"
    database = "default"
    sql = "select * from test where age = 20 limit 100"
    username = "default"
    password = ""
    result_table_name = "test"
  }
  
}
```