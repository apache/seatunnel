# Clickhouse

> Clickhouse sink connector

## Description

Used to write data to Clickhouse.

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)

The Clickhouse sink plug-in can achieve accuracy once by implementing idempotent writing, and needs to cooperate with aggregatingmergetree and other engines that support deduplication.

- [ ] [schema projection](../../concept/connector-v2-features.md)

:::tip

Write data to Clickhouse can also be done using JDBC

:::

## Options

| name           | type   | required | default value |
|----------------|--------|----------|---------------|
| host           | string | yes      | -             |
| database       | string | yes      | -             |
| table          | string | yes      | -             |
| username       | string | yes      | -             |
| password       | string | yes      | -             |
| fields         | string | yes      | -             |
| clickhouse.*   | string | no       |               |
| bulk_size      | string | no       | 20000         |
| split_mode     | string | no       | false         |
| sharding_key   | string | no       | -             |
| common-options |        | no       | -             |

### host [string]

`ClickHouse` cluster address, the format is `host:port` , allowing multiple `hosts` to be specified. Such as `"host1:8123,host2:8123"` .

### database [string]

The `ClickHouse` database

### table [string]

The table name

### username [string]

`ClickHouse` user username

### password [string]

`ClickHouse` user password

### fields [array]

The data field that needs to be output to `ClickHouse` , if not configured, it will be automatically adapted according to the sink table `schema` .

### clickhouse [string]

In addition to the above mandatory parameters that must be specified by `clickhouse-jdbc` , users can also specify multiple optional parameters, which cover all the [parameters](https://github.com/ClickHouse/clickhouse-jdbc/tree/master/clickhouse-client#configuration) provided by `clickhouse-jdbc` .

The way to specify the parameter is to add the prefix `clickhouse.` to the original parameter name. For example, the way to specify `socket_timeout` is: `clickhouse.socket_timeout = 50000` . If these non-essential parameters are not specified, they will use the default values given by `clickhouse-jdbc`.

### bulk_size [number]

The number of rows written through [Clickhouse-jdbc](https://github.com/ClickHouse/clickhouse-jdbc) each time, the `default is 20000` .

### split_mode [boolean]

This mode only support clickhouse table which engine is 'Distributed'.And `internal_replication` option
should be `true`. They will split distributed table data in seatunnel and perform write directly on each shard. The shard weight define is clickhouse will be
counted.

### sharding_key [string]

When use split_mode, which node to send data to is a problem, the default is random selection, but the
'sharding_key' parameter can be used to specify the field for the sharding algorithm. This option only
worked when 'split_mode' is true.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details

## Examples

```hocon
sink {

  Clickhouse {
    host = "localhost:8123"
    database = "default"
    table = "fake_all"
    username = "default"
    password = ""
    split_mode = true
    sharding_key = "age"
  }
  
}
```

```hocon
sink {

  Clickhouse {
    host = "localhost:8123"
    database = "default"
    table = "fake_all"
    username = "default"
    password = ""
  }
  
}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add ClickHouse Sink Connector

### 2.3.0-beta 2022-10-20
- [Improve] Clickhouse Support Int128,Int256 Type ([3067](https://github.com/apache/incubator-seatunnel/pull/3067))
