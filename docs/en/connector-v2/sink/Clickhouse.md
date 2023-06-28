# Clickhouse

> Clickhouse sink connector

## Description

Used to write data to Clickhouse.

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)

The Clickhouse sink plug-in can achieve accuracy once by implementing idempotent writing, and needs to cooperate with aggregatingmergetree and other engines that support deduplication.

- [x] [cdc](../../concept/connector-v2-features.md)

## Options

|                 name                  |  type   | required | default value |
|---------------------------------------|---------|----------|---------------|
| host                                  | string  | yes      | -             |
| database                              | string  | yes      | -             |
| table                                 | string  | yes      | -             |
| username                              | string  | yes      | -             |
| password                              | string  | yes      | -             |
| clickhouse.config                     | map     | no       |               |
| bulk_size                             | string  | no       | 20000         |
| split_mode                            | string  | no       | false         |
| sharding_key                          | string  | no       | -             |
| primary_key                           | string  | no       | -             |
| support_upsert                        | boolean | no       | false         |
| allow_experimental_lightweight_delete | boolean | no       | false         |
| common-options                        |         | no       | -             |

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

### clickhouse.config [map]

In addition to the above mandatory parameters that must be specified by `clickhouse-jdbc` , users can also specify multiple optional parameters, which cover all the [parameters](https://github.com/ClickHouse/clickhouse-jdbc/tree/master/clickhouse-client#configuration) provided by `clickhouse-jdbc` .

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

### primary_key [string]

Mark the primary key column from clickhouse table, and based on primary key execute INSERT/UPDATE/DELETE to clickhouse table

### support_upsert [boolean]

Support upsert row by query primary key

### allow_experimental_lightweight_delete [boolean]

Allow experimental lightweight delete based on `*MergeTree` table engine

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details

## Examples

Simple

```hocon
sink {
  Clickhouse {
    host = "localhost:8123"
    database = "default"
    table = "fake_all"
    username = "default"
    password = ""
    clickhouse.confg = {
      max_rows_to_read = "100"
      read_overflow_mode = "throw"
    }
  }
}
```

Split mode

```hocon
sink {
  Clickhouse {
    host = "localhost:8123"
    database = "default"
    table = "fake_all"
    username = "default"
    password = ""
    
    # split mode options
    split_mode = true
    sharding_key = "age"
  }
}
```

CDC(Change data capture)

```hocon
sink {
  Clickhouse {
    host = "localhost:8123"
    database = "default"
    table = "fake_all"
    username = "default"
    password = ""
    
    # cdc options
    primary_key = "id"
    support_upsert = true
  }
}
```

CDC(Change data capture) for *MergeTree engine

```hocon
sink {
  Clickhouse {
    host = "localhost:8123"
    database = "default"
    table = "fake_all"
    username = "default"
    password = ""
    
    # cdc options
    primary_key = "id"
    support_upsert = true
    allow_experimental_lightweight_delete = true
  }
}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add ClickHouse Sink Connector

### 2.3.0-beta 2022-10-20

- [Improve] Clickhouse Support Int128,Int256 Type ([3067](https://github.com/apache/seatunnel/pull/3067))

### next version

- [Improve] Clickhouse Sink support nest type and array type([3047](https://github.com/apache/seatunnel/pull/3047))
- [Improve] Clickhouse Sink support geo type([3141](https://github.com/apache/seatunnel/pull/3141))
- [Feature] Support CDC write DELETE/UPDATE/INSERT events ([3653](https://github.com/apache/seatunnel/pull/3653))
- [Improve] Remove Clickhouse Fields Config ([3826](https://github.com/apache/seatunnel/pull/3826))
- [Improve] Change Connector Custom Config Prefix To Map [3719](https://github.com/apache/seatunnel/pull/3719)

