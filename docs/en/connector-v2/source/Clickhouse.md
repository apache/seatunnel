# Clickhouse

> Clickhouse source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

> supports query SQL and can achieve projection effect.

## Description

Used to read data from Clickhouse.

## Supported DataSource Info

In order to use the Clickhouse connector, the following dependencies are required.
They can be downloaded via install-plugin.sh or from the Maven central repository.

| Datasource | Supported Versions |                                                    Dependency                                                    |
|------------|--------------------|------------------------------------------------------------------------------------------------------------------|
| Clickhouse | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/seatunnel-connectors-v2/connector-clickhouse) |

## Data Type Mapping

|                  Clickhouse Data type                  | SeaTunnel Data type |
|--------------------------------------------------------|---------------------|
| String / IP / UUID /Enum                               | STRING              |
| UInt8                                                  | BOOLEAN             |
| FixedString                                            | BINARY              |
| Int32 / UInt16 / Interval                              | INTEGER             |
| Int8                                                   | TINYINT             |
| Int64                                                  | BIGINT              |
| Int16 / UInt8                                          | SMALLINT            |
| Float64                                                | DOUBLE              |
| Decimal / Int128 / Int256 / UInt64 / UInt128 / UInt256 | DECIMAL             |
| Float32                                                | FLOAT               |
| Date                                                   | Date                |
| Timestamp                                              | Timestamp           |
| DateTime                                               | Time                |
| Array                                                  | ARRAY               |

## Source Options

|       Name       |  Type  | Required |        Default         |                                                               Description                                                                |
|------------------|--------|----------|------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| host             | String | Yes      | -                      | `ClickHouse` cluster address, the format is `host:port` , allowing multiple `hosts` to be specified. Such as `"host1:8123,host2:8123"` . |
| database         | String | Yes      | -                      | The `ClickHouse` database                                                                                                                |
| sql              | String | Yes      | -                      | The query sql used to search data though Clickhouse server                                                                               |
| username         | String | Yes      | -                      | `ClickHouse` user username                                                                                                               |
| password         | String | Yes      | -                      | `ClickHouse` user password                                                                                                               |
| server_time_zone | String | No       | ZoneId.systemDefault() | The session time zone in database server. If not set, then ZoneId.systemDefault() is used to determine the server time zone.             |
| common-options   |        | No       | -                      | Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details                                  |

## Examples

```hocon
source {
  
  Clickhouse {
    host = "localhost:8123"
    database = "default"
    sql = "select * from test where age = 20 limit 100"
    username = "default"
    password = ""
    server_time_zone = "UTC"
    result_table_name = "test"
  }
}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add ClickHouse Source Connector

### 2.3.0-beta 2022-10-20

- [Improve] Clickhouse Source random use host when config multi-host ([3108](https://github.com/apache/seatunnel/pull/3108))

### next version

- [Improve] Clickhouse Source support nest type and array type([3047](https://github.com/apache/seatunnel/pull/3047))

- [Improve] Clickhouse Source support geo type([3141](https://github.com/apache/seatunnel/pull/3141))

