# Oracle CDC

> Oracle CDC source connector

## Description

The Oracle CDC connector allows for reading snapshot data and incremental data from Oracle database. This document
describes how to setup the Oracle CDC connector to run SQL queries against Oracle databases.

## Key features

- [ ] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

## Options

|                      name                      |   type   | required | default value |
|------------------------------------------------|----------|----------|---------------|
| hostname                                       | String   | Yes      | -             |
| port                                           | Integer  | No       | 3306          |
| username                                       | String   | Yes      | -             |
| password                                       | String   | Yes      | -             |
| database-name                                  | String   | Yes      | -             |
| table-name                                     | String   | Yes      | -             |
| startup.mode                                   | Enum     | No       | INITIAL       |
| startup.timestamp                              | Long     | No       | -             |
| startup.specific-offset.file                   | String   | No       | -             |
| startup.specific-offset.pos                    | Long     | No       | -             |
| stop.mode                                      | Enum     | No       | NEVER         |
| stop.timestamp                                 | Long     | No       | -             |
| stop.specific-offset.file                      | String   | No       | -             |
| stop.specific-offset.pos                       | Long     | No       | -             |
| incremental.parallelism                        | Integer  | No       | 1             |
| snapshot.split.size                            | Integer  | No       | 8096          |
| snapshot.fetch.size                            | Integer  | No       | 1024          |
| server-time-zone                               | String   | No       | UTC           |
| connect.timeout                                | Duration | No       | 30s           |
| connect.max-retries                            | Integer  | No       | 3             |
| connection.pool.size                           | Integer  | No       | 20            |
| chunk-key.even-distribution.factor.upper-bound | Double   | No       | 1000          |
| chunk-key.even-distribution.factor.lower-bound | Double   | No       | 0.05          |
| debezium.*                                     | config   | No       | -             |
| common-options                                 |          | no       | -             |

### hostname [String]

IP address or hostname of the database server.

### port [Integer]

Integer port number of the database server.

### username [String]

Name of the database to use when connecting to the database server.

### password [String]

Password to use when connecting to the database server.

### database-name [String]

Database name of the database to monitor.

### table-name [String]

Table name is a combination of schema name and table name (schemaName.tableName).

### startup.mode [Enum]

Optional startup mode for Oracle CDC consumer, valid enumerations are "initial", "earliest", "latest" and "specific".

### startup.timestamp [Long]

Start from the specified epoch timestamp (in milliseconds).

**Note, This option is required when the "startup.mode" option used `'timestamp'`.**

### startup.specific-offset.file [String]

Start from the specified binlog file name.

**Note, This option is required when the "startup.mode" option used `'specific'`.**

### startup.specific-offset.pos [Long]

Start from the specified binlog file position.

**Note, This option is required when the "startup.mode" option used `'specific'`.**

### stop.mode [Enum]

Optional stop mode for Oracle CDC consumer, valid enumerations are "never".

### stop.timestamp [Long]

Stop from the specified epoch timestamp (in milliseconds).

**Note, This option is required when the "stop.mode" option used `'timestamp'`.**

### stop.specific-offset.file [String]

Stop from the specified binlog file name.

**Note, This option is required when the "stop.mode" option used `'specific'`.**

### stop.specific-offset.pos [Long]

Stop from the specified binlog file position.

**Note, This option is required when the "stop.mode" option used `'specific'`.**

### incremental.parallelism [Integer]

The number of parallel readers in the incremental phase.

### snapshot.split.size [Integer]

The split size (number of rows) of table snapshot, captured tables are split into multiple splits when read the snapshot
of table.

### snapshot.fetch.size [Integer]

The maximum fetch size for per poll when read table snapshot.

### server-time-zone [String]

The session time zone in database server.

### connect.timeout [Duration]

The maximum time that the connector should wait after trying to connect to the database server before timing out.

### connect.max-retries [Integer]

The max retry times that the connector should retry to build database server connection.

### connection.pool.size [Integer]

The connection pool size.

### debezium [Config]

Pass-through Debezium's properties to Debezium Embedded Engine which is used to capture data changes from  server.

See more about
the [Debezium's Oracle Connector properties](https://debezium.io/documentation/reference/1.6/connectors/oracle.html#oracle-connector-properties)

#### example

```conf
source {
  Oracle-CDC {
    debezium {
        snapshot.mode = "never"
        decimal.handling.mode = "double"
    }
  }
}
```

### common options

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details.

## Example

```Jdbc {
source {
  Oracle-CDC {
    result_table_name = "customers"
    hostname = "127.0.0.1"
    port = "1521"
    username = "cdcuser"
    password = "cdcpw"
    database-name = "XE"
    table-name = "cdcuser.orders"
  }
}
```

## Changelog

### next version

- Add Oracle CDC Source Connector

