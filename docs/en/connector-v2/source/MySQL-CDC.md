# MySQL CDC

> MySQL CDC source connector

## Description

The MySQL CDC connector allows for reading snapshot data and incremental data from MySQL database. This document
describes how to set up the MySQL CDC connector to run SQL queries against MySQL databases.

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
| username                                       | String   | Yes      | -             |
| password                                       | String   | Yes      | -             |
| database-names                                 | List     | No       | -             |
| table-names                                    | List     | Yes      | -             |
| base-url                                       | String   | Yes      | -             |
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
| server-id                                      | String   | No       | -             |
| server-time-zone                               | String   | No       | UTC           |
| connect.timeout.ms                             | Duration | No       | 30000         |
| connect.max-retries                            | Integer  | No       | 3             |
| connection.pool.size                           | Integer  | No       | 20            |
| chunk-key.even-distribution.factor.upper-bound | Double   | No       | 1000          |
| chunk-key.even-distribution.factor.lower-bound | Double   | No       | 0.05          |
| debezium.*                                     | config   | No       | -             |
| format                                         | Enum     | No       | DEFAULT       |
| common-options                                 |          | no       | -             |

### username [String]

Name of the database to use when connecting to the database server.

### password [String]

Password to use when connecting to the database server.

### database-names [List]

Database name of the database to monitor.

### table-names [List]

Table name of the database to monitor. The table name needs to include the database name, for example: database_name.table_name

### base-url [String]

URL has to be with database, like "jdbc:mysql://localhost:5432/db" or "jdbc:mysql://localhost:5432/db?useSSL=true".

### startup.mode [Enum]

Optional startup mode for MySQL CDC consumer, valid enumerations are "initial", "earliest", "latest" and "specific".

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

Optional stop mode for MySQL CDC consumer, valid enumerations are "never".

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

### server-id [String]

A numeric ID or a numeric ID range of this database client, The numeric ID syntax is like '5400', the numeric ID range
syntax is like '5400-5408'.

Every ID must be unique across all currently-running database processes in the MySQL cluster. This connector joins the
MySQL cluster as another server (with this unique ID) so it can read the binlog.

By default, a random number is generated between 5400 and 6400, though we recommend setting an explicit value.

### server-time-zone [String]

The session time zone in database server.

### connect.timeout.ms [long]

The maximum time that the connector should wait after trying to connect to the database server before timing out.

### connect.max-retries [Integer]

The max retry times that the connector should retry to build database server connection.

### connection.pool.size [Integer]

The connection pool size.

### debezium [Config]

Pass-through Debezium's properties to Debezium Embedded Engine which is used to capture data changes from MySQL server.

See more about
the [Debezium's MySQL Connector properties](https://debezium.io/documentation/reference/1.6/connectors/mysql.html#mysql-connector-properties)

### format [Enum]

Optional output format for MySQL CDC, valid enumerations are "DEFAULT"、"COMPATIBLE_DEBEZIUM_JSON".

#### example

```conf
source {
  MySQL-CDC {
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
  MySQL-CDC {
    result_table_name = "fake"
    parallelism = 1
    server-id = 5656
    username = "mysqluser"
    password = "mysqlpw"
    table-names = ["inventory_vwyw0n.products"]
    base-url = "jdbc:mysql://localhost:56725/inventory_vwyw0n"
  }
}
```

## Changelog

- Add MySQL CDC Source Connector

### next version

