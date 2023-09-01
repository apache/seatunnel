# Postgres CDC

> Postgres CDC source connector

## Description

The Postgres CDC connector allows for reading snapshot data and incremental data from Postgres database. This document
describes how to set up the Postgres CDC connector to run SQL queries against Postgres databases.

## Key features

- [ ] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name                                           | type     | required | default value |
| ---------------------------------------------- | -------- | -------- | ------------- |
| base-url                                       | String   | Yes      | -             |
| username                                       | String   | Yes      | -             |
| password                                       | String   | Yes      | -             |
| database-names                                 | List     | Yes      | -             |
| table-names                                    | List     | Yes      | -             |
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

### database-name [String]

Database name of the database to monitor.

### schmea-list [List]

Schmea name of the database to monitor.

### table-names [List]

Table name of the database to monitor. The table name needs to include the schema name, for example: schema_name.table_name

### startup.mode [Enum]

Optional startup mode for postgres CDC consumer, valid enumerations are "initial", "earliest", "latest" and "specific".

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

Optional stop mode for postgres CDC consumer, valid enumerations are "never".

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

### connect.timeout.ms [long]

The maximum time that the connector should wait after trying to connect to the database server before timing out.

### connect.max-retries [Integer]

The max retry times that the connector should retry to build database server connection.

### connection.pool.size [Integer]

The connection pool size.

### debezium [Config]

Pass-through Debezium's properties to Debezium Embedded Engine which is used to capture data changes from postgres server.

See more about
the [Debezium's postgres Connector properties](https://debezium.io/documentation/reference/1.6/connectors/postgresql.html#postgresql-connector-properties)

### format [Enum]

Optional output format for Postgres CDC, valid enumerations are "DEFAULT"、"COMPATIBLE_DEBEZIUM_JSON".

#### example

```conf
source {
  Postgres-CDC {
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
  Postgres-CDC  {
    base-url = "jdbc:postgresql://127.0.0.1:5432/postgres"
    username = "postgres"
    password = "postgres"
    database-names = ["postgres"]
    table-names = ["postgres.public.cdc_source_table"]
   }
   
}
```

## Changelog

- Add Postgres CDC Source Connector

### next version

