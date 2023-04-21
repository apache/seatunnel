# Postgres CDC

> PostgreSQL CDC source connector

## Description

The PostgreSQL CDC connector allows for reading snapshot data and incremental data from PostgreSQL database. This
document
describes how to setup the PostgreSQL CDC connector to run SQL queries against PostgreSQL databases.

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
| base-url                                       | String   | Yes      | -             |
| decoding.plugin.name                           | String   | No       | decoderbufs   |
| slot.name                                      | String   | No       | seatunnel     |
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

Table name of the database to monitor.

### base-url [String]

URL has to be without database, like "jdbc:postgresql://localhost:5432/" or "jdbc:mariadb://localhost:5432" rather than "
jdbc:polardb://localhost:5432/db"

### decoding.plugin.name [String]

The name of the Postgres logical decoding plug-in installed on the server. Supported values are decoderbufs, wal2json,
wal2json_rds, wal2json_streaming, wal2json_rds_streaming and pgoutput.

### slot.name [String]

The name of the PostgreSQL logical decoding slot that was created for streaming changes from a particular plug-in for a
particular database/schema. The server uses this slot to stream events to the connector that you are configuring.
Default is "seatunnel".

### startup.mode [Enum]

Optional startup mode for Postgres CDC consumer, valid enumerations are "initial", "earliest", "latest" and "specific".

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

Optional stop mode for Postgres CDC consumer, valid enumerations are "never".

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

Pass-through Debezium's properties to Debezium Embedded Engine which is used to capture data changes from PostgresSQL.

See more about
the [Debezium's PostgreSQL Connector properties](https://debezium.io/documentation/reference/1.6/connectors/postgresql.html#postgresql-connector-properties)

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
  Postgres-CDC {
    result_table_name = "fake"
    parallelism = 1
    server-id = 5656
    port = 56725
    hostname = "127.0.0.1"
    username = "postgresuser"
    password = "postgrespw"
    database-name = "inventory_vwyw0n"
    table-name = "products"
    base-url = "jdbc:postgresql://localhost:56725"
  }
}
```

## Changelog

### next version

- Add Postgres CDC Source Connector

