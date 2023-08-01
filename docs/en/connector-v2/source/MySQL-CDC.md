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
| chunk-key.even-distribution.factor.upper-bound | Double   | No       | 100           |
| chunk-key.even-distribution.factor.lower-bound | Double   | No       | 0.05          |
| sample-sharding.threshold                      | int      | No       | 1000          |
| inverse-sampling.rate                          | int      | No       | 1000          |
| exactly_once                                   | Boolean  | No       | true          |
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

### chunk-key.even-distribution.factor.upper-bound [Double]

The upper bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be less than or equal to this upper bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is greater, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 100.0.

### chunk-key.even-distribution.factor.lower-bound [Double]

The lower bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be greater than or equal to this lower bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is less, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 0.05.

### sample-sharding.threshold [Integer]

This configuration specifies the threshold of estimated shard count to trigger the sample sharding strategy. When the distribution factor is outside the bounds specified by `chunk-key.even-distribution.factor.upper-bound` and `chunk-key.even-distribution.factor.lower-bound`, and the estimated shard count (calculated as approximate row count / chunk size) exceeds this threshold, the sample sharding strategy will be used. This can help to handle large datasets more efficiently. The default value is 1000 shards.

### inverse-sampling.rate [Integer]

The inverse of the sampling rate used in the sample sharding strategy. For example, if this value is set to 1000, it means a 1/1000 sampling rate is applied during the sampling process. This option provides flexibility in controlling the granularity of the sampling, thus affecting the final number of shards. It's especially useful when dealing with very large datasets where a lower sampling rate is preferred. The default value is 1000.

### server-id [String]

A numeric ID or a numeric ID range of this database client, The numeric ID syntax is like '5400', the numeric ID range
syntax is like '5400-5408'.

Every ID must be unique across all currently-running database processes in the MySQL cluster. This connector joins the
MySQL cluster as another server (with this unique ID) so it can read the binlog.

By default, a random number is generated between 5400 and 6400, though we recommend setting an explicit value.

### server-time-zone [String]

The session time zone in database server. If not set, then ZoneId.systemDefault() is used to determine the server time zone.

### connect.timeout.ms [long]

The maximum time that the connector should wait after trying to connect to the database server before timing out.

### connect.max-retries [Integer]

The max retry times that the connector should retry to build database server connection.

### connection.pool.size [Integer]

The connection pool size.

### exactly_once [Boolean]

Enable exactly once semantic.

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

