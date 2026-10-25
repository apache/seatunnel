import ChangeLog from '../changelog/connector-jdbc.md';

# Snowflake

> JDBC Snowflake Sink Connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Write data to Snowflake through JDBC. The sink supports batch and streaming jobs, concurrent writes, and CDC events. Each batch flushes when `batch_size` rows are buffered, the time-based `batch_interval_ms` elapses, or a checkpoint is triggered.

## Database Dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATUNNEL_HOME/plugins/jdbc/lib/' working directory<br/>
> For example Snowflake datasource: cp snowflake-connector-java-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md) (via primary-key upsert / merge SQL)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [x] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Supported DataSource Info

| Datasource | Supported versions                                   | Driver                                | Url                                          | Maven                                                          |
|------------|------------------------------------------------------|---------------------------------------|----------------------------------------------|----------------------------------------------------------------|
| Snowflake  | Different dependency version has different driver class. | net.snowflake.client.jdbc.SnowflakeDriver | jdbc:snowflake://<account_name>.snowflakecomputing.com | [Download](https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc) |

## Data Type Mapping

|                             Snowflake Data Type                             | SeaTunnel Data Type |
|-----------------------------------------------------------------------------|---------------------|
| BOOLEAN                                                                     | BOOLEAN             |
| TINYINT<br/>SMALLINT<br/>BYTEINT                                            | SHORT               |
| INT<br/>INTEGER                                                             | INT                 |
| BIGINT                                                                      | LONG                |
| DECIMAL<br/>NUMERIC<br/>NUMBER<br/>                                         | DECIMAL(p, s)       |
| DECIMAL(p, s) (with `p > 38`)                                               | DECIMAL(38, 18)     |
| REAL<br/>FLOAT4                                                             | FLOAT               |
| DOUBLE<br/>DOUBLE PRECISION<br/>FLOAT8<br/>FLOAT                            | DOUBLE              |
| CHAR<br/>CHARACTER<br/>VARCHAR<br/>STRING<br/>TEXT<br/>VARIANT<br/>OBJECT   | STRING              |
| DATE                                                                        | DATE                |
| TIME                                                                        | TIME                |
| DATETIME<br/>TIMESTAMP<br/>TIMESTAMP_LTZ<br/>TIMESTAMP_NTZ<br/>TIMESTAMP_TZ | TIMESTAMP           |
| BINARY<br/>VARBINARY<br/>GEOGRAPHY<br/>GEOMETRY                             | BYTES               |

## Sink Options

|                   Name                    |  Type   | Required | Default | Description                                                                                                                                                                                |
|-------------------------------------------|---------|----------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String  | Yes      | -       | JDBC connection URL, for example `jdbc:snowflake://<account_name>.snowflakecomputing.com`.                                                                                                  |
| driver                                    | String  | Yes      | -       | JDBC driver class name. Use `net.snowflake.client.jdbc.SnowflakeDriver` for Snowflake.                                                                                                     |
| username                                  | String  | No       | -       | Username for the Snowflake account.                                                                                                                                                        |
| password                                  | String  | No       | -       | Password for the Snowflake account.                                                                                                                                                        |
| query                                     | String  | No       | -       | SQL used to write upstream rows. Takes precedence over `database`/`table` auto-generated SQL, and disables catalog-based optimizations (no `MERGE` upsert).                                |
| database                                  | String  | No       | -       | Database name. When `generate_sink_sql = true`, used with `table` to generate `INSERT`/`MERGE` SQL. Mutually exclusive with `query`; when both are set, `query` wins.                       |
| table                                     | String  | No       | -       | Target table name. Used together with `database` and `generate_sink_sql` to generate writes.                                                                                              |
| primary_keys                              | Array   | No       | -       | Primary-key columns. Required when `generate_sink_sql = true` together with `enable_upsert = true` to build a `MERGE` upsert statement.                                                    |
| connection_check_timeout_sec              | Int     | No       | 30      | Seconds to wait for the connection check before failing.                                                                                                                                   |
| max_retries                               | Int     | No       | 0       | Number of retries on `executeBatch` failures.                                                                                                                                              |
| batch_size                                | Int     | No       | 1000    | Buffered-row threshold that triggers a flush. Also flushes at `checkpoint.interval`.                                                                                                       |
| batch_interval_ms                         | Long    | No       | 0       | Maximum time (ms) between two flushes. `0` disables interval-based flushing.                                                                                                              |
| max_commit_attempts                       | Int     | No       | 3       | Number of retries on transaction-commit failures.                                                                                                                                          |
| transaction_timeout_sec                   | Int     | No       | -1      | Transaction timeout in seconds. `-1` means no timeout.                                                                                                                                     |
| auto_commit                               | Boolean | No       | true    | Whether to auto-commit each batch.                                                                                                                                                         |
| properties                                | Map     | No       | -       | Extra JDBC connection properties. When the same key appears in both `properties` and `url`, the precedence is driver-specific.                                                            |
| common-options                            |         | No       | -       | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.                                                              |
| enable_upsert                             | Boolean | No       | true    | When `primary_keys` is set with `generate_sink_sql = true`, generate `MERGE` upsert SQL. Set to `false` if your input has no duplicate keys and you want the faster insert-only path.       |

## Notes

- Use `query` when you want to fully control the `INSERT` statement and parameter order.
- Use `database`, `table`, and `primary_keys` (with `generate_sink_sql = true`) when SeaTunnel should generate sink SQL for insert, update, and delete events.
- Snowflake sink uses normal JDBC batch writes. The connector does not provide exactly-once guarantees for Snowflake.
- Keep Snowflake credentials out of shared examples, logs, and screenshots.

## Task Example

### Simple

This example reads 16 rows from a `FakeSource` and inserts them into `test_table` in Snowflake.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    parallelism = 1
    plugin_output = "fake"
    row.num = 16
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  Jdbc {
    url = "jdbc:snowflake://<account_name>.snowflakecomputing.com"
    driver = "net.snowflake.client.jdbc.SnowflakeDriver"
    username = "USER"
    password = "PASSWORD"
    query = "insert into test_table(name, age) values(?, ?)"
  }
}
```

Before running this job, create the target database and table in your Snowflake account.

### CDC Event

Configure `database`, `table`, and `primary_keys` so SeaTunnel can generate the right `INSERT`/`UPDATE`/`DELETE` SQL for CDC events.

```hocon
sink {
  Jdbc {
    url = "jdbc:snowflake://<account_name>.snowflakecomputing.com"
    driver = "net.snowflake.client.jdbc.SnowflakeDriver"
    username = "USER"
    password = "PASSWORD"
    generate_sink_sql = true
    database = "test"
    table = "sink_table"
    primary_keys = ["id", "name"]
  }
}
```

## Changelog

<ChangeLog />
