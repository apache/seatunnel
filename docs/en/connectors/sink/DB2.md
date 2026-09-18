import ChangeLog from '../changelog/connector-jdbc.md';

# DB2

> JDBC DB2 Sink Connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Write data to DB2 through JDBC. Supports batch and streaming jobs, concurrent writes, and exactly-once semantics backed by XA transactions. Enable exactly-once with `is_exactly_once = true` together with the matching `xa_data_source_class_name`.

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md) (XA transactions)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md) (via primary-key upsert / merge SQL)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [x] [timer flush](../../introduction/concepts/connector-v2-features.md)

> Use XA transactions to ensure exactly-once. You need to enable `is_exactly_once=true` and set the matching `xa_data_source_class_name` for the database.

## Supported DataSource Info

| Datasource | Supported versions                                   | Driver                  | Url                           | Maven                                                              |
|------------|------------------------------------------------------|-------------------------|-------------------------------|--------------------------------------------------------------------|
| DB2        | Different dependency version has different driver class. | com.ibm.db2.jcc.DB2Driver | jdbc:db2://127.0.0.1:50000/dbname | [Download](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc) |

## Data Type Mapping

|                                            DB2 Data Type                                             | SeaTunnel Data Type |
|------------------------------------------------------------------------------------------------------|---------------------|
| BOOLEAN                                                                                              | BOOLEAN             |
| SMALLINT                                                                                             | SHORT               |
| INT<br/>INTEGER<br/>                                                                                 | INTEGER             |
| BIGINT                                                                                               | LONG                |
| DECIMAL<br/>DEC<br/>NUMERIC<br/>NUM                                                                  | DECIMAL(38,18)      |
| REAL                                                                                                 | FLOAT               |
| FLOAT<br/>DOUBLE<br/>DOUBLE PRECISION<br/>DECFLOAT                                                   | DOUBLE              |
| CHAR<br/>VARCHAR<br/>LONG VARCHAR<br/>CLOB<br/>GRAPHIC<br/>VARGRAPHIC<br/>LONG VARGRAPHIC<br/>DBCLOB | STRING              |
| BLOB                                                                                                 | BYTES               |
| DATE                                                                                                 | DATE                |
| TIME                                                                                                 | TIME                |
| TIMESTAMP                                                                                            | TIMESTAMP           |
| ROWID<br/>XML                                                                                        | Not supported yet   |

## Sink Options

|                   Name                    |  Type   | Required | Default | Description                                                                                                                                                                                |
|-------------------------------------------|---------|----------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String  | Yes      | -       | JDBC connection URL, for example `jdbc:db2://127.0.0.1:50000/dbname`.                                                                                                                       |
| driver                                    | String  | Yes      | -       | JDBC driver class name. Use `com.ibm.db2.jcc.DB2Driver` for DB2.                                                                                                                            |
| username                                  | String  | No       | -       | Username for the DB2 instance.                                                                                                                                                              |
| password                                  | String  | No       | -       | Password for the DB2 instance.                                                                                                                                                             |
| query                                     | String  | No       | -       | SQL used to write upstream rows. Takes precedence over `database`/`table` auto-generated SQL, and disables catalog-based optimizations (no `MERGE` upsert).                              |
| database                                  | String  | No       | -       | Database name. When `generate_sink_sql = true`, used with `table` to generate `INSERT`/`MERGE` SQL. Mutually exclusive with `query`; when both are set, `query` wins.                         |
| table                                     | String  | No       | -       | Target table name. Used together with `database` and `generate_sink_sql` to generate writes.                                                                                              |
| primary_keys                              | Array   | No       | -       | Primary-key columns. Required when `generate_sink_sql = true` together with `enable_upsert = true` to build a `MERGE` upsert statement.                                                    |
| connection_check_timeout_sec              | Int     | No       | 30      | Seconds to wait for the connection check before failing.                                                                                                                                   |
| max_retries                               | Int     | No       | 0       | Number of retries on `executeBatch` failures.                                                                                                                                              |
| batch_size                                | Int     | No       | 1000    | Buffered-row threshold that triggers a flush. Also flushes at `checkpoint.interval`.                                                                                                       |
| batch_interval_ms                         | Long    | No       | 0       | Maximum time (ms) between two flushes. `0` disables interval-based flushing.                                                                                                              |
| is_exactly_once                           | Boolean | No       | false   | Enable XA-backed exactly-once. Requires `xa_data_source_class_name` to be set.                                                                                                             |
| generate_sink_sql                         | Boolean | No       | false   | Generate `INSERT` or `MERGE` SQL from `database`/`table`/`primary_keys` instead of supplying your own `query`.                                                                            |
| xa_data_source_class_name                 | String  | No       | -       | XA datasource class name. For DB2 use `com.ibm.db2.jcc.DB2XADataSource`.                                                                                                                   |
| max_commit_attempts                       | Int     | No       | 3       | Number of retries on transaction-commit failures.                                                                                                                                          |
| transaction_timeout_sec                   | Int     | No       | -1      | Transaction timeout in seconds. `-1` means no timeout. Setting a timeout may affect exactly-once.                                                                                          |
| auto_commit                               | Boolean | No       | true    | Whether to auto-commit each batch.                                                                                                                                                         |
| properties                                | Map     | No       | -       | Extra JDBC connection properties. When the same key appears in both `properties` and `url`, the precedence is driver-specific.                                                            |
| common-options                            |         | No       | -       | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.                                                              |
| enable_upsert                             | Boolean | No       | true    | When `primary_keys` is set with `generate_sink_sql = true`, generate `MERGE` upsert SQL. Set to `false` if your input has no duplicate keys and you want the faster insert-only path.       |

## Task Example

### Simple

This example reads 16 rows from a `FakeSource` and inserts them into `test_table` in DB2.

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
    url = "jdbc:db2://127.0.0.1:50000/dbname"
    driver = "com.ibm.db2.jcc.DB2Driver"
    username = "db2inst1"
    password = "123456"
    query = "insert into test_table(name, age) values(?, ?)"
  }
}
```

Before running this job, create the target database and table in DB2.

### Generate Sink SQL

Skip the manual `INSERT` and let SeaTunnel generate the SQL from `database` and `table`.

```hocon
sink {
  Jdbc {
    url = "jdbc:db2://127.0.0.1:50000/dbname"
    driver = "com.ibm.db2.jcc.DB2Driver"
    username = "db2inst1"
    password = "123456"
    generate_sink_sql = true
    database = test
    table = test_table
  }
}
```

### Exactly-once

Enable XA-backed exactly-once. The job will only commit each transaction when both phases succeed, and `max_retries`/`max_commit_attempts` provide extra robustness.

```hocon
sink {
  Jdbc {
    url = "jdbc:db2://127.0.0.1:50000/dbname"
    driver = "com.ibm.db2.jcc.DB2Driver"
    username = "db2inst1"
    password = "123456"
    query = "insert into test_table(name, age) values(?, ?)"
    max_retries = 0
    is_exactly_once = true
    xa_data_source_class_name = "com.ibm.db2.jcc.DB2XADataSource"
  }
}
```

### Generate Sink SQL With Upsert

When `generate_sink_sql = true` and `primary_keys` is set, DB2 writes via a generated `MERGE` statement. If the upstream data is insert-only, set `enable_upsert = false` for a faster insert-only path.

```hocon
sink {
  Jdbc {
    url = "jdbc:db2://127.0.0.1:50000/E2E"
    driver = "com.ibm.db2.jcc.DB2Driver"
    username = "db2inst1"
    password = "123456"
    database = "E2E"
    table = "SINK"
    generate_sink_sql = true
    enable_upsert = true
    primary_keys = ["C_INT"]
  }
}
```

## Changelog

<ChangeLog />
