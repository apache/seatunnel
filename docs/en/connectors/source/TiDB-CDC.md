import ChangeLog from '../changelog/connector-cdc-tidb.md';

# TiDB CDC

> TiDB CDC source connector

## Support Those Engines

> SeaTunnel Zeta<br/>
> Flink<br/>

## Description

The TiDB CDC connector reads snapshot data and incremental change events from TiDB by talking to its TiKV placement-driver (PD) and TiKV nodes through the `tikv-client-java` Java client. It supports parallel snapshot reads and exactly-once streaming, and is the recommended way to bring TiDB tables into a SeaTunnel pipeline.

## Supported DataSource Info

| Datasource       | Supported versions                                                                                                                                   | Driver                   | Url                              | Maven                                                                |
|------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------|----------------------------------|----------------------------------------------------------------------|
| MySQL            | <li> [MySQL](https://dev.mysql.com/doc): 5.5, 5.6, 5.7, 8.0.x </li><li> [RDS MySQL](https://www.alibabacloud.com/help/en/rds/apsaradb-rds-for-mysql/): 5.6, 5.7, 8.0.x </li> | com.mysql.cj.jdbc.Driver | jdbc:mysql://localhost:3306/test | https://mvnrepository.com/artifact/mysql/mysql-connector-java/8.0.28 |
| tikv-client-java | 3.2.0                                                                                                                                                | -                        | -                                | https://mvnrepository.com/artifact/org.tikv/tikv-client-java/3.2.0   |

## Using Dependency

### Install JDBC Driver

#### For Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) and the [tikv-client-java jar package](https://mvnrepository.com/artifact/org.tikv/tikv-client-java/3.2.0) have been placed in directory `${SEATUNNEL_HOME}/plugins/`.

#### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) and the [tikv-client-java jar package](https://mvnrepository.com/artifact/org.tikv/tikv-client-java/3.2.0) have been placed in directory `${SEATUNNEL_HOME}/lib/`.

Please download and put the MySQL driver and `tikv-client-java` in the directory required by your engine.

## Key features

- [ ] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Data Type Mapping

| MySQL Data Type                                                                              | SeaTunnel Data Type |
|----------------------------------------------------------------------------------------------|---------------------|
| BIT(1)<br/>TINYINT(1)                                                                        | BOOLEAN             |
| TINYINT                                                                                      | TINYINT             |
| TINYINT UNSIGNED<br/>SMALLINT                                                                | SMALLINT            |
| SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED<br/>INT<br/>INTEGER<br/>YEAR          | INT                 |
| INT UNSIGNED<br/>INTEGER UNSIGNED<br/>BIGINT                                                 | BIGINT              |
| BIGINT UNSIGNED                                                                              | DECIMAL(20, 0)      |
| DECIMAL(p, s) <br/>DECIMAL(p, s) UNSIGNED <br/>NUMERIC(p, s) <br/>NUMERIC(p, s) UNSIGNED     | DECIMAL(p, s)       |
| FLOAT<br/>FLOAT UNSIGNED                                                                     | FLOAT               |
| DOUBLE<br/>DOUBLE UNSIGNED<br/>REAL<br/>REAL UNSIGNED                                        | DOUBLE              |
| CHAR<br/>VARCHAR<br/>TINYTEXT<br/>MEDIUMTEXT<br/>TEXT<br/>LONGTEXT<br/>ENUM<br/>JSON         | STRING              |
| DATE                                                                                         | DATE                |
| TIME(s)                                                                                      | TIME(s)             |
| DATETIME<br/>TIMESTAMP(s)                                                                    | TIMESTAMP(s)        |
| BINARY<br/>VARBINARY<br/>BIT(p)<br/>TINYBLOB<br/>MEDIUMBLOB<br/>BLOB<br/>LONGBLOB<br/>GEOMETRY | BYTES             |

## Source Options

| Name                          | Type    | Required | Default  | Description                                                                                                                                                                                                                                                                                                              |
|-------------------------------|---------|----------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                           | String  | Yes      | -        | MySQL-compatible JDBC URL used to discover table metadata. Example: `jdbc:mysql://tidb0:4000/inventory`.                                                                                                                                                                                                                  |
| username                      | String  | Yes      | -        | Username used to connect to the TiDB server.                                                                                                                                                                                                                                                                             |
| password                      | String  | Yes      | -        | Password used to connect to the TiDB server.                                                                                                                                                                                                                                                                             |
| pd-addresses                  | String  | Yes      | -        | TiKV placement-driver (PD) endpoints, comma-separated, e.g. `pd0:2379,pd1:2379`.                                                                                                                                                                                                                                          |
| database-name                 | String  | No       | -        | Name of the TiDB database to monitor. Used together with `table-name` for single-table capture. Either this pair or `table-names` must be set.                                                                                                                                                                              |
| table-name                    | String  | No       | -        | Table name to monitor inside `database-name`. Do not include the database name.                                                                                                                                                                                                                                          |
| table-names                   | List    | No       | -        | Tables to capture in `database.table` format, e.g. `["inventory.products", "inventory.orders"]`, enabling multi-table sync from a single source. Each element is split on the first dot, so table names may themselves contain dots. Takes precedence over `database-name`/`table-name` when both are set.                 |
| startup.mode                  | Enum    | No       | INITIAL  | Optional startup mode for the TiDB CDC consumer. Valid values are `initial`, `earliest`, `latest`. `initial` snapshots historical data first, then keeps reading incremental changes. `earliest` starts from the earliest available offset. `latest` skips the initial snapshot and only consumes new changes from now on.    |
| batch-size-per-scan           | Int     | No       | 1000     | Number of rows fetched per scan request against TiKV.                                                                                                                                                                                                                                                                     |
| tikv.grpc.timeout_in_ms        | Long    | No       | -        | TiKV gRPC client timeout in milliseconds. Increase it when TiKV is slow to respond under load.                                                                                                                                                                                                                            |
| tikv.grpc.scan_timeout_in_ms   | Long    | No       | -        | TiKV gRPC scan timeout in milliseconds. Increase it when large scans time out.                                                                                                                                                                                                                                            |
| tikv.batch_get_concurrency    | Integer | No       | -        | Concurrency for TiKV `BatchGet` requests. Tune upward when reads are bottlenecked by TiKV CPU.                                                                                                                                                                                                                            |
| tikv.batch_scan_concurrency    | Integer | No       | -        | Concurrency for TiKV `BatchScan` requests. Tune upward when snapshot reads are bottlenecked by TiKV CPU.                                                                                                                                                                                                                  |

## Task Example

### Simple

This example streams CDC events from a TiDB table into a JDBC sink. Set `job.mode = "STREAMING"` and a checkpoint interval so incremental events flow continuously.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  TiDB-CDC {
    plugin_output = "products_tidb_cdc"
    url = "jdbc:mysql://tidb0:4000/tidb_cdc"
    driver = "com.mysql.cj.jdbc.Driver"
    tikv.grpc.timeout_in_ms = 20000
    pd-addresses = "pd0:2379"
    username = "root"
    password = ""
    database-name = "tidb_cdc"
    table-name = "tidb_cdc_e2e_source_table"
  }
}

sink {
  Jdbc {
    plugin_input = "products_tidb_cdc"
    url = "jdbc:mysql://tidb0:4000/tidb_cdc"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "root"
    password = ""
    database = "tidb_cdc"
    table = "tidb_cdc_e2e_sink_table"
    generate_sink_sql = true
    primary_keys = ["id"]
  }
}
```

### Start From Latest Offset

Use `startup.mode = "latest"` when only new changes are needed and the historical snapshot should be skipped.

```hocon
source {
  TiDB-CDC {
    url = "jdbc:mysql://tidb0:4000/tidb_cdc"
    driver = "com.mysql.cj.jdbc.Driver"
    pd-addresses = "pd0:2379"
    username = "root"
    password = ""
    database-name = "tidb_cdc"
    table-name = "tidb_cdc_e2e_source_table"
    startup.mode = "latest"
  }
}
```

### Multi-Table Sync

Capture multiple tables with a single source block using `table-names`, and fan them out to matching sink tables with the `${table_name}` placeholder:

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  TiDB-CDC {
    plugin_output = "products_tidb_cdc_multi"
    url = "jdbc:mysql://tidb0:4000/tidb_cdc"
    driver = "com.mysql.cj.jdbc.Driver"
    pd-addresses = "pd0:2379"
    username = "root"
    password = ""
    table-names = ["tidb_cdc.products", "tidb_cdc.orders"]
  }
}

sink {
  jdbc {
    plugin_input = "products_tidb_cdc_multi"
    url = "jdbc:mysql://tidb0:4000/tidb_cdc_sink"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "root"
    password = ""
    database = tidb_cdc_sink
    table = "${table_name}"
    primary_keys = ["${primary_key}"]
    generate_sink_sql = true
  }
}
```

## Notes

- A single `TiDB-CDC` block can capture multiple tables via `table-names`, or one table via `database-name` + `table-name`. Setting neither pair fails job validation.
- When a job is restored from a savepoint, tables added to `table-names` run a fresh snapshot before joining the incremental stream. Tables removed from `table-names` stop being captured and their checkpoint positions are discarded; if such a table is added back later it runs a fresh snapshot again.
- Splits are table key ranges assigned to readers round-robin, so a reader usually holds splits of several captured tables. Transaction assembly is isolated per table, so mixing tables on one reader does not affect correctness.
- Change-event assembly buffers (pre-write/commit staging and the committed-event queue that decouples pulling from writing) are maintained per table inside each reader. When the downstream sink stalls, every captured table fills its own buffers at the same time, so worst-case per-reader memory grows linearly with the number of captured tables — size reader memory accordingly for wide multi-table jobs.
- `startup.mode = "specific"` is not a valid TiDB CDC option. Use `initial`, `earliest`, or `latest`.
- Tune `tikv.grpc.*` and `tikv.batch_*_concurrency` only when the default TiKV client settings are not enough for your cluster.

## Changelog

<ChangeLog />
