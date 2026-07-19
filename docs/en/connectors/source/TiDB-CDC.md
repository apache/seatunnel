import ChangeLog from '../changelog/connector-cdc-tidb.md';

# TiDB CDC

> TiDB CDC source connector

## Support Those Engines

> SeaTunnel Zeta<br/>
> Flink <br/>

## Key features

- [ ] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Description

The TiDB CDC connector allows for reading snapshot data and incremental data from TiDB database. This document
describes how to set up the TiDB CDC connector to snapshot data and capture streaming event in TiDB database.

## Supported DataSource Info

| Datasource       | Supported versions                                                                                                                                   | Driver                   | Url                              | Maven                                                                |
|------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------|----------------------------------|----------------------------------------------------------------------|
| MySQL            | <li> [MySQL](https://dev.mysql.com/doc): 5.5, 5.6, 5.7, 8.0.x </li><li> [RDS MySQL](https://www.aliyun.com/product/rds/mysql): 5.6, 5.7, 8.0.x </li> | com.mysql.cj.jdbc.Driver | jdbc:mysql://localhost:3306/test | https://mvnrepository.com/artifact/mysql/mysql-connector-java/8.0.28 |
| tikv-client-java | 3.2.0                                                                                                                                                | -                        | -                                | https://mvnrepository.com/artifact/org.tikv/tikv-client-java/3.2.0   |

## Using Dependency

### Install Jdbc Driver

#### For Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) and the [tikv-client-java jar package](https://mvnrepository.com/artifact/org.tikv/tikv-client-java/3.2.0) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

#### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) and the [tikv-client-java jar package](https://mvnrepository.com/artifact/org.tikv/tikv-client-java/3.2.0) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

Please download and put the MySQL driver and tikv-java-client in the directory required by your engine.

## Data Type Mapping

| Mysql Data Type                                                                                | SeaTunnel Data Type |
|------------------------------------------------------------------------------------------------|---------------------|
| BIT(1)<br/>TINYINT(1)                                                                          | BOOLEAN             |
| TINYINT                                                                                        | TINYINT             |
| TINYINT UNSIGNED<br/>SMALLINT                                                                  | SMALLINT            |
| SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED<br/>INT<br/>INTEGER<br/>YEAR            | INT                 |
| INT UNSIGNED<br/>INTEGER UNSIGNED<br/>BIGINT                                                   | BIGINT              |
| BIGINT UNSIGNED                                                                                | DECIMAL(20,0)       |
| DECIMAL(p, s) <br/>DECIMAL(p, s) UNSIGNED <br/>NUMERIC(p, s) <br/>NUMERIC(p, s) UNSIGNED       | DECIMAL(p,s)        |
| FLOAT<br/>FLOAT UNSIGNED                                                                       | FLOAT               |
| DOUBLE<br/>DOUBLE UNSIGNED<br/>REAL<br/>REAL UNSIGNED                                          | DOUBLE              |
| CHAR<br/>VARCHAR<br/>TINYTEXT<br/>MEDIUMTEXT<br/>TEXT<br/>LONGTEXT<br/>ENUM<br/>JSON<br/>ENUM  | STRING              |
| DATE                                                                                           | DATE                |
| TIME(s)                                                                                        | TIME(s)             |
| DATETIME<br/>TIMESTAMP(s)                                                                      | TIMESTAMP(s)        |
| BINARY<br/>VARBINAR<br/>BIT(p)<br/>TINYBLOB<br/>MEDIUMBLOB<br/>BLOB<br/>LONGBLOB <br/>GEOMETRY | BYTES               |

## Source Options

| Name                    | Type    | Required | Default | Description                                                                                                                                                                                                                                                                                                                                                                                  |
|-------------------------|---------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                     | String  | Yes      | -       | The URL of the JDBC connection. Refer to a case: `jdbc:mysql://tidb0:4000/inventory`.                                                                                                                                                                                                                                                                                                        |
| username                | String  | Yes      | -       | Username used to connect to the TiDB server.                                                                                                                                                                                                                                                                                                                                                 |
| password                | String  | Yes      | -       | Password to use when connecting to the database server.                                                                                                                                                                                                                                                                                                                                      |
| pd-addresses            | String  | Yes      | -       | TiKV cluster's PD address                                                                                                                                                                                                                                                                                                                                                                    |
| database-name           | String  | Yes      | -       | Database name of the database to monitor.                                                                                                                                                                                                                                                                                                                                                    |
| table-name              | String  | Yes      | -       | Table name to monitor in `database-name`. Do not include the database name here.                                                                                                                                                                                                                                                                                                             |
| startup.mode            | Enum    | No       | INITIAL | Optional startup mode for TiDB CDC consumer, valid enumerations are `initial`, `earliest` and `latest`. <br/> `initial`: Synchronize historical data at startup, and then synchronize incremental data.<br/> `earliest`: Startup from the earliest available offset.<br/> `latest`: Startup from the latest offset and skip the initial snapshot.                                                                                                           |
| batch-size-per-scan     | Int     | No       | 1000    | Size per scan.                                                                                                                                                                                                                                                                                                                                                                               |
| tikv.grpc.timeout_in_ms | Long    | No       | -       | TiKV GRPC timeout in ms.                                                                                                                                                                                                                                                                                                                                                                     |
| tikv.grpc.scan_timeout_in_ms | Long    | No       | -       | TiKV GRPC scan timeout in ms.                                                                                                                                                                                                                                                                                                                                                                |
| tikv.batch_get_concurrency | Integer | No       | -       | TiKV GRPC batch get concurrency                                                                                                                                                                                                                                                                                                                                                              |
| tikv.batch_scan_concurrency | Integer | No       | -       | TiKV GRPC batch scan concurrency                                                                                                                                                                                                                                                                                                                                                             |

## Task Example

### Simple

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  # This is a example source plugin **only for test and demonstrate the feature source plugin**
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

transform {
}

sink {
  jdbc {
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

## Notes

- TiDB CDC reads one table per source block. Use multiple `TiDB-CDC` source blocks if one job needs to capture multiple tables.
- `startup.mode = "specific"` is not a valid TiDB CDC option. Use `initial`, `earliest`, or `latest`.
- Tune `tikv.grpc.*` and `tikv.batch_*_concurrency` only when the default TiKV client settings are not enough for your cluster.

## Changelog

<ChangeLog />
