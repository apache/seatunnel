# TiDB CDC

> TiDB CDC模式的连接器

## 支持的引擎

> SeaTunnel Zeta<br/>
> Flink <br/>

## 主要功能

- [ ] [批处理](../../concept/connector-v2-features.md)
- [x] [流处理](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户定义的拆分](../../concept/connector-v2-features.md)

## Description

TiDB-CDC连接器允许从 TiDB 数据库读取快照数据和增量数据。本文将介绍如何设置 TiDB-CDC 连接器，在 TiDB 数据库中对数据进行快照和捕获流事件。

## 支持的数据源信息

| 数据源              | 支持的版本                                                                                                                                                | 驱动                       |                                Maven                                 |
|------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------|----------------------------------------------------------------------|
| MySQL            | <li> [MySQL](https://dev.mysql.com/doc): 5.5, 5.6, 5.7, 8.0.x </li><li> [RDS MySQL](https://www.aliyun.com/product/rds/mysql): 5.6, 5.7, 8.0.x </li> | com.mysql.cj.jdbc.Driver | https://mvnrepository.com/artifact/mysql/mysql-connector-java/8.0.28 |
| tikv-client-java | 3.2.0                                                                                                                                                | -                        | https://mvnrepository.com/artifact/org.tikv/tikv-client-java/3.2.0   |

## Using Dependency

### 安装驱动

#### 在 Flink 引擎下

> 1. 你需要确保 [jdbc 驱动 jar 包]（https:/mvnrepository.com/artifact/mysql/mysql-connector-java） 和 [tikv-client-java jar 包]（https:/mvnrepository.com/artifact/org.tikv/tikv-client-java/3.2.0） 已经放在目录 '${SEATUNNEL_HOME}/plugins/'.

#### 在 SeaTunnel Zeta 引擎下

> 1. 你需要确保 [jdbc 驱动 jar 包]（https:/mvnrepository.com/artifact/mysql/mysql-connector-java） 和 [tikv-client-java jar 包]（https:/mvnrepository.com/artifact/org.tikv/tikv-client-java/3.2.0） 已经放在目录 `${SEATUNNEL_HOME}/lib/` .

请下载Mysql驱动和tikv-java-client并将其放在`${SEATUNNEL_HOME}/lib/`目录中。例如：cp mysql-connector-java-xxx.jar`$SEATNUNNEL_HOME/lib/`

## 数据类型映射

|                                        Mysql Data Type                                         | SeaTunnel Data Type |
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

|             Name             | Type    | Required | Default |                                                                                                                                                                                         Description                                                                                                                                                                                          |
|------------------------------|---------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| base-url                     | String  | Yes      | -       | The URL of the JDBC connection. Refer to a case: `jdbc:mysql://tidb0:4000/inventory`.                                                                                                                                                                                                                                                                                                        |
| username                     | String  | Yes      | -       | Name of the database to use when connecting to the database server.                                                                                                                                                                                                                                                                                                                          |
| password                     | String  | Yes      | -       | Password to use when connecting to the database server.                                                                                                                                                                                                                                                                                                                                      |
| pd-addresses                 | String  | Yes      | -       | TiKV cluster's PD address                                                                                                                                                                                                                                                                                                                                                                    |
| database-name                | String  | Yes      | -       | Database name of the database to monitor.                                                                                                                                                                                                                                                                                                                                                    |
| table-name                   | String  | Yes      | -       | Table name of the database to monitor. The table name needs to include the database name.                                                                                                                                                                                                                                                                                                    |
| startup.mode                 | Enum    | No       | INITIAL | Optional startup mode for TiDB CDC consumer, valid enumerations are `initial`, `earliest`, `latest` and `specific`. <br/> `initial`: Synchronize historical data at startup, and then synchronize incremental data.<br/> `earliest`: Startup from the earliest offset possible.<br/> `latest`: Startup from the latest offset.<br/> `specific`: Startup from user-supplied specific offsets. |
| tikv.grpc.timeout_in_ms      | Long    | No       | -       | TiKV GRPC timeout in ms.                                                                                                                                                                                                                                                                                                                                                                     |
| tikv.grpc.scan_timeout_in_ms | Long    | No       | -       | TiKV GRPC scan timeout in ms.                                                                                                                                                                                                                                                                                                                                                                |
| tikv.batch_get_concurrency   | Integer | No       | -       | TiKV GRPC batch get concurrency                                                                                                                                                                                                                                                                                                                                                              |
| tikv.batch_scan_concurrency  | Integer | No       | -       | TiKV GRPC batch scan concurrency                                                                                                                                                                                                                                                                                                                                                             |

## 任务示例

### 简单示例

```
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  TiDB-CDC {
    result_table_name = "products_tidb_cdc"
    base-url = "jdbc:mysql://tidb0:4000/inventory"
    driver = "com.mysql.cj.jdbc.Driver"
    tikv.grpc.timeout_in_ms = 20000
    pd-addresses = "pd0:2379"
    username = "root"
    password = ""
    database-name = "inventory"
    table-name = "products"
  }
}

transform {
}

sink {
  jdbc {
    source_table_name = "products_tidb_cdc"
    url = "jdbc:mysql://tidb0:4000/inventory"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = ""
    database = "inventory"
    table = "products_sink"
    generate_sink_sql = true
    primary_keys = ["id"]
  }
}
```

