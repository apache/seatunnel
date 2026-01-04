import ChangeLog from '../changelog/connector-cdc-tidb.md';

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

## 描述

TiDB-CDC连接器允许从 TiDB 数据库读取快照数据和增量数据。本文将介绍如何设置 TiDB-CDC 连接器，在 TiDB 数据库中对数据进行快照和捕获流事件。

## 支持的数据源信息

| 数据源              | 支持的版本                                                                                                                                                | 驱动                       |                                Maven                                 |
|------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------|----------------------------------------------------------------------|
| MySQL            | <li> [MySQL](https://dev.mysql.com/doc): 5.5, 5.6, 5.7, 8.0.x </li><li> [RDS MySQL](https://www.aliyun.com/product/rds/mysql): 5.6, 5.7, 8.0.x </li> | com.mysql.cj.jdbc.Driver | https://mvnrepository.com/artifact/mysql/mysql-connector-java/8.0.28 |
| tikv-client-java | 3.2.0                                                                                                                                                | -                        | https://mvnrepository.com/artifact/org.tikv/tikv-client-java/3.2.0   |

## Using Dependency

### 安装驱动

#### 在 Flink 引擎下

> 1. 你需要确保 [jdbc 驱动 jar 包](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 和 [tikv-client-java jar 包](https://mvnrepository.com/artifact/org.tikv/tikv-client-java/3.2.0) 已经放在目录 `${SEATUNNEL_HOME}/plugins/`。

#### 在 SeaTunnel Zeta 引擎下

> 1. 你需要确保 [jdbc 驱动 jar 包](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 和 [tikv-client-java jar 包](https://mvnrepository.com/artifact/org.tikv/tikv-client-java/3.2.0) 已经放在目录 `${SEATUNNEL_HOME}/lib/` 。

请下载 Mysql 驱动和 tikv-java-client 并将其放在 `${SEATUNNEL_HOME}/lib/` 目录中。例如：

```bash
cp mysql-connector-java-xxx.jar ${SEATUNNEL_HOME}/lib/
```

## 数据类型映射

| Mysql 数据类型                                                                                     | SeaTunnel 数据类型 |
|------------------------------------------------------------------------------------------------|----------------|
| BIT(1)<br/>TINYINT(1)                                                                          | BOOLEAN        |
| TINYINT                                                                                        | TINYINT        |
| TINYINT UNSIGNED<br/>SMALLINT                                                                  | SMALLINT       |
| SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED<br/>INT<br/>INTEGER<br/>YEAR            | INT            |
| INT UNSIGNED<br/>INTEGER UNSIGNED<br/>BIGINT                                                   | BIGINT         |
| BIGINT UNSIGNED                                                                                | DECIMAL(20,0)  |
| DECIMAL(p, s) <br/>DECIMAL(p, s) UNSIGNED <br/>NUMERIC(p, s) <br/>NUMERIC(p, s) UNSIGNED       | DECIMAL(p,s)   |
| FLOAT<br/>FLOAT UNSIGNED                                                                       | FLOAT          |
| DOUBLE<br/>DOUBLE UNSIGNED<br/>REAL<br/>REAL UNSIGNED                                          | DOUBLE         |
| CHAR<br/>VARCHAR<br/>TINYTEXT<br/>MEDIUMTEXT<br/>TEXT<br/>LONGTEXT<br/>ENUM<br/>JSON<br/>ENUM  | STRING         |
| DATE                                                                                           | DATE           |
| TIME(s)                                                                                        | TIME(s)        |
| DATETIME<br/>TIMESTAMP(s)                                                                      | TIMESTAMP(s)   |
| BINARY<br/>VARBINAR<br/>BIT(p)<br/>TINYBLOB<br/>MEDIUMBLOB<br/>BLOB<br/>LONGBLOB <br/>GEOMETRY | BYTES          |

## 源选项

| 名称                      | 类型      | 必需 | 默认      | 描述                                                                                                                                                                                             |
|-------------------------|---------|----|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                     | String  | 是  | -       | JDBC 连接的 URL，例如：`jdbc:mysql://tidb0:4000/inventory`。                                                                                                                                           |
| username                | String  | 是  | -       | 连接数据库服务器时使用的用户名。                                                                                                                                                                               |
| password                | String  | 是  | -       | 连接数据库服务器时使用的密码。                                                                                                                                                                                |
| pd-addresses            | String  | 是  | -       | TiKV 集群的 PD 地址。                                                                                                                                                                                |
| database-name           | String  | 是  | -       | 要监控的数据库名称。                                                                                                                                                                                     |
| table-name              | String  | 是  | -       | 要监控的表名称。表名称需要包含数据库名称。                                                                                                                                                                          |
| startup.mode            | Enum    | 否  | INITIAL | TiDB CDC 消费器的可选启动模式，可选值有 `initial`、`earliest`、`latest` 和 `specific`。<br/>`initial`：启动时同步历史数据，然后同步增量数据。<br/>`earliest`：从最早的可用偏移量开始启动。<br/>`latest`：从最新的偏移量开始启动。<br/>`specific`：从用户提供的特定偏移量开始启动。 |
| batch-size-per-scan     | Int     | 否  | 1000    | 每次扫描的大小。                                                                                                                                                                                       |
| tikv.grpc.timeout_in_ms | Long    | 否  | -       | TiKV GRPC 超时时间（毫秒）。                                                                                                                                                                            |
| tikv.grpc.scan_timeout_in_ms | Long    | 否  | -       | TiKV GRPC 扫描超时时间（毫秒）。                                                                                                                                                                          |
| tikv.batch_get_concurrency | Integer | 否  | -       | TiKV GRPC 批量获取并发度。                                                                                                                                                                             |
| tikv.batch_scan_concurrency | Integer | 否  | -       | TiKV GRPC 批量扫描并发度。                                                                                                                                                                             |

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
    plugin_output = "products_tidb_cdc"
    url = "jdbc:mysql://tidb0:4000/inventory"
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
    plugin_input = "products_tidb_cdc"
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

## 变更日志

<ChangeLog />