import ChangeLog from '../changelog/connector-cdc-tidb.md';

# TiDB CDC

> TiDB CDC 源连接器

## 支持以下引擎

> SeaTunnel Zeta<br/>
> Flink<br/>

## 描述

TiDB CDC 连接器通过 `tikv-client-java` 客户端与 TiKV 的 Placement Driver (PD) 和 TiKV 节点通信，读取 TiDB 的快照数据和增量变更事件。它支持并行快照读取和精确一次的流式消费，是把 TiDB 表接入 SeaTunnel 流水线的推荐方式。

## 支持的数据源信息

| 数据源              | 支持的版本                                                                                                                                                | 驱动                        | Url                              | Maven                                                                |
|------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------|----------------------------------|----------------------------------------------------------------------|
| MySQL            | <li> [MySQL](https://dev.mysql.com/doc): 5.5, 5.6, 5.7, 8.0.x </li><li> [RDS MySQL](https://www.aliyun.com/product/rds/mysql): 5.6, 5.7, 8.0.x </li> | com.mysql.cj.jdbc.Driver  | jdbc:mysql://localhost:3306/test | https://mvnrepository.com/artifact/mysql/mysql-connector-java/8.0.28 |
| tikv-client-java | 3.2.0                                                                                                                                                | -                         | -                                | https://mvnrepository.com/artifact/org.tikv/tikv-client-java/3.2.0   |

## 依赖

### 安装驱动

#### 在 Flink 引擎下

> 1. 你需要确保 [jdbc 驱动 jar 包](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 和 [tikv-client-java jar 包](https://mvnrepository.com/artifact/org.tikv/tikv-client-java/3.2.0) 已经放到目录 `${SEATUNNEL_HOME}/plugins/`。

#### 在 SeaTunnel Zeta 引擎下

> 1. 你需要确保 [jdbc 驱动 jar 包](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 和 [tikv-client-java jar 包](https://mvnrepository.com/artifact/org.tikv/tikv-client-java/3.2.0) 已经放到目录 `${SEATUNNEL_HOME}/lib/`。

请下载 MySQL 驱动和 `tikv-client-java`，并按所使用引擎的要求放到对应目录。

## 主要功能

- [ ] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户定义的拆分](../../introduction/concepts/connector-v2-features.md)

## 数据类型映射

| MySQL 数据类型                                                                                     | SeaTunnel 数据类型 |
|------------------------------------------------------------------------------------------------|----------------|
| BIT(1)<br/>TINYINT(1)                                                                          | BOOLEAN        |
| TINYINT                                                                                        | TINYINT        |
| TINYINT UNSIGNED<br/>SMALLINT                                                                  | SMALLINT       |
| SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED<br/>INT<br/>INTEGER<br/>YEAR            | INT            |
| INT UNSIGNED<br/>INTEGER UNSIGNED<br/>BIGINT                                                   | BIGINT         |
| BIGINT UNSIGNED                                                                                | DECIMAL(20,0)  |
| DECIMAL(p, s) <br/>DECIMAL(p, s) UNSIGNED <br/>NUMERIC(p, s) <br/>NUMERIC(p, s) UNSIGNED       | DECIMAL(p, s)  |
| FLOAT<br/>FLOAT UNSIGNED                                                                       | FLOAT          |
| DOUBLE<br/>DOUBLE UNSIGNED<br/>REAL<br/>REAL UNSIGNED                                          | DOUBLE         |
| CHAR<br/>VARCHAR<br/>TINYTEXT<br/>MEDIUMTEXT<br/>TEXT<br/>LONGTEXT<br/>ENUM<br/>JSON           | STRING         |
| DATE                                                                                           | DATE           |
| TIME(s)                                                                                        | TIME(s)        |
| DATETIME<br/>TIMESTAMP(s)                                                                      | TIMESTAMP(s)   |
| BINARY<br/>VARBINARY<br/>BIT(p)<br/>TINYBLOB<br/>MEDIUMBLOB<br/>BLOB<br/>LONGBLOB<br/>GEOMETRY | BYTES          |

## 源选项

| 名称                          | 类型      | 必需 | 默认      | 描述                                                                                                                                                                                              |
|-------------------------------|---------|----|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                           | String  | 是  | -       | 用于发现表元数据的 MySQL 兼容 JDBC URL，例如 `jdbc:mysql://tidb0:4000/inventory`。                                                                                                                         |
| username                      | String  | 是  | -       | 连接 TiDB 时使用的用户名。                                                                                                                                                                                |
| password                      | String  | 是  | -       | 连接 TiDB 时使用的密码。                                                                                                                                                                                |
| pd-addresses                  | String  | 是  | -       | TiKV Placement Driver（PD）地址，多个用逗号分隔，例如 `pd0:2379,pd1:2379`。                                                                                                                              |
| database-name                 | String  | 是  | -       | 要监控的 TiDB 数据库名。                                                                                                                                                                                |
| table-name                    | String  | 是  | -       | `database-name` 下要监控的表名，不要再包含数据库名。                                                                                                                                                              |
| startup.mode                  | Enum    | 否  | INITIAL | TiDB CDC 消费器的启动模式，可选值 `initial`、`earliest`、`latest`。`initial` 先同步历史数据再读增量；`earliest` 从最早的可用偏移量开始；`latest` 跳过初始快照，仅消费新变更。                                                                       |
| batch-size-per-scan           | Int     | 否  | 1000    | 每次向 TiKV 扫描请求拉取的行数。                                                                                                                                                                          |
| tikv.grpc.timeout_in_ms        | Long    | 否  | -       | TiKV gRPC 客户端超时时间（毫秒）。TiKV 响应较慢时可适当调大。                                                                                                                                                    |
| tikv.grpc.scan_timeout_in_ms   | Long    | 否  | -       | TiKV gRPC 扫描超时时间（毫秒）。大扫描任务超时时可适当调大。                                                                                                                                                      |
| tikv.batch_get_concurrency    | Integer | 否  | -       | TiKV `BatchGet` 请求的并发度。读操作受 TiKV CPU 限制时可上调。                                                                                                                                                  |
| tikv.batch_scan_concurrency    | Integer | 否  | -       | TiKV `BatchScan` 请求的并发度。快照读受 TiKV CPU 限制时可上调。                                                                                                                                               |

## 任务示例

### 简单示例

此示例把 TiDB 表的 CDC 事件流式写入 JDBC sink。请设置 `job.mode = "STREAMING"` 并配置 checkpoint 间隔，使增量事件持续流动。

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

### 从最新位置启动

如果只需要读取新的变更、不需要同步历史快照，可以使用 `startup.mode = "latest"`。

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

## 注意事项

- TiDB CDC 每个 source 块读取一张表；如果一个任务要读取多张表，请配置多个 `TiDB-CDC` source 块。
- `startup.mode = "specific"` 不是 TiDB CDC 的有效配置值，请使用 `initial`、`earliest` 或 `latest`。
- 只有默认 TiKV 客户端设置不能满足集群需求时，才需要调整 `tikv.grpc.*` 和 `tikv.batch_*_concurrency` 参数。

## 变更日志

<ChangeLog />
