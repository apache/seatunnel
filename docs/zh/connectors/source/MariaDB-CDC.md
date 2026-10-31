import ChangeLog from '../changelog/connector-cdc-mariadb.md';

# MariaDB CDC

> MariaDB CDC 数据源连接器

## 支持的引擎

> SeaTunnel Zeta<br/>
> Flink <br/>

## 描述

MariaDB CDC 连接器支持从 MariaDB 数据库中读取全量历史数据与增量变更数据。

## 关键特性

- [ ] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次 (Exactly-once)](../../introduction/concepts/connector-v2-features.md)
- [ ] [列裁剪](../../introduction/concepts/connector-v2-features.md)
- [x] [并行读取](../../introduction/concepts/connector-v2-features.md)
- [x] [支持自定义分片](../../introduction/concepts/connector-v2-features.md)

## 支持的数据源信息

| 数据源 | 支持的版本 | 驱动 | 连接 URL | Maven 依赖 |
|------------|-------------------|--------|-----|-------|
| MariaDB | 10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.8, 10.9, 10.10, 10.11, 11.x | org.mariadb.jdbc.Driver | jdbc:mariadb://localhost:3306/test | https://mvnrepository.com/artifact/org.mariadb.jdbc/mariadb-java-client |

## 驱动安装

### Flink 引擎

> 确保 `mariadb-java-client` 驱动已放入 `${SEATUNNEL_HOME}/plugins/` 目录。

### SeaTunnel Zeta 引擎

> 确保 `mariadb-java-client` 驱动已放入 `${SEATUNNEL_HOME}/lib/` 目录。

## 参数配置

### url [string]

MariaDB JDBC 连接 URL。

### username [string]

数据库用户名。

### password [string]

数据库密码。

### database-names [array]

需要读取的数据库名称列表。

### table-names [array]

需要读取的表名称列表，例如 `["inventory.products", "inventory.orders"]`。

### startup.mode [string]

启动模式：
- `initial` (默认): 先全量快照，再读取 binlog 增量。
- `earliest`: 从最早可用的 binlog 位置开始读取。
- `latest`: 从最新的 binlog 位置开始读取。
- `specific`: 从指定的 binlog 位点或 GTID 开始读取。
- `timestamp`: 从指定的时间戳开始读取。

### stop.mode [string]

停止模式：
- `never` (默认): 持续读取增量变更。
- `latest`: 读取到任务启动时的最新 binlog 位点后停止。
- `specific`: 读取到指定的 binlog 位点后停止。

## 配置示例

```hocon
source {
  MariaDB-CDC {
    url = "jdbc:mariadb://localhost:3306/inventory"
    username = "seatunnel"
    password = "password"
    table-names = ["inventory.products"]
    startup.mode = "initial"
  }
}
```

## 变更日志

<ChangeLog />
