import ChangeLog from '../changelog/connector-cdc-oceanbase.md';

# OceanBase CDC

> OceanBase CDC 源连接器

## 支持这些引擎

> SeaTunnel Zeta<br/>
> Flink <br/>

## 主要功能

- [ ] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户定义的拆分](../../introduction/concepts/connector-v2-features.md)

## 描述

OceanBase CDC 首个交付版本只覆盖稳定、可验证的 OceanBase MySQL 兼容模式路径，并依赖
OceanBase Binlog Service 提供增量订阅能力。

为了把实现范围控制在最小且确保重启恢复语义可靠，`OceanBase-CDC` 直接复用 SeaTunnel
现有 `MySQL-CDC` 的增量运行时，包括：

- 全量快照 + 增量读取
- checkpoint / restore 处理
- 多表 CDC 行语义
- `MySQL-CDC` 已支持的模式演进能力

本次首批交付暂不支持 OceanBase Oracle 兼容模式的增量 CDC。

## 支持的数据源信息

| 数据源 | 支持版本 | 驱动 | URL | 说明 |
| --- | --- | --- | --- | --- |
| OceanBase CE / OceanBase EE（MySQL 兼容模式） | 暴露 MySQL 兼容快照读取端点，并开启 OceanBase Binlog Service 的部署 | `com.mysql.cj.jdbc.Driver` | `jdbc:mysql://localhost:2881/test` | 依赖 OceanBase Binlog Service |

## 依赖使用

### 安装 JDBC 驱动

#### 对于 Flink 引擎

> 1. 你需要确保 [MySQL JDBC 驱动](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 已经放在 `${SEATUNNEL_HOME}/plugins/` 目录下。

#### 对于 SeaTunnel Zeta 引擎

> 1. 你需要确保 [MySQL JDBC 驱动](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 已经放在 `${SEATUNNEL_HOME}/lib/` 目录下。

## OceanBase 前置准备

在使用 `OceanBase-CDC` 之前，请确保被采集租户满足以下条件：

1. OceanBase 运行在 MySQL 兼容模式。
2. OceanBase Binlog Service 已部署并开启增量订阅能力。
3. JDBC `url` 指向 SeaTunnel 可用于快照读取的 MySQL 兼容端点。
4. 配置账号同时具备快照读取和增量订阅所需权限。

## 源端可选项

`OceanBase-CDC` 有意复用了 `MySQL-CDC` 的完整参数契约。

完整参数请直接参考 [MySQL CDC 源端参数](./MySQL-CDC.md#配置参数选项)。

### OceanBase 特有约束

- JDBC URL 需要使用 MySQL 兼容写法，例如 `jdbc:mysql://host:2881/database`。
- JDBC 驱动固定使用 MySQL 驱动 `com.mysql.cj.jdbc.Driver`。
- 首批版本只支持显式配置的表，即通过 `table-names`、`table-pattern`、
  `table-names-config` 指定采集范围。
- 启动模式、checkpoint / restore 语义、模式演进行为与 `MySQL-CDC` 保持一致。

## 任务示例

### 简单示例

> 读取 OceanBase MySQL 兼容模式下的多张表

```
env {
  execution.parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  OceanBase-CDC {
    plugin_output = "oceanbase_cdc"
    username = "root"
    password = "123456"
    url = "jdbc:mysql://127.0.0.1:2881/inventory"
    database-names = ["inventory"]
    table-names = ["inventory.orders", "inventory.customers"]
    server-time-zone = "Asia/Shanghai"
    startup.mode = "initial"
    exactly_once = true
  }
}

transform {

}

sink {
  Console {}
}
```

## 变更日志

<ChangeLog />
