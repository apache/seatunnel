---
sidebar_position: 1
title: MySQL CDC 到 Doris
---

# MySQL CDC 到 Doris

当你想把 MySQL 的行级变更持续同步到 Doris，并让 Doris 始终保持最新状态时，可以直接使用这条链路。

## 前置条件

1. 先完成 [跑第一个任务](../locally/run-your-first-job.md)，确认本地基础链路正常。

2. 安装这条链路需要的插件。先看 [部署 > 下载连接器插件](../locally/deployment.md#下载连接器插件)，然后把 `config/plugin_config` 改成下面这样：

```plugin_config
--seatunnel-connectors--
connector-cdc-mysql
connector-doris
--end--
```

```bash
cd "${SEATUNNEL_HOME}"
sh bin/install-plugin.sh
ls connectors | rg 'connector-(cdc-mysql|doris)'
```

3. 如果你用的是 SeaTunnel Zeta，再把 MySQL JDBC 驱动放进 `${SEATUNNEL_HOME}/lib`，并确认 jar 已经落盘：

```bash
ls "${SEATUNNEL_HOME}/lib" | rg 'mysql-connector'
```

如果你用的是 Flink 或 Spark，就把同一个驱动 jar 放到对应引擎实际加载的插件目录里。

4. 先准备 MySQL 源表。这条教程依赖稳定主键，这样下游才能正确回放更新和删除事件。

```sql
CREATE DATABASE IF NOT EXISTS inventory;

CREATE TABLE IF NOT EXISTS inventory.orders (
  id BIGINT PRIMARY KEY,
  order_status VARCHAR(32),
  amount DECIMAL(10, 2),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO inventory.orders (id, order_status, amount, updated_at) VALUES
  (1001, 'CREATED', 19.99, NOW()),
  (1002, 'CREATED', 29.99, NOW());
```

5. 按照 [MySQL CDC Source](../../connectors/source/MySQL-CDC.md) 的要求创建 CDC 用户并授权：

```sql
CREATE USER IF NOT EXISTS 'st_user_source'@'%' IDENTIFIED BY 'mysqlpw';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
ON *.* TO 'st_user_source'@'%';
FLUSH PRIVILEGES;
```

6. 检查 MySQL binlog 是否已经满足 CDC 要求：

```sql
SHOW VARIABLES WHERE variable_name IN ('log_bin', 'binlog_format', 'binlog_row_image');
```

期望值是 `log_bin = ON`、`binlog_format = ROW`、`binlog_row_image = FULL`。
如果还没有配置好，就修改 `my.cnf` 并重启 MySQL：

```ini
[mysqld]
server-id = 223344
log_bin = mysql-bin
binlog_format = ROW
binlog_row_image = FULL
```

7. 先准备 Doris 目标库。这篇教程保留 `schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"`，所以第一次启动时会用 MySQL 主键信息自动创建 `sync_demo.orders`。

```sql
CREATE DATABASE IF NOT EXISTS sync_demo;
```

## 最小配置

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MySQL-CDC {
    plugin_output = "orders_cdc"
    parallelism = 1
    startup.mode = "initial"
    server-id = 5652
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["inventory.orders"]
    url = "jdbc:mysql://mysql:3306/inventory"
  }
}

sink {
  Doris {
    plugin_input = "orders_cdc"
    fenodes = "doris-fe:8030"
    username = "root"
    password = ""
    database = "sync_demo"
    table = "orders"
    sink.label-prefix = "orders-cdc"
    sink.enable-delete = true
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    doris.config = {
      format = "csv"
      column_separator = ","
    }
  }
}
```

## 运行任务

把配置保存为 `config/mysql-cdc-to-doris.conf`，然后用本地模式启动 SeaTunnel：

```bash
cd "${SEATUNNEL_HOME}"
./bin/seatunnel.sh --config ./config/mysql-cdc-to-doris.conf -m local
```

这是一条流式 CDC 作业，所以执行下面的验证 SQL 时，任务需要保持运行中。

## 验证结果

1. 启动作业，等首轮快照完成。
2. 在 MySQL 中执行下面几条变更：

```sql
INSERT INTO inventory.orders (id, order_status, amount, updated_at)
VALUES (1003, 'CREATED', 39.99, NOW());

UPDATE inventory.orders
SET order_status = 'PAID', updated_at = NOW()
WHERE id = 1001;

DELETE FROM inventory.orders
WHERE id = 1002;
```

3. 在 Doris 中查询最新结果：

```sql
SELECT COUNT(*) FROM sync_demo.orders;
SELECT id, order_status, amount FROM sync_demo.orders ORDER BY id;
```

此时你应该能看到 `1001` 变成 `PAID`，`1003` 成功插入，`1002` 已经消失。如果 MySQL 的新增、更新、删除都能体现在 Doris 里，这条链路就是通的。

## 常见坑

- MySQL 没开 binlog，或者 binlog 不是 `ROW` 格式。
- CDC 用户缺少复制相关权限。
- `server-id` 和其他 MySQL 副本或 CDC 作业冲突。
- 多个运行中的任务复用了同一个 `sink.label-prefix`，导致 Doris stream load 冲突。
- 开启了删除同步，但 Doris 目标表模型不支持预期的删除行为。
- 源表没有稳定主键，导致 Doris 自动建表和下游 upsert 结果都不稳定。

## 相关文档

- [MySQL CDC Source](../../connectors/source/MySQL-CDC.md)
- [Doris Sink](../../connectors/sink/Doris.md)
- [SeaTunnel 引擎快速开始](../locally/quick-start-seatunnel-engine.md)
