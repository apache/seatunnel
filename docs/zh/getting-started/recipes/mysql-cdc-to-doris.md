---
sidebar_position: 1
title: MySQL CDC 到 Doris
---

# MySQL CDC 到 Doris

当你想把 MySQL 的行级变更持续同步到 Doris，并让 Doris 始终保持最新状态时，可以直接使用这条链路。

## 前置条件

- 先完成 [跑第一个任务](../locally/run-your-first-job.md)，确认本地基础链路正常。
- 安装 `connector-cdc-mysql` 和 `connector-doris`。
- 如果使用 SeaTunnel Zeta，把 MySQL JDBC 驱动放到 `${SEATUNNEL_HOME}/lib`。
- 打开 MySQL binlog，并确保格式是 `ROW`。
- 为 CDC 用户授予 `SELECT`、`RELOAD`、`SHOW DATABASES`、`REPLICATION SLAVE`、`REPLICATION CLIENT` 权限。
- 准备好 Doris 目标库和目标表。如果你要同步删除事件，目标表模型要支持对应删除行为。

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

## 验证结果

1. 启动作业，等首轮快照完成。
2. 在 MySQL 里执行几条插入、更新、删除。
3. 在 Doris 中查询最新结果。

```sql
SELECT COUNT(*) FROM sync_demo.orders;
SELECT id, order_status, updated_at FROM sync_demo.orders ORDER BY id;
```

如果 MySQL 里的新增、更新、删除都能体现在 Doris 里，这条链路就是通的。

## 常见坑

- MySQL 没开 binlog，或者 binlog 不是 `ROW` 格式。
- CDC 用户缺少复制相关权限。
- 多个运行中的任务复用了同一个 `sink.label-prefix`，导致 Doris stream load 冲突。
- 开启了删除同步，但 Doris 目标表模型不支持预期的删除行为。
- 源表没有稳定主键，导致下游 upsert 结果不确定。

## 相关文档

- [MySQL CDC Source](../../connectors/source/MySQL-CDC.md)
- [Doris Sink](../../connectors/sink/Doris.md)
- [SeaTunnel 引擎快速开始](../locally/quick-start-seatunnel-engine.md)
