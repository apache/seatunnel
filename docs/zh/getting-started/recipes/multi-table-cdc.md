---
sidebar_position: 6
title: 多表 CDC
---

# 多表 CDC

当你想用一个 SeaTunnel 作业同时采集多个上游表的变更，并自动把每张表路由到各自下游表时，可以使用这条链路。

## 前置条件

- 先完成 [跑第一个任务](../locally/run-your-first-job.md)。
- 安装 `connector-cdc-mysql` 和 `connector-jdbc`。
- 把 MySQL JDBC 驱动和目标库 JDBC 驱动都放到 `${SEATUNNEL_HOME}/lib`。
- 如果你希望下游具备稳定 upsert 行为，上游表需要有稳定主键。

## 最小配置

下面这个示例通过一个 `table-pattern` 同时读取多张 MySQL 表，并把它们分别写入 PostgreSQL 中名为 `st_<上游表名>` 的目标表。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MySQL-CDC {
    plugin_output = "mysql_multi"
    server-id = 5652
    username = "st_user_source"
    password = "mysqlpw"
    database-pattern = "inventory"
    table-pattern = "inventory\\.(orders|customers|products)"
    url = "jdbc:mysql://mysql:3306/inventory"
  }
}

sink {
  Jdbc {
    plugin_input = "mysql_multi"
    driver = "org.postgresql.Driver"
    url = "jdbc:postgresql://postgresql:5432/sync_demo"
    username = "postgres"
    password = "password"
    generate_sink_sql = true
    database = "sync_demo"
    table = "public.st_${table_name}"
    primary_keys = ["${primary_key}"]
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

## 验证结果

1. 启动作业，等首轮快照完成。
2. 确认下游自动创建了多张目标表。
3. 分别在上游多张表执行插入或更新，检查是否流入对应下游表。

```sql
SELECT COUNT(*) FROM public.st_orders;
SELECT COUNT(*) FROM public.st_customers;
SELECT COUNT(*) FROM public.st_products;
```

如果每张上游表都能进入对应的下游表，并且后续变更还能持续同步，这条多表 CDC 链路就是通的。

## 常见坑

- `table-pattern` 的正则没有转义好。在 HOCON 里，字面量 `.` 通常要写成 `\\.`。
- 没有配置基于占位符的 sink 路由，结果多张源表被写进了同一张目标表。
- 上游表没有主键，但下游配置却按 upsert 语义来用。
- 不同数据库对 schema 和 table 占位符的命名规则不同，直接照搬会失败。

## 相关文档

- [MySQL CDC Source](../../connectors/source/MySQL-CDC.md)
- [JDBC Sink](../../connectors/sink/Jdbc.md)
- [多表同步架构](../../architecture/features/multi-table.md)
