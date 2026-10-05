---
sidebar_position: 6
title: 多表 CDC
---

# 多表 CDC

当你想用一个 SeaTunnel 作业同时采集多个上游表的变更，并自动把每张表路由到各自下游表时，可以使用这条链路。

## 前置条件

1. 先完成 [跑第一个任务](../locally/run-your-first-job.md)。

2. 安装这条链路需要的插件。先看 [部署 > 下载连接器插件](../locally/deployment.md#下载连接器插件)，然后把 `config/plugin_config` 改成下面这样：

```plugin_config
--seatunnel-connectors--
connector-cdc-mysql
connector-jdbc
--end--
```

```bash
cd "${SEATUNNEL_HOME}"
sh bin/install-plugin.sh
ls connectors | rg 'connector-(cdc-mysql|jdbc)'
```

3. 如果你用的是 SeaTunnel Zeta，再把 MySQL JDBC 驱动和 PostgreSQL JDBC 驱动都放进 `${SEATUNNEL_HOME}/lib`，并确认 jar 已经能看到：

```bash
ls "${SEATUNNEL_HOME}/lib" | rg 'mysql-connector|postgresql'
```

4. 先准备 MySQL 源表。因为这条链路会自动把 CDC 事件路由到下游 upsert 表，所以每张上游表都要有稳定主键。

```sql
CREATE DATABASE IF NOT EXISTS inventory;

CREATE TABLE IF NOT EXISTS inventory.orders (
  id BIGINT PRIMARY KEY,
  order_status VARCHAR(32),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS inventory.customers (
  id BIGINT PRIMARY KEY,
  customer_name VARCHAR(64),
  city VARCHAR(64),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS inventory.products (
  id BIGINT PRIMARY KEY,
  product_name VARCHAR(64),
  unit_price DECIMAL(10, 2),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO inventory.orders (id, order_status, updated_at) VALUES
  (2001, 'CREATED', NOW());

INSERT INTO inventory.customers (id, customer_name, city, updated_at) VALUES
  (3001, 'Alice', 'Shanghai', NOW());

INSERT INTO inventory.products (id, product_name, unit_price, updated_at) VALUES
  (4001, 'Keyboard', 99.00, NOW());
```

5. 创建 MySQL CDC 用户并授权：

```sql
CREATE USER IF NOT EXISTS 'st_user_source'@'%' IDENTIFIED BY 'mysqlpw';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
ON *.* TO 'st_user_source'@'%';
FLUSH PRIVILEGES;
```

6. 检查 MySQL binlog 是否满足 CDC 要求：

```sql
SHOW VARIABLES WHERE variable_name IN ('log_bin', 'binlog_format', 'binlog_row_image');
```

期望值是 `log_bin = ON`、`binlog_format = ROW`、`binlog_row_image = FULL`。

7. 准备 PostgreSQL 目标库，并给 sink 用户授予在 `public` schema 自动建表的权限：

```sql
CREATE USER st_user_sink WITH PASSWORD 'pgpw';
CREATE DATABASE sync_demo;
GRANT ALL PRIVILEGES ON DATABASE sync_demo TO st_user_sink;
```

重新连到 `sync_demo` 以后，再执行：

```sql
GRANT USAGE, CREATE ON SCHEMA public TO st_user_sink;
```

这篇教程使用 `generate_sink_sql = true`，所以第一次运行时 SeaTunnel 会自动创建 `public.st_orders`、`public.st_customers` 这类目标表。

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
    startup.mode = "initial"
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
    username = "st_user_sink"
    password = "pgpw"
    generate_sink_sql = true
    database = "sync_demo"
    table = "public.st_${table_name}"
    primary_keys = ["${primary_key}"]
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

## 运行任务

把配置保存为 `config/multi-table-cdc.conf`，然后用本地模式启动 SeaTunnel：

```bash
cd "${SEATUNNEL_HOME}"
./bin/seatunnel.sh --config ./config/multi-table-cdc.conf -m local
```

这是一条流式 CDC 作业，所以执行下面的验证步骤时，任务需要保持运行中。

## 验证结果

1. 启动作业，等首轮快照完成。
2. 先确认 PostgreSQL 里已经自动创建出多张目标表：

```sql
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public' AND table_name LIKE 'st_%'
ORDER BY table_name;
```

3. 分别在 MySQL 源表执行几条变更：

```sql
INSERT INTO inventory.orders (id, order_status, updated_at)
VALUES (2002, 'PAID', NOW());

UPDATE inventory.customers
SET city = 'Hangzhou', updated_at = NOW()
WHERE id = 3001;

INSERT INTO inventory.products (id, product_name, unit_price, updated_at)
VALUES (4002, 'Mouse', 59.00, NOW());
```

4. 检查每张目标表是否只接收到自己的数据：

```sql
SELECT id, order_status FROM public.st_orders ORDER BY id;
SELECT id, customer_name, city FROM public.st_customers ORDER BY id;
SELECT id, product_name, unit_price FROM public.st_products ORDER BY id;
```

如果每张上游表都能进入对应的下游表，并且后续变更还能持续同步，这条多表 CDC 链路就是通的。

## 常见坑

- `table-pattern` 的正则没有转义好。在 HOCON 里，字面量 `.` 通常要写成 `\\.`。
- MySQL binlog 或 CDC 用户权限不完整，导致任务只能读快照，后续增量读不出来。
- 没有配置基于占位符的 sink 路由，结果多张源表被写进了同一张目标表。
- PostgreSQL sink 用户虽然能连库，但没有 `public` schema 的 `CREATE` 权限，自动建表会失败。
- 上游表没有主键，但下游配置却按 upsert 语义来用。
- 不同数据库对 schema 和 table 占位符的命名规则不同，直接照搬会失败。

## 相关文档

- [MySQL CDC Source](../../connectors/source/MySQL-CDC.md)
- [JDBC Sink](../../connectors/sink/Jdbc.md)
- [多表同步架构](../../architecture/features/multi-table.md)
