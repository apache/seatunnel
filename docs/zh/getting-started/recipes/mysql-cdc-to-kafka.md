---
sidebar_position: 2
title: MySQL CDC 到 Kafka：消息头元数据与字段整形
---

# MySQL CDC 到 Kafka：消息头元数据与字段整形

这篇文档只讲一条已经在 Docker 里完成端到端验证的链路。验证时间是 2026 年 7 月 16 日，下面保留的配置、输入数据和结果，都是这次实测里真正跑过并断言过的内容。

这条已验证链路是：

- `MySQL-CDC` 读取 `shop.orders` 的快照和后续 binlog
- `Metadata` 暴露库名、表名和变更类型
- `Sql` 重命名字段并补充业务字段
- `Kafka` 输出 JSON payload，同时把部分元数据写入消息头

## 这次验证环境具备的前置条件

这条 Docker 端到端验证在任务启动前，已经具备了下面这些条件：

- MySQL 使用了 `docker/server-gtids/my.cnf` 里的 GTID / binlog 配置。
- `MySQL-CDC` 插件的 `lib` 目录里已经放入了 MySQL JDBC driver JAR。
- 预先创建了名为 `st_user_source` 的 CDC 用户，并授予了 `SELECT`、`RELOAD`、`SHOW DATABASES`、`REPLICATION SLAVE`、`REPLICATION CLIENT`、`LOCK TABLES` 权限。
- SeaTunnel 任务启动前，Kafka topic `recipe_mysql_orders` 已经提前创建好。

## 已验证的源表数据

E2E 测试先创建了下面这张表，并插入两条初始数据：

```sql
CREATE TABLE orders (
  id BIGINT NOT NULL PRIMARY KEY,
  order_no VARCHAR(64) NOT NULL,
  user_id BIGINT NOT NULL,
  status INT NOT NULL,
  amount DECIMAL(10, 2) NOT NULL
);

INSERT INTO orders (id, order_no, user_id, status, amount) VALUES
  (1001, 'ORD-1001', 501, 0, 19.99),
  (1002, 'ORD-1002', 502, 1, 29.99);
```

任务启动后，测试又执行了这两条增量变更：

```sql
UPDATE shop.orders SET status = 2, amount = 39.99 WHERE id = 1001;

INSERT INTO shop.orders (id, order_no, user_id, status, amount) VALUES
  (1003, 'ORD-1003', 503, 0, 59.99);
```

## Docker 实测通过的完整配置

下面这份配置就是 Docker 测试环境里真实跑通过的那份作业配置。里面的主机名是当时测试网络里的服务别名。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  MySQL-CDC {
    plugin_output = "mysql_orders_raw"
    url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"
    username = "st_user_source"
    password = "mysqlpw"
    server-id = 5601-5604
    table-names = ["shop.orders"]
    startup.mode = "initial"
    schema-changes.enabled = false
  }
}

transform {
  Metadata {
    plugin_input = "mysql_orders_raw"
    plugin_output = "mysql_orders_with_meta"
    metadata_fields {
      Database = source_database
      Table = source_table
      RowKind = change_type
    }
  }

  Sql {
    plugin_input = "mysql_orders_with_meta"
    plugin_output = "kafka_orders"
    query = "select id as order_id, order_no, user_id, amount, case when status = 0 then 'CREATED' when status = 1 then 'PAID' when status = 2 then 'SHIPPED' else 'OTHER' end as status_name, source_database, source_table, change_type, CONCAT(source_database, '.', source_table) as source_name, 'mysql_cdc' as sync_source from dual where id is not null"
  }
}

sink {
  Kafka {
    plugin_input = "kafka_orders"
    bootstrap.servers = "kafkaCluster:9092"
    topic = "recipe_mysql_orders"
    format = json
    partition_key_fields = ["order_id"]
    kafka_headers_fields = ["source_database", "source_table", "change_type"]
  }
}
```

## 这次端到端验证实际证明了什么

Docker E2E 测试对下面这些结果做了断言：

- 快照阶段成功把初始 MySQL 数据写进了 Kafka。
- 订单 `1001` 的 payload 中包含 `order_id`、`status_name`、`source_name`、`sync_source`。
- 在快照阶段 `1001` 这条记录上，Kafka 消息头里包含 `source_database=shop`、`source_table=orders`、`change_type=+I`。
- 在同一条快照记录上，配置了 `kafka_headers_fields` 之后，这三个字段不会再留在 JSON payload 里。
- 执行 `UPDATE shop.orders SET status = 2 ... WHERE id = 1001` 后，`1001` 最新一条消息的 `change_type=+U`，`status_name=SHIPPED`。
- 插入 `1003` 后，`1003` 最新一条消息的 `change_type=+I`，`status_name=CREATED`。

## 为什么这条链路这样写

### 1. `Metadata` 只暴露了后续真的会用到的 CDC 字段

这份已验证配置里，只补了三个字段：

- `source_database`
- `source_table`
- `change_type`

### 2. `Sql` 负责统一做业务整形

这次已验证链路里，SQL 实际产出的结果是：

- 快照阶段，`1001` 被写成 `status_name=CREATED`
- 快照阶段，`1002` 被写成 `status_name=PAID`
- 更新之后，`1001` 变成了 `status_name=SHIPPED`
- 插入之后，`1003` 被写成 `status_name=CREATED`
- 这次被断言的记录里还包含了 `sync_source`，其中快照阶段的 `1001` 还包含 `source_name`

### 3. `kafka_headers_fields` 会同时影响 header 和 payload

这个行为是在快照阶段 `1001` 那条记录上明确断言过的：`source_database`、`source_table`、`change_type` 被写入 Kafka header 后，就不会继续保留在那条 JSON payload 里。

## 相关文档

- [`MySQL-CDC` source 连接器](../../connectors/source/MySQL-CDC.md)
- [`Kafka` sink 连接器](../../connectors/sink/Kafka.md)
- [`Metadata` transform](../../transforms/metadata.md)
- [`Sql` transform](../../transforms/sql.md)
