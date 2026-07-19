---
title: PostgreSQL CDC 到 Iceberg：字段整形与 Upsert
---

# PostgreSQL CDC 到 Iceberg：字段整形与 Upsert

这个场景会先读取 PostgreSQL 全量快照，再持续捕获增量变更，对字段进行清洗和补充，最后在 Iceberg 中维护最新状态。示例使用 PostgreSQL 14 和 `pgoutput`，覆盖快照、更新、新增和删除事件。

完整链路是：

- `Postgres-CDC` 通过 PostgreSQL 逻辑复制读取 `sales.inventory.customer_orders`。
- `Sql` 清理客户名称、统一状态值，并填充 `sync_source`。
- `Iceberg` 使用 `id` 作为标识字段，以 upsert 模式应用 CDC 事件。

## 前置条件

1. 先使用 Zeta 引擎完成[运行第一个任务](../locally/run-your-first-job.md)。

2. 安装所需 connector：

```plugin_config
--seatunnel-connectors--
connector-cdc-postgres
connector-iceberg
--end--
```

```bash
cd "${SEATUNNEL_HOME}"
sh bin/install-plugin.sh
ls connectors | grep -E 'connector-(cdc-postgres|iceberg)'
```

3. Zeta 引擎需要把兼容版本的 PostgreSQL JDBC 驱动放入 `${SEATUNNEL_HOME}/lib`：

```bash
ls "${SEATUNNEL_HOME}/lib" | grep 'postgresql-'
```

4. 在 PostgreSQL 主库启用逻辑复制。修改 `wal_level` 后必须重启 PostgreSQL：

```conf
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
```

确认实际生效值：

```sql
SHOW wal_level;
SHOW max_replication_slots;
SHOW max_wal_senders;
```

5. 创建专用 CDC 用户，并按实际环境替换数据库、schema、用户名和密码：

```sql
CREATE ROLE seatunnel_cdc WITH REPLICATION LOGIN PASSWORD 'change_me';
GRANT CONNECT ON DATABASE sales TO seatunnel_cdc;
GRANT USAGE ON SCHEMA inventory TO seatunnel_cdc;
GRANT SELECT ON TABLE inventory.customer_orders TO seatunnel_cdc;
```

6. 设置包含变更前完整行数据的 replica identity，满足 CDC 默认安全检查：

```sql
ALTER TABLE inventory.customer_orders REPLICA IDENTITY FULL;
```

7. 选择所有 SeaTunnel Worker 都能访问且可写的空 Iceberg warehouse。只有所有 Worker 共享同一文件系统时才适合使用本地 `file://` 路径；分布式集群应使用 HDFS、S3 或其他共享 catalog 存储。

## 准备源数据

在 PostgreSQL 14 中创建源 schema 和表：

```sql
CREATE SCHEMA inventory;

CREATE TABLE inventory.customer_orders (
  id BIGINT PRIMARY KEY,
  customer_name VARCHAR(64) NOT NULL,
  amount NUMERIC(10, 2) NOT NULL,
  status VARCHAR(16) NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

ALTER TABLE inventory.customer_orders REPLICA IDENTITY FULL;

INSERT INTO inventory.customer_orders VALUES
  (1001, ' Alice Zhang ', 120.50, 'pending', '2026-07-18 09:00:00'),
  (1002, 'Bob Li', 80.00, 'paid', '2026-07-18 09:05:00');
```

初始快照进入 Iceberg 后，在 PostgreSQL 中执行以下变更：

```sql
UPDATE inventory.customer_orders
SET amount = 150.75, status = 'paid', updated_at = '2026-07-18 10:00:00'
WHERE id = 1001;

INSERT INTO inventory.customer_orders VALUES
  (1003, ' Carol Wang ', 42.00, 'pending', '2026-07-18 10:05:00');

DELETE FROM inventory.customer_orders WHERE id = 1002;
```

## 完整任务配置

下面的 HOCON 实现了完整链路。请按实际环境替换示例主机名、账号、slot 名称和 warehouse 路径。同一个 PostgreSQL 实例上的并发 CDC 任务必须使用不同的 `slot.name`。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 3000
}

source {
  Postgres-CDC {
    plugin_output = "postgres_orders_raw"
    url = "jdbc:postgresql://postgres_iceberg_recipe:5432/sales"
    username = "postgres"
    password = "postgres"
    database-names = ["sales"]
    schema-names = ["inventory"]
    table-names = ["sales.inventory.customer_orders"]
    startup.mode = "initial"
    decoding.plugin.name = "pgoutput"
    slot.name = ${slot_name}
  }
}

transform {
  Sql {
    plugin_input = "postgres_orders_raw"
    plugin_output = "iceberg_customer_orders"
    query = "select id, trim(customer_name) as customer_name, amount, upper(status) as status_name, updated_at, 'postgresql_cdc' as sync_source from dual"
  }
}

sink {
  Iceberg {
    plugin_input = "iceberg_customer_orders"
    catalog_name = "recipe_catalog"
    iceberg.catalog.config = {
      "type" = "hadoop"
      "warehouse" = ${warehouse}
    }
    namespace = "sales_analytics"
    table = "customer_orders"
    iceberg.table.primary-keys = "id"
    iceberg.table.upsert-mode-enabled = true
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

把配置保存为 `${SEATUNNEL_HOME}/config/postgresql-cdc-to-iceberg.conf`，将 `${slot_name}` 和 `${warehouse}` 替换为带引号的实际值，然后运行：

```bash
cd "${SEATUNNEL_HOME}"
./bin/seatunnel.sh --config ./config/postgresql-cdc-to-iceberg.conf -m local
```

例如：

```hocon
slot.name = "seatunnel_sales_orders"
"warehouse" = "file:///tmp/seatunnel/iceberg/postgres-cdc-recipe/"
```

## 已验证结果

增量 SQL 提交且下一次 Iceberg checkpoint 成功后，`sales_analytics.customer_orders` 中只包含：

| id | customer_name | amount | status_name | sync_source |
| --- | --- | ---: | --- | --- |
| 1001 | Alice Zhang | 150.75 | PAID | postgresql_cdc |
| 1003 | Carol Wang | 42.00 | PENDING | postgresql_cdc |

查询 Iceberg 表并核对上述主键集合和每个转换字段；被删除的 `1002` 不得继续存在。

## 运行检查

- 持续监控 `pg_replication_slots`，停止消费的 slot 可能无限保留 WAL。
- 只有对应 CDC 任务永久下线后才能删除 slot；活动任务之间禁止共用 slot。
- `iceberg.table.primary-keys` 必须对应稳定的源表主键，upsert 模式要求显式配置该参数。
- 确认 checkpoint 持续成功，Iceberg 变更会在成功提交后可见。
- 多 Worker 集群不要使用节点本地 warehouse，除非该路径实际由共享存储承载。

## 常见问题

- `wal_level` 不是 `logical`，或者修改配置后没有重启 PostgreSQL。
- CDC 账号缺少 `REPLICATION`、`CONNECT`、schema `USAGE` 或表 `SELECT` 权限。
- 配置的 replication slot 已被另一个任务使用。
- 默认安全检查开启时，源表没有设置 `REPLICA IDENTITY FULL`。
- Iceberg warehouse 不可写，或并非所有 SeaTunnel Worker 都可见。
- 开启 upsert 模式，却没有显式设置 `iceberg.table.primary-keys`。

## 相关文档

- [PostgreSQL CDC Source](../../connectors/source/PostgreSQL-CDC.md)
- [Iceberg Sink](../../connectors/sink/Iceberg.md)
- [Sql Transform](../../transforms/sql.md)
