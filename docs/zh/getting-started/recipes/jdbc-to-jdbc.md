---
sidebar_position: 3
title: JDBC 到 JDBC
---

# JDBC 到 JDBC

当你需要在关系型数据库之间进行批量迁移，并希望在写入前过滤或调整数据时，可以使用本教程。示例从 MySQL 读取订单，只保留已支付订单，统一客户姓名格式，调整金额精度，填充来源系统字段，最后写入 PostgreSQL。

## 前置条件

1. 先完成[运行第一个本地任务](../locally/run-your-first-job.md)。

2. 安装 JDBC connector。按照[部署 > 下载连接器插件](../locally/deployment.md#下载连接器插件)操作，并在 `config/plugin_config` 中保留以下插件：

```plugin_config
--seatunnel-connectors--
connector-jdbc
--end--
```

```bash
cd "${SEATUNNEL_HOME}"
sh bin/install-plugin.sh
ls connectors | rg 'connector-jdbc'
```

`Sql` transform 已包含在 SeaTunnel 发布包中，不需要再添加 connector 配置。

3. 把两个数据库的驱动都放入 `${SEATUNNEL_HOME}/lib`，然后确认 SeaTunnel 可以看到它们：

```bash
ls "${SEATUNNEL_HOME}/lib" | rg 'mysql-connector|postgresql'
```

4. 创建 MySQL 源表，并插入三条固定数据：

```sql
CREATE DATABASE IF NOT EXISTS source_db;

CREATE TABLE IF NOT EXISTS source_db.orders (
  id BIGINT PRIMARY KEY,
  customer_name VARCHAR(100) NOT NULL,
  amount DECIMAL(16, 4) NOT NULL,
  status VARCHAR(20) NOT NULL
);

TRUNCATE TABLE source_db.orders;

INSERT INTO source_db.orders (id, customer_name, amount, status) VALUES
  (1001, 'alice chen', 120.5000, 'PAID'),
  (1002, 'bob li', 80.0000, 'CREATED'),
  (1003, 'carol wu', 42.0000, 'PAID');
```

5. 创建 PostgreSQL 目标库和目标表。Sink 用户需要具有这张表的 `INSERT` 权限。

```sql
CREATE USER test WITH PASSWORD 'test';
CREATE DATABASE target_db OWNER test;
```

使用 `test` 用户重新连接 `target_db`，然后执行：

```sql
CREATE TABLE IF NOT EXISTS public.paid_orders (
  id BIGINT PRIMARY KEY,
  customer_name VARCHAR(100) NOT NULL,
  amount DECIMAL(12, 2) NOT NULL,
  source_system VARCHAR(20) NOT NULL
);

TRUNCATE TABLE public.paid_orders;
```

如果用户或数据库已经存在，请直接复用，不要重复执行对应的 `CREATE` 语句。

## 完整配置

三个阶段通过 `plugin_output` 和 `plugin_input` 串联。转换后的字段顺序还必须与 sink `query` 中的四个占位符顺序一致；修改示例时需要同步调整这两处顺序。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    plugin_output = "mysql_orders"
    driver = "com.mysql.cj.jdbc.Driver"
    url = "jdbc:mysql://mysql.example.com:3306/source_db?useSSL=false&allowPublicKeyRetrieval=true"
    username = "root"
    password = "password"
    query = "SELECT id, customer_name, amount, status FROM orders"
  }
}

transform {
  Sql {
    plugin_input = "mysql_orders"
    plugin_output = "paid_orders"
    query = """
      SELECT
        id,
        UPPER(customer_name) AS customer_name,
        CAST(amount AS DECIMAL(12, 2)) AS amount,
        'MYSQL' AS source_system
      FROM dual
      WHERE status = 'PAID'
    """
  }
}

sink {
  Jdbc {
    plugin_input = "paid_orders"
    driver = "org.postgresql.Driver"
    url = "jdbc:postgresql://postgresql.example.com:5432/target_db"
    username = "test"
    password = "test"
    query = "INSERT INTO public.paid_orders (id, customer_name, amount, source_system) VALUES (?, ?, ?, ?)"
  }
}
```

`dual` 是默认 SeaTunnel SQL transform 引擎使用的虚拟输入表，不是 MySQL 或 PostgreSQL 中的真实表。

## 运行任务

把配置保存为 `config/jdbc-to-jdbc.conf`，按实际环境替换主机名和凭据，然后使用 local 模式运行 SeaTunnel：

```bash
cd "${SEATUNNEL_HOME}"
./bin/seatunnel.sh --config ./config/jdbc-to-jdbc.conf -m local
```

## 验证结果

任务结束后查询 PostgreSQL 目标表：

```sql
SELECT id, customer_name, amount, source_system
FROM public.paid_orders
ORDER BY id;
```

预期结果：

| id | customer_name | amount | source_system |
| --- | --- | ---: | --- |
| 1001 | ALICE CHEN | 120.50 | MYSQL |
| 1003 | CAROL WU | 42.00 | MYSQL |

这组结果分别验证了每一步转换：订单 `1002` 因未支付而被过滤，姓名已转为大写，金额保留两位小数，并新增了固定值为 `MYSQL` 的 `source_system` 字段。

## 常见问题

- `${SEATUNNEL_HOME}/lib` 中缺少 MySQL 或 PostgreSQL 驱动，或者驱动版本与数据库不兼容。
- 原样复制示例主机名，却没有替换为 SeaTunnel 进程能够解析的实际地址。
- 示例 MySQL URL 为简化开发环境而关闭了 TLS，并允许公钥检索。用于生产环境前，应按照所在组织的安全要求配置 TLS。
- 调整了 `SELECT` 字段顺序，却没有同步调整 sink `INSERT` 语句中的列顺序。
- 目标表中已有数据，第二次运行时与主键冲突。教程重复执行前可以清空目标表；生产环境应根据业务选择 upsert 策略。
- 源端 `DECIMAL` 数据超出 `DECIMAL(12, 2)` 的精度或小数位范围。正式迁移前需要按真实数据选择合适的目标类型。
- 批处理任务运行期间源表仍在变化。如果需要持续捕获变更，应改用 CDC source。

## 相关文档

- [JDBC source](../../connectors/source/Jdbc.md)
- [SQL transform](../../transforms/sql.md)
- [JDBC sink](../../connectors/sink/Jdbc.md)
- [多表 CDC](./multi-table-cdc.md)
