---
title: MySQL 到 HDFS
---

# MySQL 到 HDFS

这条场景教程将 MySQL 订单数据批量写入 HDFS，生成按日期分区、使用 Snappy 压缩的 Parquet 文件。链路由 JDBC Source、SQL Transform 和 HdfsFile Sink 组成。

转换步骤将 `id` 重命名为 `order_id`，把订单状态转为大写，过滤负金额，并生成日期分区键 `pt_dt`。这是批量快照示例，不是 CDC 或增量同步任务。

## 前置条件

1. 先完成 [跑第一个任务](../locally/run-your-first-job.md)。本教程使用 Linux 上的 SeaTunnel Zeta 本地模式。将 `SEATUNNEL_HOME` 设置为 SeaTunnel 发行包的解压目录，不需要检出源码。
2. 安装与发行包版本一致的 `connector-jdbc` 和 `connector-file-hadoop`。参照 [部署文档](../locally/deployment.md)，在 `config/plugin_config` 中包含以下条目，并保留环境中其他任务需要的连接器。

```plugin_config
--seatunnel-connectors--
connector-jdbc
connector-file-hadoop
--end--
```

3. 将 MySQL JDBC 驱动（例如 `mysql-connector-j-8.x.jar`）放入 `${SEATUNNEL_HOME}/lib`。安装连接器，并检查连接器与驱动是否存在：

```bash
cd "${SEATUNNEL_HOME}"
sh bin/install-plugin.sh
ls connectors | grep -E 'connector-(jdbc|file-hadoop)'
ls lib | grep 'mysql-connector'
```

Zeta 发行包包含 Hadoop jar，应先检查 `lib` 再补充依赖，不要随意混用 Hadoop 客户端版本。环境相关的要求见 [HdfsFile Sink](../../connectors/sink/HdfsFile.md)。

4. 准备可访问的 MySQL 实例，以及对源表有 `SELECT` 权限的作业账号。下方初始化 SQL 需要使用具有建库、建表和插入权限的初始化账号；作业账号不需要这些初始化权限。
5. 准备可访问的 HDFS 集群和未使用的输出目录。SeaTunnel 进程需要对输出目录和 Sink 临时目录（默认 `/tmp/seatunnel`）均有写权限。本例假设 HDFS 未启用 Kerberos；Kerberos 或 HA 环境请按 HdfsFile 文档补充配置。用于验证的 Hadoop CLI 也需要能够访问该集群。

## 准备源数据

:::caution 使用独立测试数据库

在 MySQL 客户端中使用初始化账号执行一次下方 SQL。这里有意使用不带 `IF NOT EXISTS` 的 `CREATE DATABASE`，且不会删除或清空任何表。如果 `trade_db` 已存在，请停止执行，选择未使用的测试库名，并同步替换 SQL、JDBC URL、`table_path` 和查询中的库名。不要强制 SQL 客户端在出错后继续执行。

:::

```sql
CREATE DATABASE trade_db;
USE trade_db;

CREATE TABLE orders (
  id BIGINT NOT NULL PRIMARY KEY,
  order_no VARCHAR(64) NOT NULL,
  user_id BIGINT NOT NULL,
  amount DECIMAL(10, 2) NOT NULL,
  status VARCHAR(32) NOT NULL,
  create_time DATETIME NOT NULL
);

INSERT INTO orders (id, order_no, user_id, amount, status, create_time) VALUES
  (1, 'ORD-20260823-001', 10001, 99.50, 'completed', '2026-08-23 10:15:30'),
  (2, 'ORD-20260823-002', 10002, 199.00, 'pending', '2026-08-23 14:20:00'),
  (3, 'ORD-20260824-001', 10003, 49.90, 'COMPLETED', '2026-08-24 09:00:15'),
  (4, 'ORD-20260824-002', 10001, 350.00, 'paid', '2026-08-24 18:45:10'),
  (5, 'ORD-20260824-003', 10004, -10.00, 'cancelled', '2026-08-24 20:00:00');
```

这五条订单跨越两个日期，其中一条金额为负数，用于验证过滤效果。运行 SeaTunnel 前，请配置作业账号对该测试表的访问权限。

## 完整配置

将以下内容保存到 SeaTunnel 发行包内的 `config/mysql-to-hdfs.conf`。

将 JDBC 主机、库名、`username` 和 `password` 替换为测试环境的值，将 `fs.defaultFS` 和 `path` 替换为实际 HDFS 地址及未使用的测试输出路径。这里的 `localhost` 指运行 SeaTunnel 的机器，`namenode` 必须能在该机器上解析。示例凭据仅为占位符；在独立测试场景之外，应按环境要求配置 TLS。

```hocon
env {
  job.name = "mysql_to_hdfs_batch_dw"
  job.mode = "BATCH"
  parallelism = 4
}

source {
  Jdbc {
    plugin_output = "src_mysql_orders"
    url = "jdbc:mysql://localhost:3306/trade_db?useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "test_user"
    password = "test_password"

    table_path = "trade_db.orders"
    query = "select id, order_no, user_id, amount, status, create_time, date(create_time) as create_date from trade_db.orders"

    partition_column = "id"
    partition_num = 4
    partition_lower_bound = 1
    partition_upper_bound = 10000000
    fetch_size = 2000
  }
}

transform {
  Sql {
    plugin_input = "src_mysql_orders"
    plugin_output = "dwd_orders"
    query = """
      select
        id as order_id,
        order_no,
        user_id,
        amount,
        upper(status) as order_status,
        create_time,
        FORMATDATETIME(create_date, 'yyyy-MM-dd') as pt_dt
      from src_mysql_orders
      where amount >= 0
    """
  }
}

sink {
  HdfsFile {
    plugin_input = "dwd_orders"
    fs.defaultFS = "hdfs://namenode:8020"
    path = "/user/hive/warehouse/dwd.db/dwd_orders_df"
    file_format_type = "parquet"
    partition_by = ["pt_dt"]
    compress_codec = "snappy"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

JDBC 查询中的 `date(create_time)` 由 MySQL 执行，SQL Transform 再使用 SeaTunnel 的 `FORMATDATETIME(create_date, 'yyyy-MM-dd')` 生成分区键。各插件间的 `plugin_input` 和 `plugin_output` 必须保持一致。

分片边界和 `partition_num` 用于展示 JDBC 分片读取配置，不是针对五条记录的性能调优。用于大表时应根据实际源数据调整；少量数据不一定使用全部 Writer，也不保证生成大小相同的文件。

## 运行任务

第一次运行前，先在 MySQL 中记录预期行数：

```sql
SELECT DATE(create_time) AS pt_dt, COUNT(*) AS expected_rows
FROM trade_db.orders
WHERE amount >= 0
GROUP BY DATE(create_time)
ORDER BY pt_dt;
```

对于给定样例数据，每个日期应有两条记录，共四条。随后提交任务：

```bash
cd "${SEATUNNEL_HOME}"
./bin/seatunnel.sh --config ./config/mysql-to-hdfs.conf -m local
```

等待任务成功结束后，再检查输出。

:::caution 重复运行会追加数据

本任务使用 `APPEND_DATA`：对相同源数据和输出路径重复运行，可能追加重复记录。四条记录的预期仅适用于向未使用的输出目录成功运行一次。再次验证时请使用新的测试输出路径，不要删除或覆盖已有数仓数据。

:::

## 验证结果

### 检查分区目录

使用实际 HDFS 地址和输出路径执行：

```bash
hdfs dfs -ls -R hdfs://namenode:8020/user/hive/warehouse/dwd.db/dwd_orders_df
```

预期目录结构如下：

```text
/user/hive/warehouse/dwd.db/dwd_orders_df/
├── pt_dt=2026-08-23/
│   └── <generated-file>.parquet
└── pt_dt=2026-08-24/
    └── <generated-file>.parquet
```

以上目录树仅为示意。实际数据文件的名称和数量取决于 Writer 实例、并行度和数据分布，不能仅凭文件名判断压缩算法。

### 检查数据和文件格式

使用环境中已有的 Parquet 读取工具，读取两个分区目录内所有已提交的数据文件。对于给定样例数据和单次运行，应核对以下值（不保证行顺序）：

| order_id | order_status | amount | pt_dt |
| --- | --- | --- | --- |
| 1 | COMPLETED | 99.50 | 2026-08-23 |
| 2 | PENDING | 199.00 | 2026-08-23 |
| 3 | COMPLETED | 49.90 | 2026-08-24 |
| 4 | PAID | 350.00 | 2026-08-24 |

还应确认 `order_no`、`user_id` 和 `create_time` 保持不变。订单 `5` 因金额为负数应被过滤。通过 Parquet 元数据确认 Snappy 压缩，不要使用 `cat` 将二进制文件当作文本读取。

`pt_dt` 表示目录分区值。根据读取工具的行为，可能需要从目录推断该值，而不是从单个文件中读取同名列。Hive 风格目录不会自动创建或注册 Hive 表，仅有目录存在不足以证明数据验证通过。

## 常见问题

- SeaTunnel 进程使用的 `lib` 目录缺少 MySQL JDBC 驱动，或连接器版本与发行包不一致。
- `localhost` 指向错误的机器，或 SeaTunnel 无法访问 HDFS NameNode/DataNode 主机。
- 作业账号可以连接 MySQL，但没有读取 `trade_db.orders` 的权限；或者 HDFS 身份无法写入输出或临时目录。
- 使用已有数据库或输出目录运行教程，导致样例数据或预期行数不成立。
- 修改源查询时移除了 `create_date`，但 SQL Transform 仍然引用该字段。
- 将 Parquet 文件当作纯文本读取，或者假设输出文件名与文件数量固定不变。

## 相关文档

- [JDBC Source](../../connectors/source/Jdbc.md)
- [SQL Transform](../../transforms/sql.md)
- [SQL 函数](../../transforms/sql-functions.md)
- [HdfsFile Sink](../../connectors/sink/HdfsFile.md)
