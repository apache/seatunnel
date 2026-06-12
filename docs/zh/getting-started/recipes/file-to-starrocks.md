---
sidebar_position: 5
title: File 到 StarRocks
---

# File 到 StarRocks

当你想把本地 CSV 或文本文件导入 StarRocks，供后续高性能分析查询使用时，可以使用这条链路。

## 前置条件

1. 先完成 [跑第一个任务](../locally/run-your-first-job.md)。

2. 安装这条链路需要的插件。先看 [部署 > 下载连接器插件](../locally/deployment.md#下载连接器插件)，然后把 `config/plugin_config` 改成下面这样：

```plugin_config
--seatunnel-connectors--
connector-file-local
connector-starrocks
--end--
```

```bash
cd "${SEATUNNEL_HOME}"
sh bin/install-plugin.sh
ls connectors | rg 'connector-(file-local|starrocks)'
```

3. 把 StarRocks sink 依赖的 MySQL JDBC 驱动放进 `${SEATUNNEL_HOME}/lib`，并确认 jar 已经落盘：

```bash
ls "${SEATUNNEL_HOME}/lib" | rg 'mysql-connector'
```

4. 先准备本地输入文件，并确保 SeaTunnel 进程能读到它：

```bash
mkdir -p /tmp/seatunnel/input
cat <<'EOF' > /tmp/seatunnel/input/customers.csv
id,name,city,updated_at
1001,Alice,Shanghai,2026-06-12 10:00:00
1002,Bob,Beijing,2026-06-12 10:05:00
1003,Carol,Hangzhou,2026-06-12 10:10:00
EOF
```

5. 运行任务前，先在 StarRocks 中创建好目标库和目标表。

## 最小配置

下面的示例读取一个带表头的本地 CSV 文件，并把数据写入已经存在的 StarRocks 主键表。

先创建目标表：

```sql
CREATE DATABASE IF NOT EXISTS sync_demo;

CREATE TABLE IF NOT EXISTS sync_demo.customers (
  id BIGINT NOT NULL,
  name STRING,
  city STRING,
  updated_at DATETIME
)
ENGINE=OLAP
PRIMARY KEY(id)
DISTRIBUTED BY HASH(id)
PROPERTIES (
  "replication_num" = "1"
);
```

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  LocalFile {
    plugin_output = "customers_file"
    path = "/tmp/seatunnel/input/customers.csv"
    file_format_type = "csv"
    csv_use_header_line = true
    schema = {
      fields {
        id = bigint
        name = string
        city = string
        updated_at = timestamp
      }
    }
  }
}

sink {
  StarRocks {
    plugin_input = "customers_file"
    nodeUrls = ["starrocks-fe:8030"]
    base-url = "jdbc:mysql://starrocks-fe:9030/sync_demo"
    username = "root"
    password = ""
    database = "sync_demo"
    table = "customers"
    batch_max_rows = 1000
    schema_save_mode = "IGNORE"
    starrocks.config = {
      format = "JSON"
      strip_outer_array = true
    }
  }
}
```

## 运行任务

把配置保存为 `config/file-to-starrocks.conf`，然后用本地模式运行 SeaTunnel：

```bash
cd "${SEATUNNEL_HOME}"
./bin/seatunnel.sh --config ./config/file-to-starrocks.conf -m local
```

## 验证结果

1. 运行任务，确认没有 StarRocks stream load 错误。
2. 在 StarRocks 中查询目标表。

```sql
SELECT COUNT(*) FROM sync_demo.customers;
SELECT id, name, city, updated_at FROM sync_demo.customers ORDER BY id;
```

如果 StarRocks 中的导入结果和文件内容一致，这条链路就是通的。

## 常见坑

- 配了 `nodeUrls`，但漏了 `base-url`。
- 文件带表头，但没有设置 `csv_use_header_line = true`。
- 源文件 schema、分隔符、时间格式和实际文件内容不一致。
- 运行前没有先创建目标表。本教程使用 `schema_save_mode = "IGNORE"`，因为本地文件源不会提供 StarRocks 自动建表需要的主键元数据。

## 相关文档

- [LocalFile Source](../../connectors/source/LocalFile.md)
- [StarRocks Sink](../../connectors/sink/StarRocks.md)
