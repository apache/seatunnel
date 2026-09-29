---
sidebar_position: 1
---

# CDC 生产实战手册：全量 + 增量同步

## 概述

本手册涵盖 SeaTunnel CDC 连接器的生产级配置与运维，是各连接器参数参考文档的补充，提供最常见目标系统的端到端示例和故障排查检查清单。

**本手册覆盖的 CDC 数据源：**

| 数据源 | 连接器 |
|---|---|
| MySQL | [MySQL-CDC](source/MySQL-CDC.md) |
| PostgreSQL | [PostgreSQL-CDC](source/PostgreSQL-CDC.md) |
| Oracle | [Oracle-CDC](source/Oracle-CDC.md) |

---

## 1. 全量 + 增量同步生命周期

CDC 作业分两个阶段顺序执行：

```
阶段一：全量快照（批量读取）
  ──> 以并行 Split 方式读取每张表的现有数据行
  ──> 将所有行写入目标（快照阶段无需 2PC）
  ──> 记录快照开始时的 binlog/WAL/SCN 位置

阶段二：增量 CDC（流式）
  ──> 从快照起始位置持续消费 binlog/WAL/logminer
  ──> 将 INSERT/UPDATE/DELETE 事件转换为带 RowKind 的 SeaTunnelRow
  ──> 通过 2PC 写入目标（若目标支持 exactly-once）
```

此生命周期由 `startup.mode` 控制：

| `startup.mode` | 行为 |
|---|---|
| `initial` | 先执行全量快照，再切换到增量（**生产推荐**）|
| `earliest` | 跳过快照，从最早可用的 binlog/WAL 位置开始 |
| `latest` | 跳过快照，从当前最新的 binlog/WAL 位置开始 |
| `specific` | 从用户指定的 binlog 文件+偏移量或 LSN/SCN 开始 |
| `timestamp` | 从指定时间戳对应的 binlog/WAL 位置开始 |

**生产建议**：始终使用 `startup.mode=initial`，除非是在恢复失败的增量作业。使用 `latest` 会静默跳过现有数据。

---

## 2. 各数据库前置条件

### MySQL CDC

```sql
-- 1. 开启 binlog
-- my.cnf / my.ini：
-- binlog_format = ROW
-- binlog_row_image = FULL

-- 2. 创建 CDC 用户并授予复制权限
CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'password';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';

-- 3. （可选但推荐）开启 GTID 实现自动故障转移
-- gtid_mode = ON
-- enforce_gtid_consistency = ON
```

**`server-id` 的必要性**：MySQL 将每个复制客户端视为唯一的 Server。作业配置中的 `server-id` 不得与
任何其他 MySQL 副本或连接同一 MySQL 实例的 SeaTunnel CDC 作业冲突。
建议为每个作业分配独立区间（如 5400–5499）。

### PostgreSQL CDC

```sql
-- 1. 设置 WAL 级别（需重启 PG）
-- postgresql.conf：wal_level = logical

-- 2. 创建复制槽
SELECT pg_create_logical_replication_slot('seatunnel_slot', 'pgoutput');

-- 3. 授予复制权限
ALTER ROLE cdc_user REPLICATION LOGIN;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;

-- 4. 创建 Publication（pgoutput 解码器）
CREATE PUBLICATION seatunnel_pub FOR ALL TABLES;
```

### Oracle CDC

```sql
-- 1. 开启补充日志
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- 2. 启用 LogMiner（一次性）
EXECUTE DBMS_LOGMNR_D.BUILD(OPTIONS => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS);

-- 3. 创建 LogMiner 用户
CREATE USER logminer_user IDENTIFIED BY password;
GRANT CREATE SESSION, LOGMINING, SELECT ANY TRANSACTION TO logminer_user;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO logminer_user;
```

---

## 3. 生产示例

### 3.1 MySQL CDC → Apache Doris

```hocon
env {
  parallelism = 4
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  MySQL-CDC {
    base-url = "jdbc:mysql://mysql-host:3306/orders_db"
    username = "cdc_user"
    password = "password"
    database-names = ["orders_db"]
    table-names = ["orders_db.orders", "orders_db.order_items"]
    startup.mode = "initial"
    server-id = "5400-5404"
    exactly-once = true
  }
}

sink {
  Doris {
    fenodes = "doris-fe:8030"
    username = "root"
    password = ""
    database = "orders_dw"
    table = "${table_name}"
    sink.enable-2pc = true
    sink.label-prefix = "seatunnel_cdc_orders"
    doris.config {
      format = "json"
      read_json_by_line = "true"
    }
  }
}
```

**关键说明：**
- `sink.enable-2pc = true` 与 SeaTunnel 的 exactly-once checkpoint 协同
- `checkpoint.interval` 同时决定 CDC 提交频率和 2PC 提交间隔
- 较短的间隔降低延迟，但会增加 Doris label 元数据开销

---

### 3.2 MySQL CDC → StarRocks

```hocon
env {
  parallelism = 4
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  MySQL-CDC {
    base-url = "jdbc:mysql://mysql-host:3306/inventory"
    username = "cdc_user"
    password = "password"
    database-names = ["inventory"]
    table-names = ["inventory.products", "inventory.customers"]
    startup.mode = "initial"
    server-id = "5410-5414"
    exactly-once = true
  }
}

sink {
  StarRocks {
    nodeUrls = ["starrocks-fe:8030"]
    username = "root"
    password = ""
    database = "inventory_dw"
    table = "${table_name}"
    sink.properties.format = "json"
    sink.properties.strip_outer_array = "true"
    enable_upsert_delete = true
  }
}
```

---

### 3.3 MySQL CDC → Kafka

```hocon
env {
  parallelism = 4
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  MySQL-CDC {
    base-url = "jdbc:mysql://mysql-host:3306/app_db"
    username = "cdc_user"
    password = "password"
    database-names = ["app_db"]
    table-names = ["app_db.users", "app_db.events"]
    startup.mode = "initial"
    server-id = "5420-5424"
    format = "COMPATIBLE_DEBEZIUM_JSON"
  }
}

sink {
  Kafka {
    topic = "cdc.${database_name}.${table_name}"
    bootstrap.servers = "kafka:9092"
    kafka.config {
      acks = "all"
      transactional.id = "seatunnel-cdc-${table_name}"
    }
  }
}
```

**Debezium 兼容格式**：设置 `format = COMPATIBLE_DEBEZIUM_JSON` 可生成下游（Flink、Kafka Connect 等）
可直接使用标准 Debezium Schema 处理的 Kafka 消息。

---

### 3.4 PostgreSQL CDC → JDBC / Doris / StarRocks

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 20000
}

source {
  Postgres-CDC {
    base-url = "jdbc:postgresql://pg-host:5432/mydb"
    username = "cdc_user"
    password = "password"
    database-names = ["mydb"]
    schema-names = ["public"]
    table-names = ["public.orders", "public.customers"]
    startup.mode = "initial"
    slot.name = "seatunnel_slot"
    decoding.plugin.name = "pgoutput"
  }
}

sink {
  Doris {
    fenodes = "doris-fe:8030"
    username = "root"
    password = ""
    database = "mydb_ods"
    table = "${table_name}"
    sink.enable-2pc = true
    sink.label-prefix = "seatunnel_pg_cdc"
    doris.config {
      format = "json"
      read_json_by_line = "true"
    }
  }
}
```

---

### 3.5 Oracle CDC → Doris / StarRocks / JDBC

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 60000
}

source {
  Oracle-CDC {
    base-url = "jdbc:oracle:thin:@oracle-host:1521:ORCL"
    username = "logminer_user"
    password = "password"
    database-names = ["ORCL"]
    schema-names = ["HR"]
    table-names = ["HR.EMPLOYEES", "HR.DEPARTMENTS"]
    startup.mode = "initial"
  }
}

sink {
  Doris {
    fenodes = "doris-fe:8030"
    username = "root"
    password = ""
    database = "hr_ods"
    table = "${table_name}"
    sink.enable-2pc = true
    sink.label-prefix = "seatunnel_oracle_cdc"
    doris.config {
      format = "json"
      read_json_by_line = "true"
    }
  }
}
```

---

## 4. Checkpoint 与 2PC 的协同机制

`checkpoint.interval` 同时控制容错频率和 Doris/StarRocks 等 Sink 的 2PC 提交节拍：

```
Checkpoint N 触发：
  1. Source：将当前 binlog 偏移量刷入 Checkpoint 存储
  2. Sink：提交所有待处理的微批事务（2PC 第二阶段）
  3. Sink：开启下一批次的新事务（2PC 第一阶段）

故障恢复流程：
  1. 引擎从最后一次成功 Checkpoint 恢复 binlog 偏移量
  2. Sink 回滚所有未提交的进行中事务
  3. CDC 从 Checkpoint 偏移量继续回放——无数据丢失，无重复
```

**`checkpoint.interval` 选择建议：**

| 场景 | 推荐间隔 |
|---|---|
| 低延迟 CDC（需要 < 5 秒数据新鲜度）| 5 000–10 000 ms |
| 标准生产 CDC | 30 000–60 000 ms |
| Oracle LogMiner（查询开销较大）| 60 000–120 000 ms |
| 高吞吐批量 CDC | 60 000–300 000 ms |

---

## 5. Schema 变更与 DDL 支持边界

| DDL 操作 | MySQL CDC | PostgreSQL CDC | Oracle CDC |
|---|---|---|---|
| ADD COLUMN（加列）| 支持（2.3.x 起）| 支持 | 有限支持 |
| DROP COLUMN（删列）| 默认不传播 | 默认不传播 | 默认不传播 |
| RENAME COLUMN（列重命名）| 不支持 | 不支持 | 不支持 |
| ALTER COLUMN TYPE（修改列类型）| 有风险，可能引起反序列化错误 | 有风险 | 有风险 |
| TRUNCATE TABLE | 不捕获 | 不捕获 | 不捕获 |
| DROP TABLE | 不捕获 | 不捕获 | 不捕获 |

**生产建议**：避免在 CDC 正在同步的表上直接执行 `ALTER TABLE`，需制定协调好的 Schema 迁移方案。
对于复杂 DDL 变更，推荐以下流程：

1. 以 Savepoint 方式优雅停止 CDC 作业
2. 在源端和目标端同步执行 DDL
3. 若 Schema 映射发生变化，更新作业配置
4. 从 Savepoint 恢复作业

---

## 6. 观测 CDC 延迟

### 通过 REST API 查询运行作业指标

```bash
# 查询 CDC 当前延迟（落后记录数）
curl http://<master>:8080/hazelcast/rest/maps/running-job-metrics/<job-id>
```

### 关键监控指标

| 指标 | 含义 |
|---|---|
| `SourceReceivedCount` | 从数据源读取的总记录数（快照 + 增量）|
| `SinkWriteCount` | 写入目标的总记录数 |
| `lag` | MySQL binlog 偏移量落后量 |

### 延迟监控脚本

```bash
#!/bin/bash
JOB_ID=$1
MASTER=$2
while true; do
  echo "$(date) --- CDC 延迟："
  curl -s http://$MASTER:8080/hazelcast/rest/maps/running-job-metrics/$JOB_ID | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print(d)"
  sleep 30
done
```

---

## 7. 故障排查检查清单

### 权限类错误

| 错误 | 修复方法 |
|---|---|
| `Access denied for user ... REPLICATION` | 授予 CDC 用户 `REPLICATION SLAVE` 和 `REPLICATION CLIENT` 权限 |
| `pg_hba.conf` 拒绝复制连接 | 在 pg_hba.conf 中添加 `host replication cdc_user 0.0.0.0/0 md5` |
| Oracle `ORA-01031: insufficient privileges` | 授予 LogMiner 用户 `LOGMINING`、`SELECT ANY TRANSACTION` 权限 |

### 网络类问题

| 错误 | 修复方法 |
|---|---|
| binlog 端口 `Connection refused` | 确认 MySQL `bind-address` 不是 `127.0.0.1`；检查防火墙规则 |
| PostgreSQL 复制槽未创建 | 确认 `wal_level = logical` 并重启 PG；创建复制槽需要 superuser 或 replication 角色 |

### 复制位置 / 偏移量问题

| 错误 | 修复方法 |
|---|---|
| `Could not find first log file name in binary log` | MySQL binlog 轮转删除了起始位置；以 `startup.mode=initial` 重新提交作业 |
| `Replication slot ... does not exist` | 复制槽被删除；重新创建后以 `startup.mode=initial` 重新提交作业 |
| `SCN not found in redo log` | Oracle redo log 已回收起始 SCN；以 `startup.mode=initial` 重新提交作业 |

### Checkpoint 故障

| 错误 | 修复方法 |
|---|---|
| `Checkpoint timeout exceeded` | 增大 `checkpoint.timeout`；降低 `parallelism` 以减少 Checkpoint 开销 |
| `Checkpoint storage path not accessible` | 确认所有 Worker 节点可访问 HDFS / S3；检查存储凭据 |
| 恢复时提示 `state not found` | Checkpoint 已被删除或路径已变更；以全新方式提交作业 |

### 2PC Sink 故障

| 错误 | 修复方法 |
|---|---|
| Doris `Label already exists` | 未通过 Savepoint 重启导致 label 重复；更改 `sink.label-prefix` |
| StarRocks 事务超时 | 增大 `sink.properties.timeout`；检查 StarRocks FE 连接池配置 |

---

## 参考

- [MySQL CDC 连接器参考](source/MySQL-CDC.md)
- [PostgreSQL CDC 连接器参考](source/PostgreSQL-CDC.md)
- [Oracle CDC 连接器参考](source/Oracle-CDC.md)
- [Zeta 状态存储与恢复](../engines/zeta/state-storage-and-recovery.md)
- [REST API 作业生命周期手册](../engines/zeta/rest-api-job-lifecycle.md)
