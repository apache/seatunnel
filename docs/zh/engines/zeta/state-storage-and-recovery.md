---
sidebar_position: 8
---

# 状态存储与恢复

## 概述

SeaTunnel Engine（Zeta）在作业执行过程中会持久化多类状态数据。了解每类数据的存储内容、存储位置和管理方式，
是在生产环境中稳定运行 CDC 或长期流处理作业的关键。

| 存储类别 | 用途 | 默认位置 |
|---|---|---|
| Checkpoint（检查点）| 流水线算子状态的容错快照 | `seatunnel.yaml` `checkpoint.storage` 配置路径 |
| Savepoint（保存点）| 用户手动触发的命名检查点，用于计划性停止或重启 | 与 Checkpoint 同路径，目录名不同 |
| IMap / MapStore | 分布式内存状态（作业元数据、运行中作业数据、指标）| `hazelcast.yaml` MapStore base-dir |
| WAL（预写日志）| IMap 持久化的耐久性日志 | 与 MapStore base-dir 相同 |
| 历史作业元数据 | 已完成 / 失败作业记录 | MapStore base-dir |

---

## 1. Checkpoint 存储

### 存储内容

Checkpoint 在某一时间点捕获所有流水线算子状态的一致性快照。对于 CDC 作业，包括：

- Binlog / WAL 偏移量（MySQL binlog 位置、PostgreSQL LSN、Oracle SCN）
- 并行 Reader 的 Split 级进度
- 2PC 写入事务状态（Doris、StarRocks、Kafka 事务 ID）
- 正在经过 Transform 的 SeaTunnelRow 缓冲区

### 存储路径结构

```
<namespace>/                          # 配置的 namespace，默认 /seatunnel/checkpoint/
  <job-id>/
    <pipeline-id>/
      <checkpoint-id>/
        <task-location>/state-data
```

### 配置参考

```yaml
seatunnel:
  engine:
    checkpoint:
      interval: 10000              # 两次 Checkpoint 间隔（毫秒）
      timeout: 60000               # Checkpoint 完成超时（毫秒）
      max-concurrent: 1            # 最大并发 Checkpoint 数
      tolerable-failure: 2         # 允许连续失败次数
      storage:
        type: hdfs                 # hdfs | localfile（已废弃）
        plugin-config:
          namespace: /seatunnel/checkpoint/    # 必须以 / 结尾
          # S3 示例：
          # fs.s3a.endpoint: https://s3.amazonaws.com
          # fs.s3a.access.key: <your-access-key>
          # fs.s3a.secret.key: <your-secret-key>
```

### Checkpoint 保留与清理

- Checkpoint **不**受 `history-job-expire-minutes` 管理，必须手动清理或配置独立保留策略。
- 每条流水线只保留最新 N 个 Checkpoint 的 Hazelcast 内存引用；作业异常终止时，旧目录可能残留在磁盘上。
- **安全清理规则**：只有在作业已取消且确认不会从该 Checkpoint 恢复时，才可删除对应 job-id 目录。

---

## 2. Savepoint（保存点）

### Savepoint 与 Checkpoint 的区别

| 维度 | Checkpoint | Savepoint |
|---|---|---|
| 触发方式 | 周期性 / 自动 | 手动（`seatunnel.sh -r <jobId> --savepoint`）|
| 用途 | 容错恢复 | 计划性停止、升级、迁移 |
| 生命周期 | 引擎管理 | 操作人员管理 |
| 保留策略 | 自动轮转 | 手动删除前永久保留 |

### 触发 Savepoint

```bash
# 停止运行中的作业并创建 Savepoint
$SEATUNNEL_HOME/bin/seatunnel.sh --stop-job <job-id> --savepoint

# 或通过 REST API v2
curl -X POST http://<master>:8080/stop-job \
  -H 'Content-Type: application/json' \
  -d '{
    "jobId": <job-id>,
    "isStopWithSavePoint": true,
    "force": false
  }'
```

### 从 Savepoint 恢复

```bash
# 提交时携带 --restore 从最新 Savepoint 恢复
$SEATUNNEL_HOME/bin/seatunnel.sh --config job.conf --restore <savepoint-path>
```

### Savepoint 路径结构

```
<namespace>/
  savepoint/
    <job-id>/
      <savepoint-timestamp>/
        <pipeline-id>/
          <task-location>/state-data
```

### 安全清理

只有在确认永不从该 Savepoint 恢复作业时，才可删除。恢复过程中删除 Savepoint 会导致作业以"状态未找到"错误失败。

---

## 3. IMap 与 MapStore（Hazelcast 分布式状态）

### IMap 存储的内容

SeaTunnel Engine 使用 Hazelcast IMap 作为分布式内存键值存储。以下逻辑映射会被持久化：

| IMap 名称 | 内容 |
|---|---|
| `running-job-state` | 每个运行中作业的状态机当前状态 |
| `running-job-metrics` | 实时吞吐量、延迟和记录数指标 |
| `running-pipeline-state` | 每条逻辑流水线的 Pipeline 级状态 |
| `finished-job-state` | 已完成、已取消或失败作业的终态 |
| `finished-job-metrics` | 作业终止后的最终指标快照 |

### MapStore（磁盘持久化）

Hazelcast MapStore 将 IMap 条目写入本地磁盘，使其在进程重启后可以恢复。这与 Checkpoint 存储**相互独立**。

默认存储路径在 `hazelcast.yaml` 中配置：

```yaml
map:
  seatunnel:
    map-store:
      enabled: true
      initial-load-mode: EAGER
      properties:
        hazelcast.fs.base-dir: /tmp/seatunnel/imap   # 绝对路径
        hazelcast.fs.write-behind-delay-seconds: 1
```

MapStore 目录结构：

```
<hazelcast.fs.base-dir>/
  maps/
    running-job-state/
    running-job-metrics/
    finished-job-state/
    finished-job-metrics/
```

### IMap、MapStore 与 Checkpoint 的关系

```
Checkpoint 存储  <──────────────────────────────────>  算子状态（偏移量、Split）
IMap / MapStore  <──────────────────────────────────>  作业 / 流水线生命周期状态
```

二者**完全独立**。删除 Checkpoint 存储不影响 IMap；反之亦然。
作业可以从 Checkpoint 恢复，即使 IMap MapStore 已被清除——但作业 ID 和流水线映射需要**重新提交**，
因为 `running-job-state` 已丢失。

---

## 4. WAL（预写日志）

Hazelcast 使用 WAL 保证 MapStore 的耐久性。WAL 文件累积在：

```
<hazelcast.fs.base-dir>/
  wal/
    <imap-name>-<partition>.wal
```

### 长期运行 CDC 作业的 WAL 增长

每次 CDC binlog 事件更新 `running-job-metrics` 或 `running-pipeline-state` 时，都会产生一次 WAL 写入。
随着时间推移（数天或数周），如果出现以下情况，WAL 文件可能增长至数 GB：

- `write-behind-delay-seconds` 设置过低（刷新频率过高）
- 作业每秒处理数百万事件

**缓解措施：**

```yaml
hazelcast.fs.write-behind-delay-seconds: 5   # 增大刷新间隔
hazelcast.fs.compaction-threshold: 1000       # N 条写入后触发压缩
```

**已完成作业**的 WAL 文件在对应 IMap 条目已刷入 MapStore 文件后可以安全压缩或删除。
**不要**删除运行中作业的 WAL 文件。

---

## 5. 历史作业过期

### `history-job-expire-minutes` 的作用范围

```yaml
seatunnel:
  engine:
    history-job-expire-minutes: 1440   # 24 小时
```

| 操作 | 是否受过期影响 |
|---|---|
| 从 `finished-job-state` IMap 中移除 | 是 |
| 从 `finished-job-metrics` IMap 中移除 | 是 |
| 删除该作业的 MapStore 持久化文件 | 是（IMap 驱逐后） |
| 删除 Checkpoint 存储目录 | **否** |
| 删除 Savepoint 目录 | **否** |
| 删除 WAL 文件 | **否**（仅通过压缩间接处理）|

**核心结论**：`history-job-expire-minutes` 仅清理 `finished-job-state` 中的作业元数据。
HDFS / S3 / 本地的 Checkpoint 和 Savepoint 目录**不受此配置影响**，必须独立管理。

---

## 6. 长期运行 CDC 作业的容量规划

### Checkpoint 大小估算

| 来源 | 单次 Checkpoint 典型大小 |
|---|---|
| MySQL CDC（1 张表，低流量）| 1–10 KB（binlog 偏移量 + Split 状态）|
| MySQL CDC（多表，100 个 Split）| 100 KB – 1 MB |
| MySQL CDC（全量快照阶段）| 10–500 MB（快照 Split 状态）|
| PostgreSQL CDC（逻辑复制）| 每张表 1–50 KB |
| Oracle CDC（LogMiner）| 50 KB – 2 MB |

公式：`checkpoint_size ≈ 表数量 × 平均 Split 数 × 每 Split 状态大小 × 最大并发 Checkpoint 数`

### 存储容量经验值

每个作业至少保留 **3 个 Checkpoint**，建议分配：

```
所需存储 = checkpoint_size × 3 × 安全系数(1.5)
```

### IMap / WAL 容量估算

每个运行中的 CDC 作业大约占用：

- `running-job-state`：每条流水线约 50–200 字节
- `running-job-metrics`：每次指标刷新每条流水线约 1–2 KB

对于运行 100 个 CDC 作业的集群：

```
imap 内存 ≈ 100 × 200B ≈ 20 KB（状态）
WAL 磁盘：建议每节点规划 2–5 GB
```

使用以下命令持续监控：

```bash
du -sh $SEATUNNEL_HOME/imap/wal/
watch -n 60 'du -sh /tmp/seatunnel/imap/'
```

---

## 7. 故障排查

### Checkpoint 目录持续增长

**现象**：HDFS / S3 用量持续增加，即使作业已完成。

**诊断**：

```bash
# 列出各作业 ID 的 Checkpoint 目录
hadoop fs -ls /seatunnel/checkpoint/
# 或本地存储
ls -lh /tmp/seatunnel/checkpoint/
```

**根因**：SeaTunnel 没有内置 Checkpoint 目录 TTL。每次新 Checkpoint 都会新增目录；仅当作业正常完成且
状态被轮转出时，旧目录才会被清除。

**修复**：

1. 通过 REST API v2 查询已完成作业元数据：
   ```bash
   curl "http://<master>:8080/finished-jobs/FINISHED?page=1&rows=100"
   curl "http://<master>:8080/finished-jobs/CANCELED?page=1&rows=100"
   curl "http://<master>:8080/finished-jobs/FAILED?page=1&rows=100"
   curl "http://<master>:8080/finished-jobs/SAVEPOINT_DONE?page=1&rows=100"
   curl "http://<master>:8080/finished-jobs/UNKNOWABLE?page=1&rows=100"
   ```
2. 响应中不包含的 job-id，其 Checkpoint 目录为孤立目录，可安全删除。

---

### IMap / WAL 目录增长

**现象**：`/tmp/seatunnel/imap/` 占满 Master / Worker 节点磁盘。

**诊断**：

```bash
du -sh /tmp/seatunnel/imap/wal/
du -sh /tmp/seatunnel/imap/maps/
```

**可能根因**：

- `running-job-metrics` 在每次 Checkpoint 时为每个运行中作业更新
- `write-behind-delay-seconds` 过低（默认 1 秒）
- WAL 压缩触发频率不足

**修复**：

```yaml
# hazelcast.yaml
hazelcast.fs.write-behind-delay-seconds: 10
hazelcast.fs.compaction-threshold: 500
```

重启引擎后，已完成作业的旧 WAL 文件将在启动时被压缩。

---

### 重启时出现"状态未找到"错误

**现象**：作业重启后立即失败，提示 `checkpoint state not found` 或 `restore pipeline state failed`。

**原因**：Checkpoint 目录已被删除，或存储路径已变更。

**修复**：

1. 确认 `seatunnel.yaml` 中的 Checkpoint 路径与实际存储位置一致。
2. 如果 Checkpoint 已不存在，以全新方式提交作业（不带 `--restore`）。对于 CDC 作业，需决定：
   - `startup.mode=initial`：重新全量快照
   - `startup.mode=latest`：跳过已错过的数据

---

### 安全清理检查清单

删除任何状态目录前，请确认：

- [ ] 作业已处于 `FINISHED`、`CANCELLED` 或 `FAILED` 终态
- [ ] 确认不会从该 Checkpoint 或 Savepoint 恢复
- [ ] 确认该作业 ID 未被任何监控或告警规则引用
- [ ] Checkpoint 存储：递归删除 `<namespace>/<job-id>/`
- [ ] Savepoint：递归删除 `<namespace>/savepoint/<job-id>/`
- [ ] MapStore / WAL：手动删除后重启引擎以触发重载

---

## 参考

- [Checkpoint 存储配置](checkpoint-storage.md)
- [REST API v2](rest-api-v2.md) — 通过 API 查询作业状态和指标
