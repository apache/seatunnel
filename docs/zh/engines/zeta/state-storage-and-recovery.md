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
| IMap / MapStore | 分布式内存状态（作业元数据、运行中作业数据、指标）| `hazelcast.yaml` `map` MapStore 配置（可选，默认不持久化）|
| WAL（预写日志）| 文件型 IMap 存储插件的追加写日志文件 | `<namespace>/<clusterName>/<businessName>/`（IMap 存储）|
| 历史作业元数据 | 已完成 / 失败作业记录 | 与 IMap 存储位置相同 |

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
      interval: 10000              # 两次 Checkpoint 间隔（毫秒，默认 300000）
      timeout: 60000               # Checkpoint 完成超时（毫秒，默认 30000）
      min-pause: -1                # 两次 Checkpoint 之间的最小间隔（毫秒），-1 表示不启用
      monitor-history-size: 32     # 每条流水线保留的 Checkpoint 历史条数（默认 32）
      retain-after-job-cancelled: false   # 作业取消后是否保留 Checkpoint 数据
      storage:
        type: hdfs                 # Checkpoint 存储插件：hdfs | localfile（默认 localfile，
                                   # 已废弃，建议使用 hdfs 插件）
        max-retained: 20           # 保留的最大 Checkpoint 数（默认 20）
        plugin-config:
          namespace: /seatunnel/checkpoint/    # 必须以 / 结尾
          storage.type: hdfs       # hdfs 插件的后端文件系统：
                                   # hdfs（默认）| local | s3 | oss | cos | gcs
          fs.defaultFS: hdfs://localhost:9000  # storage.type 为 hdfs 时必填
          # S3 示例（storage.type: s3）：
          # fs.s3a.endpoint: https://s3.amazonaws.com
          # fs.s3a.access.key: <your-access-key>
          # fs.s3a.secret.key: <your-secret-key>
          # 环境变量凭据：
          # fs.s3a.aws.credentials.provider: com.amazonaws.auth.EnvironmentVariableCredentialsProvider
```

### Checkpoint 保留与清理

- Checkpoint **不**受 `history-job-expire-minutes` 管理，必须手动清理或使用下方的保留策略。
- 只保留最新 `checkpoint.storage.max-retained` 个 Checkpoint（默认 20），更早的 Checkpoint 数据会被删除；作业异常终止时，旧目录可能残留在磁盘上。
- 默认情况下，被取消的作业仍会清理 Checkpoint 数据。如果希望取消后的作业还能从最新成功完成的 Checkpoint 恢复，需要在历史作业取消前将
  作业 `env` 中的 `checkpoint.retain-after-job-cancelled` 或 `seatunnel.yaml` 中的
  `seatunnel.engine.checkpoint.retain-after-job-cancelled` 设置为 `true`。
- **安全清理规则**：只有在作业已取消且确认不会从该 Checkpoint 恢复时，才可删除对应 job-id 目录。

---

## 2. Savepoint（保存点）

### Savepoint 与 Checkpoint 的区别

| 维度 | Checkpoint | Savepoint |
|---|---|---|
| 触发方式 | 周期性 / 自动 | 手动（`seatunnel.sh -s <jobId>`）|
| 用途 | 容错恢复 | 计划性停止、升级、迁移 |
| 生命周期 | 引擎管理 | 操作人员管理 |
| 保留策略 | 自动轮转 | 手动删除前永久保留 |

### 触发 Savepoint

```bash
# 为运行中的作业触发 Savepoint（Savepoint 完成后作业停止）
$SEATUNNEL_HOME/bin/seatunnel.sh -s <job-id>
# 长参数形式：--savepoint <job-id> 或 --savepoint-job <job-id>

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
$SEATUNNEL_HOME/bin/seatunnel.sh --config job.conf --restore <job-id>
```

### 从最新成功完成的 Checkpoint 恢复

```bash
# 提交时携带 --restore-with-checkpoint，从历史作业的最新成功完成 Checkpoint 恢复。
# 新一次运行会生成新的运行时 jobId。
$SEATUNNEL_HOME/bin/seatunnel.sh --config job.conf --restore-with-checkpoint <job-id>
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

SeaTunnel Engine 使用 Hazelcast IMap 作为分布式内存键值存储。主要的逻辑映射包括：

| IMap 名称 | 内容 |
|---|---|
| `engine_runningJobInfo` | 运行中作业的元数据（作业 ID、作业状态等）|
| `engine_runningJobState` | 每个运行中作业及其流水线的状态机当前状态 |
| `engine_runningJobMetrics` | 实时吞吐量、延迟和记录数指标 |
| `engine_finishedJobState` | 已完成、已取消或失败作业的终态 |
| `engine_finishedJobMetrics` | 作业终止后的最终指标快照 |
| `engine_finishedJobVertexInfo` | 已完成作业的 Vertex 级信息 |
| `engine_stateTimestamps` | 状态流转时间戳 |

### MapStore（磁盘持久化）

默认情况下，IMap 数据仅保存在集群内存中。通过 Hazelcast MapStore 可以将 IMap 条目写入文件系统，
使其在整个集群重启后仍可恢复。它与 Checkpoint 存储**相互独立**，并且发行版自带的
`config/hazelcast.yaml` 中**默认未启用**。

如需启用，请在 `hazelcast.yaml` 中添加 `map` 配置。配置项名称支持 Hazelcast 的 `*`
通配符，可同时覆盖所有引擎 IMap：

```yaml
# hazelcast.yaml
map:
  engine_*:
    map-store:
      enabled: true
      initial-mode: EAGER
      class-name: org.apache.seatunnel.engine.server.persistence.FileMapStore
      properties:
        type: hdfs                  # 后端文件系统：hdfs（默认）| s3 | oss | gcs
        namespace: /seatunnel-imap  # IMap 文件根路径（默认 /seatunnel-imap）
        clusterName: seatunnel      # 必填，用于集群隔离的路径段
        businessName: seatunnel     # 必填，用于区分业务数据
        fs.defaultFS: hdfs://localhost:9000   # type 为 hdfs 时必填
```

注意：`engine_runningJobMetrics` 不会被持久化——运行时指标快照属于辅助观测状态。

IMap 文件写入 `<namespace>/<clusterName>/<businessName>/` 目录。

### IMap、MapStore 与 Checkpoint 的关系

```
Checkpoint 存储  <──────────────────────────────────>  算子状态（偏移量、Split）
IMap / MapStore  <──────────────────────────────────>  作业 / 流水线生命周期状态
```

二者**完全独立**。删除 Checkpoint 存储不影响 IMap；反之亦然。
作业可以从 Checkpoint 恢复，即使 IMap MapStore 已被清除——但作业 ID 和流水线映射需要**重新提交**，
因为 `engine_runningJobState` 已丢失。

---

## 4. WAL（预写日志）

文件型 IMap 存储插件（`imap-storage-file`）将 IMap 更新以追加写方式写入
`<namespace>/<clusterName>/<businessName>/` 目录下的 WAL 文件，并周期性地将其归档为紧凑的 ORC 文件。
WAL 用于保证 IMap 数据在进程崩溃时的耐久性；WAL 没有面向用户的压缩阈值或刷新间隔配置项。

**已完成作业**的 WAL 文件，可以在作业历史已过期且确认不会再从该作业恢复后删除。
**不要**在作业运行期间手动删除 IMap 存储文件。

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
| 集群重启后作业仍保持已移除（启用 IMap 持久化时） | 是（过期时间随条目一起持久化，已过期的作业不会被重新加载，尚未过期的作业保留剩余时间） |
| 删除该作业的 MapStore 持久化文件 | **否**（过期记录仍保留在 IMap 存储文件中） |
| 删除 Checkpoint 存储目录 | **否** |
| 删除 Savepoint 目录 | **否** |
| 删除 WAL 文件 | **否** |

**核心结论**：`history-job-expire-minutes` 仅清理 `engine_finishedJobState` 中的作业元数据。
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

- `engine_runningJobState`：每条流水线约 50–200 字节
- `engine_runningJobMetrics`：每次指标刷新每条流水线约 1–2 KB

对于运行 100 个 CDC 作业的集群：

```
imap 内存 ≈ 100 × 200B ≈ 20 KB（状态）
WAL 磁盘：建议每节点规划 2–5 GB
```

使用以下命令持续监控：

```bash
du -sh <namespace>/<clusterName>/<businessName>/
watch -n 60 'du -sh /seatunnel-imap/'
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

**现象**：IMap 存储目录（如 `/seatunnel-imap/`）占满 Master / Worker 节点磁盘。

**诊断**：

```bash
du -sh <namespace>/<clusterName>/<businessName>/
```

**可能根因**：

- `engine_runningJobMetrics` 与已完成作业条目的增长速度快于 `history-job-expire-minutes` 的清理速度
- IMap 存储文件在两次归档之间为追加写，长期运行作业的频繁小量更新会推高磁盘占用

**修复**：

```yaml
# seatunnel.yaml —— 缩短已完成作业历史的保留时间
seatunnel:
  engine:
    history-job-expire-minutes: 1440
```

作业过期后，对应的 IMap 条目会被移除，存储文件可在维护窗口内压缩或删除（手动删除后需重启引擎以触发重载）。

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
