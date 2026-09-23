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
| Savepoint（保存点）| 用户手动触发的命名检查点，用于计划性停止或重启 | 与 Checkpoint 同一存储，位于所属作业自身的目录下 |
| IMap / MapStore | 分布式内存状态（作业元数据、作业状态、历史记录）| 默认仅存于内存；可通过 `hazelcast.yaml` 中的 Hazelcast MapStore 配置持久化 |

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
      storage:
        type: hdfs                 # hdfs（通过 HDFS API 同时支持 S3、本地文件）| localfile（已废弃）
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
- 默认情况下，被取消的作业仍会清理 Checkpoint 数据。如果希望取消后的作业还能从最新成功完成的 Checkpoint 恢复，需要在历史作业取消前将
  作业 `env` 中的 `checkpoint.retain-after-job-cancelled` 或 `seatunnel.yaml` 中的
  `seatunnel.engine.checkpoint.retain-after-job-cancelled` 设置为 `true`。
- **安全清理规则**：只有在作业已取消且确认不会从该 Checkpoint 恢复时，才可删除对应 job-id 目录。

---

## 2. Savepoint（保存点）

### Savepoint 与 Checkpoint 的区别

| 维度 | Checkpoint | Savepoint |
|---|---|---|
| 触发方式 | 周期性 / 自动 | 手动（`$SEATUNNEL_HOME/bin/seatunnel.sh --savepoint <jobId>`）|
| 用途 | 容错恢复 | 计划性停止、升级、迁移 |
| 生命周期 | 引擎管理 | 操作人员管理 |
| 保留策略 | 自动轮转 | 手动删除前永久保留 |

### 触发 Savepoint

```bash
# 停止运行中的作业并创建 Savepoint
$SEATUNNEL_HOME/bin/seatunnel.sh --savepoint <job-id>

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

Savepoint 是保存点类型的 Checkpoint：它写入上文配置的同一个 Checkpoint 存储中，位于所属作业的目录下
（`<namespace>/<job-id>/`）。不存在独立的 savepoint 根目录。

### 安全清理

只有在确认永不从该 Savepoint 恢复作业时，才可删除。恢复过程中删除 Savepoint 会导致作业以"状态未找到"错误失败。

---

## 3. IMap 与 MapStore（Hazelcast 分布式状态）

### IMap 存储的内容

SeaTunnel Engine 使用 Hazelcast IMap 作为分布式内存键值存储。引擎使用的主要逻辑映射包括：

| IMap 名称 | 内容 |
|---|---|
| `engine_runningJobInfo` | 已提交运行作业的信息（作业 ID、作业名称、指标快照）|
| `engine_runningJobState` | 运行中作业和流水线的状态机当前状态 |
| `engine_stateTimestamps` | 作业 / 流水线状态流转的时间戳 |
| `engine_finishedJobState` | 已完成、已取消或失败作业的终态 |
| `engine_finishedJobMetrics` | 作业终止后的最终指标快照 |
| `engine_finishedJobVertexInfo` | 已完成作业的执行节点信息 |

### MapStore（磁盘持久化）

默认情况下 IMap 数据仅存储在内存中（并按备份份数在节点间复制）。如果所有节点停止，数据将丢失，
除非启用了 MapStore 持久化。启用后，Hazelcast MapStore 会将 IMap 条目写入外部文件系统（HDFS、S3，
或通过 HDFS API 访问的本地文件），使数据在整个集群重启后仍然可用。这与 Checkpoint 存储**相互独立**。

持久化在 `hazelcast.yaml` 中配置（详见[Zeta 集群分离式部署](separated-cluster-deployment.md)）：

```yaml
map:
  engine*:
    map-store:
      enabled: true
      initial-mode: EAGER
      factory-class-name: org.apache.seatunnel.engine.server.persistence.FileMapStoreFactory
      properties:
        type: hdfs
        namespace: /tmp/seatunnel/imap     # 存储 namespace（未设置时默认 /seatunnel-imap）
        clusterName: seatunnel-cluster
        storage.type: hdfs
        fs.defaultFS: hdfs://localhost:9000
```

在分离式集群模式下，只有 Master 节点存储 IMap 数据，因此该配置仅在 Master 节点生效。

### IMap、MapStore 与 Checkpoint 的关系

```
Checkpoint 存储  <──────────────────────────────────>  算子状态（偏移量、Split）
IMap / MapStore  <──────────────────────────────────>  作业 / 流水线生命周期状态
```

二者**完全独立**。删除 Checkpoint 存储不影响 IMap；反之亦然。
作业可以从 Checkpoint 恢复，即使 IMap MapStore 数据已被清除——但作业 ID 和流水线映射需要**重新提交**，
因为运行作业的状态已丢失。

---

## 4. MapStore 文件维护

IMap 持久化会把每个 map 以文件形式写入配置的 `namespace` 目录下。长期运行的集群中，只要作业持续运行、
状态持续变化，这些文件就会不断增长。

**缓解措施：**

- 让已完成作业按期过期：`history-job-expire-minutes` 会将已完成作业的记录从 IMap 中移除，
  使其不再被持久化。
- 在 Master 节点上监控 namespace 目录：

```bash
du -sh /tmp/seatunnel/imap/   # 替换为你在 hazelcast.yaml 中配置的 namespace
```

**不要**删除运行中作业的 MapStore 文件。记录已从 IMap 中过期的已完成作业，可以在集群停止时删除其文件。

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
| 从已完成作业 IMap（`engine_finishedJobState`、`engine_finishedJobMetrics`）中移除记录 | 是 |
| 删除 Checkpoint 存储目录 | **否** |
| 删除 Savepoint 数据 | **否** |

**核心结论**：`history-job-expire-minutes` 仅清理已完成作业 IMap 中的作业元数据。
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

### IMap 存储容量估算

每个运行中的 CDC 作业大约占用：

- 作业状态：每条流水线约 50–200 字节
- 指标：每次指标刷新每条流水线约 1–2 KB

对于运行 100 个 CDC 作业的集群，IMap 数据量在几 MB 量级。MapStore namespace 的磁盘占用随状态
流转历史增长，长期运行的集群建议规划数 GB 空间，并使用以下命令持续监控：

```bash
du -sh /tmp/seatunnel/imap/   # 替换为你配置的 MapStore namespace
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

### MapStore namespace 目录增长

**现象**：配置的 MapStore `namespace` 目录占满 Master 节点磁盘。

**诊断**：

```bash
du -sh /tmp/seatunnel/imap/   # 替换为你在 hazelcast.yaml 中配置的 namespace
```

**可能根因**：

- 已完成作业未过期（`history-job-expire-minutes` 未设置或过大）
- 运行中作业过多，状态和指标持续更新

**修复**：

在 `seatunnel.yaml` 中启用 / 调整 `history-job-expire-minutes`；如有需要，可在集群停止时清理
已过期作业的文件。

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

- [ ] 作业已处于 `FINISHED`、`CANCELED` 或 `FAILED` 终态
- [ ] 确认不会从该 Checkpoint 或 Savepoint 恢复
- [ ] 确认该作业 ID 未被任何监控或告警规则引用
- [ ] Checkpoint 存储：递归删除 `<namespace>/<job-id>/`
- [ ] MapStore 数据：先停止集群，再删除已过期作业的文件

---

## 参考

- [Checkpoint 存储配置](checkpoint-storage.md)
- [REST API v2](rest-api-v2.md) — 通过 API 查询作业状态和指标
