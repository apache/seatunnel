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
      interval: 300000             # 两次 Checkpoint 间隔（毫秒）
      timeout: 60000               # Checkpoint 完成超时（毫秒）
      storage:
        type: hdfs                 # hdfs（通过 HDFS API 同时支持 S3、本地文件）| localfile（已废弃）
        plugin-config:
          namespace: /seatunnel/checkpoint/    # 必须以 / 结尾
          # S3 示例：
          # fs.s3a.endpoint: https://s3.amazonaws.com
          # fs.s3a.access.key: <your-access-key>
          # fs.s3a.secret.key: <your-secret-key>
          # 环境变量凭据：
          # fs.s3a.aws.credentials.provider: com.amazonaws.auth.EnvironmentVariableCredentialsProvider
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
| `engine_runningJobInfo` | 已提交作业的信息与初始化元数据（作业 ID、作业名称等；指标在作业完成后存于 `engine_finishedJobMetrics`）|
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

在分离式集群模式下，Master 角色的成员存储 IMap 数据；Worker 角色的成员是 Hazelcast lite member，
不拥有 IMap 分区。因此，持久化可能在多个 Master 角色的成员上执行，而不只是当选的 active master。

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

IMap 持久化是一种追加写（append-only）的预写日志：`FileMapStore` 委托 `IMapFileStorage`，通过 WAL
写入器把每次状态变更追加到配置的 `namespace` 目录下的 region 文件中。每个写入器针对每个 IMap
使用独立的 region。重启时，重放会读取该 IMap 的所有 region，在内存中选出每个 key 的最新变更，
但不会压缩这些文件。

### 可选的 region 级 WAL 压缩

Region 级快照压缩可以减少写入器当前 region 中同一个 key 的重复更新记录。默认关闭。在相关 IMap
的 `map-store.properties` 中启用：

```yaml
        type: hdfs
        storage.type: hdfs
        fs.defaultFS: hdfs://localhost:9000  # 单节点集群也可使用 file:///
        compaction.enabled: true
        compaction.interval-seconds: 300
        wal.segment-size: 67108864
```

| 配置项 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `compaction.enabled` | boolean | `false` | 为 HDFS 或本地文件 IMap 存储启用 region 级快照压缩。 |
| `compaction.interval-seconds` | long | `300` | 两次压缩检查之间的间隔秒数，必须大于零。仅在启用压缩时使用。 |
| `wal.segment-size` | long | `67108864`（64 MiB）| WAL 分段轮转大小，单位为字节，必须大于零。仅在启用压缩时使用。 |

压缩要求 `storage.type: hdfs`，且底层文件系统为 `hdfs://` 或 `file:///`。
对 S3、OSS、GCS 或其他文件系统 scheme 启用压缩会被拒绝；这些后端仍使用原有的写入器。
使用这些后端时请保持压缩关闭。

启用后，写入器会将 WAL 轮转为多个分段。每个 JVM 共享一个后台线程，依次处理各个 IMap。
每次压缩只合并该写入器当前 region 中已关闭的分段和上一次快照，不处理活动分段、其他写入器的
region，也不会接管或压缩之前运行遗留的 region。重启时仍然会读取这些旧 region。

快照保留每个 key 的最新原始变更，包括删除标记和已过期的 put。其他 region 中可能仍有该 key 的旧值，
因此必须保留这些记录。Region 级压缩可以减少更新历史，但不会限制历史 key 的总数，也不会回收所有
已过期作业元数据。它不改变 IMap 过期规则或 Checkpoint 保留规则。
如果最新变更按 WAL 排序规则比较相等，但过期时间不同，则本次压缩会推迟且不删除输入文件，以保留原有重放语义。

快照完整写入后，通过原子重命名发布；如果加载期间快照发生变化，重放会重试。分段和使用唯一名称的
快照沿用现有的 WAL 记录格式，文件名以 `wal.txt` 结尾，因此旧版本可以重放不再变化的文件。
但是，旧版本无法在压缩同时执行时安全地加载。启用压缩前，必须将所有使用 IMap 持久化的成员升级到
支持压缩期间重放的版本。回滚到旧版本或使用旧版本重放前，应关闭压缩并等待正在执行的压缩完成。
默认值仍为 `false`；关闭压缩时，原有的单个 `wal.txt` 写入方式保持不变。

压缩会在内存中为输入中的每个不同 key 保留一条最新变更。规划 JVM 堆内存时，应在内存 IMap 之外
预留这部分空间，并继续监控存储增长。

**缓解措施：**

- 让已完成作业按期过期：`history-job-expire-minutes` 会将已完成作业的记录从 IMap 中移除，
  使其不再被持久化。
- 在 Master 节点上监控 namespace 目录：

```bash
du -sh /tmp/seatunnel/imap/   # 替换为你在 hazelcast.yaml 中配置的 namespace（本地文件存储，即 fs.defaultFS 以 file:/// 开头时）
hadoop fs -du -s -h <fs-uri>/<namespace>   # HDFS 或对象存储后端使用该命令查看
```

**不要**按作业选择性地删除 MapStore 文件：并不存在按作业划分的文件，手工删除文件可能损坏下次
Master 启动时的 IMap 重放。正确做法是为 namespace 卷规划足够的容量；只有当集群停止、且可以接受
丢失所有运行中作业的 IMap 状态时，才整体清空 namespace。

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
| 从 `engine_finishedJobState` IMap 中移除 | 是 |
| 从 `engine_finishedJobMetrics` IMap 中移除 | 是 |
| 集群重启后作业仍保持已移除（启用 IMap 持久化时） | 是（过期时间随条目一起持久化，已过期的作业不会被重新加载，尚未过期的作业保留剩余时间） |
| 删除该作业的 MapStore 持久化文件 | **否**（region 级压缩也会保留每个 key 的最新过期变更） |
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

公式：`checkpoint_size ≈ 表数量 × 平均 Split 数 × 每 Split 状态大小`（同一时刻只有一个 Checkpoint 在途，因此无需乘并发系数）

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
du -sh /tmp/seatunnel/imap/   # 替换为你配置的 MapStore namespace（本地文件存储场景）
hadoop fs -du -s -h <fs-uri>/<namespace>   # HDFS 或对象存储后端使用该命令查看
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
du -sh /tmp/seatunnel/imap/   # 替换为你在 hazelcast.yaml 中配置的 namespace（本地文件存储，即 fs.defaultFS 以 file:/// 开头时）
hadoop fs -du -s -h <fs-uri>/<namespace>   # HDFS 或对象存储后端使用该命令查看
```

**可能根因**：

- 已完成作业未过期（`history-job-expire-minutes` 未设置或过大）
- 运行中作业过多，状态和指标持续更新

**修复**：

在 `seatunnel.yaml` 中启用 / 调整 `history-job-expire-minutes`。MapStore 的 region 文件无法按作业
裁剪；要回收其空间需要停止集群并整体清空 namespace，这会丢弃所有作业的 IMap 状态。

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
- [ ] MapStore 数据：仅支持整体清空 namespace（先停止集群），不支持按作业选择性删除

---

## 参考

- [Checkpoint 存储配置](checkpoint-storage.md)
- [REST API v2](rest-api-v2.md) — 通过 API 查询作业状态和指标
