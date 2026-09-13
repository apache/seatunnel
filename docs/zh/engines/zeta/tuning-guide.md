---
sidebar_position: 15
---

# 调优指南

本文为大家介绍 SeaTunnel Engine 的调优方法，帮助用户根据实际需求优化 SeaTunnel Engine 的性能和稳定性。
阅读次篇前请知晓，当前指南结合的是大部分用户的真实使用情况总结而成，可能并不适用于所有场景，用户可以根据实际情况进行调整。

SeaTunnel Engine 是基于 [JVM](https://zh.wikipedia.org/wiki/Java%E8%99%9A%E6%8B%9F%E6%9C%BA) 运行的数据集成引擎，所以 JVM 部分的调优对 SeaTunnel Engine 同样适用，这里就不再赘述。

## 集群响应缓慢或假死

### JVM

如果 SeaTunnel Engine 集群响应缓慢或假死，可能是由于 JVM 堆内存不足导致的。可以通过以下步骤进行排查：

#### 堆内存不足

##### 排查流程

1. 检查 JVM 堆内存实时占用
   使用 `jcmd` 命令查看 JVM 堆内存使用情况, 其中 `<pid>` 是 SeaTunnel Engine 进程的 PID。
   ```bash
   jmap -heap <pid>
   ```
   输出结果示例：
   ```shell
    Attaching to process ID 2111950, please wait...
    Debugger attached successfully.
    Server compiler detected.
    JVM version is 25.192-b12
    
    using thread-local object allocation.
    Garbage-First (G1) GC with 13 thread(s)
    
    Heap Configuration:
    MinHeapFreeRatio         = 40
    MaxHeapFreeRatio         = 70
    MaxHeapSize              = 17179869184 (16384.0MB)
    NewSize                  = 1363144 (1.2999954223632812MB)
    MaxNewSize               = 10301210624 (9824.0MB)
    OldSize                  = 5452592 (5.1999969482421875MB)
    NewRatio                 = 2
    SurvivorRatio            = 8
    MetaspaceSize            = 21807104 (20.796875MB)
    CompressedClassSpaceSize = 1073741824 (1024.0MB)
    MaxMetaspaceSize         = 2147483648 (2048.0MB)
    G1HeapRegionSize         = 8388608 (8.0MB)
    
    Heap Usage:
    G1 Heap:
    regions  = 2048
    capacity = 17179869184 (16384.0MB)
    used     = 2997548048 (2858.684585571289MB)
    free     = 14182321136 (13525.315414428711MB)
    17.448026034981012% used
    G1 Young Generation:
    Eden Space:
    regions  = 348
    capacity = 10737418240 (10240.0MB)
    used     = 2919235584 (2784.0MB)
    free     = 7818182656 (7456.0MB)
    27.1875% used
    Survivor Space:
    regions  = 10
    capacity = 83886080 (80.0MB)
    used     = 83886080 (80.0MB)
    free     = 0 (0.0MB)
    100.0% used
    G1 Old Generation:
    regions  = 0
    capacity = 6358564864 (6064.0MB)
    used     = 0 (0.0MB)
    free     = 6358564864 (6064.0MB)
    0.0% used
   ```
   重点关注G1 Old Generation的使用情况，如果 Old Generation 的使用率接近 100%，则可能是堆内存不足导致的。
2. 检查日志
   系统会不定期输出健康监控日志，检查 SeaTunnel Engine 的日志，查看是否有频繁的 Full GC 或者长时间的 GC 暂停，这可能是由于堆内存不足导致的。
   下边是一个日志示例：
   ```log
   [] 2025-07-04 16:42:54,818 INFO  [c.h.i.d.HealthMonitor         ] [hz.main.HealthMonitor] - [127.0.0.1]:5801 [seatunnel] [5.1] processors=16, physical.memory.total=31.1G, physical.memory.free=9.7G, swap.space.total=0, swap.space.free=0, heap.memory.used=198.7M, heap.memory.free=15.8G, heap.memory.total=16.0G, heap.memory.max=16.0G, heap.memory.used/total=1.21%, heap.memory.used/max=1.21%, minor.gc.count=2, minor.gc.time=44ms, major.gc.count=0, major.gc.time=0ms, load.process=0.00%, load.system=66.67%, load.systemAverage=5.66, thread.count=118, thread.peakCount=118, cluster.timeDiff=0, event.q.size=0, executor.q.async.size=0, executor.q.client.size=0, executor.q.client.query.size=0, executor.q.client.blocking.size=0, executor.q.query.size=0, executor.q.scheduled.size=0, executor.q.io.size=0, executor.q.system.size=0, executor.q.operations.size=0, executor.q.priorityOperation.size=0, operations.completed.count=13, executor.q.mapLoad.size=0, executor.q.mapLoadAllKeys.size=0, executor.q.cluster.size=0, executor.q.response.size=0, operations.running.count=0, operations.pending.invocations.percentage=0.00%, operations.pending.invocations.count=0, proxy.count=9, clientEndpoint.count=0, connection.active.count=0, client.connection.count=0, connection.count=0
   ```
   重点关注：
    - `heap.memory.used/max`：堆内存使用率，如果接近 100%，则可能是堆内存不足。
    - `major.gc.count` 和 `major.gc.time` ：如果 Full GC 频繁，可能是堆内存不足导致的。
   可以通过持续查看日志来判断是否存在频繁的 Full GC 或者长时间的 GC 暂停。

##### 解决方案

通过降低任务并发和任务数量来降低同一时间的内存占用。如果确实需要更多的内存，请参考 [安装部署](deployment.md) 中的配置 SeaTunnel Engine JVM 选项来增加内存。

##### 内存无限制占用
1. 生成内存快照

   有些时候，我们的任务量固定，但是内存使用量却不断增加，这可能是由于任务中存在内存泄漏导致的。请dump下对应的内存快照信息。
   ```shell
   jmap -dump:live,format=b,file=heap.hprof <pid>
   ```
   然后使用 [Eclipse Memory Analyzer](https://www.eclipse.org/mat/) 等工具分析内存快照，查找内存泄漏的原因。
   针对非二开的用户或者连接器，也可以创建一个 issue 并附上内存快照，我们会帮助您分析。

2. 打印对象占用排行

   有些时候，生成内存快照会随着JVM的假死而失败，这时可以尝试打印对象占用排行来查看内存使用情况。
   ```shell
   jmap -histo:live <pid> | head -n 100
   ```
   同样的，可以通过分析输出结果来查找内存泄漏的原因。
   针对非二开的用户或者连接器，也可以创建一个 issue 并附上对象占用信息，我们会帮助您分析。

#### CPU占用率过高

CPU占用率过高也是一个集群节点假死的常见原因，但是出现概率基本没有内存占用过高的情况高。可以通过以下步骤进行排查：

##### 排查流程
1. 检查 CPU 占用率
   - 使用 `top` 或 `htop` 命令查看 SeaTunnel Engine 进程的 CPU 占用率。
   - 如果 CPU 占用率接近 100%，则可能是 CPU 资源不足导致的。如果有多个核，需要考虑多个核的占用率。

##### 解决方案

如果 CPU 占用率过高，可以尝试以下解决方案：
- 降低任务并发和任务数量，减少 CPU 资源的占用。
- 增加集群节点数量，分担 CPU 资源的压力。

### Hazelcast

Hazelcast相关的配置也是影响 SeaTunnel Engine 性能的重要因素。可以通过修改`hazelcast.yaml`系列文件的配置参数修改，请参考 [安装部署](deployment.md) 。
以下是一些常见的调优参数：
- `hazelcast.operation.generic.thread.count`: 该参数控制 Hazelcast 的通用操作线程数。SeaTunnel Engine 使用此线程用于执行RPC请求。可以根据实际情况调整该参数，以提高 Hazelcast RPC 的性能。
如果监控到日志中频繁出现如下类型日志，同时CPU占用率不算很高。请尝试调高该参数：
```log
2024-09-03 06:15:45,807 WARN  [.s.i.o.s.SlowOperationDetector] [hz.main.SlowOperationDetectorThread] - [seatunnel-worker-1]:5802 [seatunnel] [5.1] Slow operation detected:
```

## 慢操作排查手册

本节提供一份实用的分步排查指南，帮助诊断和解决生产环境中 SeaTunnel Zeta 集群的 Hazelcast 慢操作告警。

### 1. 理解 `SlowOperationDetector` 告警

Hazelcast 的 `SlowOperationDetector` 监控分区线程上的操作执行时间。当某个操作的执行时间超过配置的阈值（默认 10 秒）时，会记录一条告警日志：

```log
2024-09-03 06:15:45,807 WARN  [.s.i.o.s.SlowOperationDetector] [hz.main.SlowOperationDetectorThread] -
  [seatunnel-worker-1]:5802 [seatunnel] [5.1] Slow operation detected:
  operation=com.hazelcast.map.impl.operation.PutOperation, duration=5234ms, ...
```

**在 SeaTunnel Zeta 中的含义：**
- Hazelcast 操作是 SeaTunnel 分布式协调的骨干——作业提交、状态同步、Checkpoint 协调和 IMap 读写都通过 Hazelcast 操作完成。
- 慢操作告警表明分区线程被阻塞的时间超过了预期，可能连锁导致作业提交超时、Checkpoint 失败或集群不稳定。
- 告警本身是**症状**，而非根因。你必须定位到具体导致延迟的层面。

| 症状 | 可能原因 |
|---|---|
| 作业提交时出现慢操作 | Master 节点 CPU 饱和、通用操作线程不足、或作业配置序列化过大 |
| Checkpoint 期间出现慢操作 | Checkpoint 存储 I/O 延迟（S3/HDFS）、状态数据过大、或网络争用 |
| IMap 访问时出现慢操作 | MapStore 磁盘 I/O 瓶颈、WAL 写入压力、或内存压力导致 GC |
| 所有负载下持续出现慢操作 | 集群资源不足、节点间网络延迟、或 JVM GC 暂停 |

### 2. 诊断延迟来源

使用以下决策树来缩小慢操作根因的排查范围。

#### 第一步：确定慢操作发生的时间点

```bash
# 检查慢操作日志的频率和时间
grep "SlowOperationDetector" $SEATUNNEL_HOME/logs/seatunnel-server.log | tail -50
```

将时间戳与以下事件关联：
- 作业提交事件（REST API 调用）
- Checkpoint 间隔（默认每 10 秒一次）
- 高负载时段（数据摄入高峰期）

#### 第二步：检查节点整体健康状态

```bash
# 检查 CPU、内存和磁盘 I/O
top -bn1 | head -20
iostat -x 1 5
free -h
```

#### 第三步：定位瓶颈层

**REST 提交延迟：**
- 症状：通过 REST API 提交作业时出现慢操作，且提交客户端响应时间较长。
- 检查：`grep "submitJob" $SEATUNNEL_HOME/logs/seatunnel-server.log` —— 关注耗时。
- 常见原因：Master 节点并发提交过载，或作业配置非常庞大（连接器/Transform 数量多）。
- 缓解：限制并发提交速率、增加 Master 节点资源、或调整 `hazelcast.operation.generic.thread.count`。

**Master 调度压力：**
- 症状：慢操作集中在作业生命周期事件（INIT → RUNNING 转换）附近，且 Master 节点 CPU 持续较高。
- 检查：Master 节点健康监控日志中的 `executor.q.operations.size` 和 `operations.pending.invocations.percentage`。
- 常见原因：并发作业或流水线过多，竞争 Master 调度线程。
- 缓解：减少并发作业数，或调整 Master 节点的 `hazelcast.operation.generic.thread.count`。

**Worker 执行压力：**
- 症状：Worker 节点上出现慢操作，尤其在 Checkpoint 协调期间。
- 检查：Worker 节点健康监控日志中的 `executor.q.operations.size` 和线程池饱和度。
- 常见原因：Worker 因连接器执行导致 CPU 或 I/O 饱和，剩余资源不足以处理 Hazelcast 操作。
- 缓解：增加 Worker 节点、降低单 Worker 任务并发度、或调整 Worker 节点的 `hazelcast.operation.generic.thread.count`。

**Checkpoint 存储延迟：**
- 症状：慢操作与 Checkpoint 间隔对齐，且 Checkpoint 耗时超过配置的超时。
- 检查：为 `org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinator` 启用 DEBUG 日志，然后执行 `grep "pending checkpoint completed" $SEATUNNEL_HOME/logs/seatunnel-server.log | grep -oP 'cost: \d+ms'` 查看 Checkpoint 耗时。
- 如果使用 S3：运行 `aws s3api head-object --bucket <bucket> --key <checkpoint-path>` 测量延迟，或查看 CloudWatch S3 指标（`FirstByteLatency`、`TotalRequestLatency`）。
- 常见原因：到 S3/HDFS 的网络延迟高、小文件导致多次往返、或 S3 限流。
- 缓解：参见[第 6 节](#6-s3-checkpoint状态存储延迟)。

**IMap / MapStore 延迟：**
- 症状：对 IMap 键执行 `PutOperation` 或 `GetOperation` 时出现慢操作。
- 检查：`du -sh $SEATUNNEL_HOME/imap/wal/` 和 `du -sh $SEATUNNEL_HOME/imap/maps/` —— WAL 目录过大表示写入压力大。
- 常见原因：MapStore 目录磁盘 I/O 饱和、WAL 写入频率过高、或磁盘空间耗尽。
- 缓解：参见[第 6 节](#6-s3-checkpoint状态存储延迟)，增加 `write-behind-delay-seconds`，启用 WAL 压缩。

### 3. 合理配置 `hazelcast.operation.generic.thread.count`

`hazelcast.operation.generic.thread.count` 参数控制 Hazelcast 用于执行通用操作（包括 RPC 请求、IMap 操作和 Checkpoint 协调）的线程数。正确的配置取决于你的部署模式。

**配置位置：** `hazelcast.yaml` 中 `hazelcast` 顶级属性下：

```yaml
hazelcast:
  properties:
    hazelcast.operation.generic.thread.count: <number>
```

#### 混合模式（Master + Worker 在同一节点）

在混合模式下，每个节点同时运行 Master 和 Worker 进程。通用操作线程池由 Master 协调和 Worker 任务执行共享。

| 每节点物理 CPU 核数 | 推荐 `generic.thread.count` |
|---|---|
| 4–8 | 4–8 |
| 8–16 | 8–16 |
| 16–32 | 16–24 |
| 32+ | 24–32（极少需要更多） |

**经验法则：** `generic.thread.count = min(CPU 核数, 24)`。不要超过物理核数，过度订阅会导致上下文切换开销，反而加剧延迟。

**配置不足的警告信号：**
- 健康监控日志中 `executor.q.operations.size` 持续 > 0
- `operations.pending.invocations.percentage` > 10%
- 正常作业提交时频繁出现 `SlowOperationDetector` 告警

**配置过度的警告信号：**
- CPU 使用率高（>80%）但并非来自应用工作
- 上下文切换率升高（`vmstat 1` 显示 `cs` > 100k/秒）

#### 分离模式（Master 和 Worker 在不同节点）

在分离模式下，Master 节点只处理集群协调和作业调度，Worker 节点只执行任务。应分别调整不同角色的线程数。

**Master 节点：**
- Master 节点处理作业提交、Checkpoint 协调和 IMap 操作。
- `generic.thread.count = min(CPU 核数, 16)` 通常足够。
- 重点关注避免排队：如果 `executor.q.operations.size` 增长，增加线程数。
- Master 节点通常 CPU 负载较轻；8 核 Master 上 4–8 个线程是合理的起点。

**Worker 节点：**
- Worker 节点执行连接器任务并通过 Hazelcast 参与 Checkpoint 协调。
- `generic.thread.count = min(CPU 核数 - 为连接器预留的核数, 16)`。
- 至少为连接器执行预留 2–4 个核。例如，在 16 核的 Worker 上：`generic.thread.count = 12`。
- Worker 节点更有可能出现慢操作告警，因为它们的应用负载更重。

| 节点角色 | CPU 核数 | 推荐 `generic.thread.count` |
|---|---|---|
| Master（分离） | 4–8 | 4–8 |
| Master（分离） | 8–16 | 8–12 |
| Worker（分离） | 8–16 | 4–12（为连接器预留 2–4 核） |
| Worker（分离） | 16–32 | 8–16（为连接器预留 4–8 核） |

### 4. 调优前需要收集的指标和日志

在进行任何配置更改之前，请收集以下数据建立基线。

#### 4.1 健康监控日志

SeaTunnel 定期输出健康监控日志（默认每 60 秒一次）。这些日志包含关键指标：

```log
[] 2025-07-04 16:42:54,818 INFO  [c.h.i.d.HealthMonitor] [hz.main.HealthMonitor] -
  [127.0.0.1]:5801 [seatunnel] [5.1]
  heap.memory.used/max=1.21%,
  major.gc.count=0, major.gc.time=0ms,
  executor.q.operations.size=0,
  executor.q.priorityOperation.size=0,
  operations.pending.invocations.percentage=0.00%,
  operations.pending.invocations.count=0,
  operations.running.count=0
```

**重点关注的指标：**
- `executor.q.operations.size`：通用操作队列中等待的操作数。如果持续 > 0，增加 `generic.thread.count`。
- `operations.pending.invocations.percentage`：待处理远程调用百分比。如果 > 10%，检查网络延迟或增加线程数。
- `operations.running.count`：当前正在执行的操作数。数值较高可能表示存在长时间运行的操作。
- `heap.memory.used/max`：如果 > 85%，GC 压力可能导致慢操作。
- `major.gc.count` 和 `major.gc.time`：频繁 Full GC 会导致操作暂停。

#### 4.2 慢操作日志

```bash
# 提取慢操作告警及其耗时
grep "SlowOperationDetector" $SEATUNNEL_HOME/logs/seatunnel-server.log | tail -20
```

#### 4.3 节点资源指标

```bash
# 每核心 CPU 使用率
mpstat -P ALL 1 5

# 内存使用
free -h

# 磁盘 I/O
iostat -x 1 5

# 网络
netstat -i
```

#### 4.4 集群概览

```bash
# 检查运行中的作业
curl http://<master>:8080/running-jobs

# 检查已完成的作业
curl "http://<master>:8080/finished-jobs/FINISHED?page=1&rows=100"
```

### 5. 配置变更：重启 vs 热加载

并非所有配置更改都会立即生效。请使用此参考表确定是否需要重启。

| 配置项 | 文件 | 是否需要重启？ | 备注 |
|---|---|---|---|
| `hazelcast.operation.generic.thread.count` | `hazelcast.yaml` | **是**（全集群重启） | Hazelcast 线程池在启动时初始化 |
| `hazelcast.operation.call.timeout.millis` | `hazelcast.yaml` | **是**（全集群重启） | 操作超时在成员初始化时读取 |
| `seatunnel.engine.checkpoint.interval` | `seatunnel.yaml` | **是**（节点重启） | 重启后生效 |
| `seatunnel.engine.checkpoint.timeout` | `seatunnel.yaml` | **是**（节点重启） | 重启后生效 |
| `seatunnel.engine.checkpoint.storage.*` | `seatunnel.yaml` | **是**（节点重启） | 重启后生效 |
| `seatunnel.engine.history-job-expire-minutes` | `seatunnel.yaml` | **是**（节点重启） | 重启后生效 |
| JVM 堆大小（`-Xmx`、`-Xms`） | JVM 选项 | **是**（进程重启） | JVM 堆在进程启动时分配 |
| `hazelcast.initial.min.cluster.size` | `hazelcast.yaml` | **是**（全集群重启） | 集群组建参数在启动时读取 |

**重要提示：** 对于需要全集群重启的 Hazelcast 配置变更，必须重启所有节点（Master 和 Worker）以确保集群配置一致。混合配置的滚动重启可能导致不可预测的行为。

### 6. S3 Checkpoint/状态存储延迟

当 Checkpoint 存储配置为 S3 时，网络延迟和 S3 限流可能成为慢操作的主要原因。

#### 6.1 诊断 S3 延迟

**从集群节点检查 S3 端点延迟：**
```bash
# 测量 DNS 解析和连接时间
curl -w "DNS: %{time_namelookup}s, Connect: %{time_connect}s, TTFB: %{time_starttransfer}s, Total: %{time_total}s\n" \
  -o /dev/null -s https://s3.amazonaws.com

# 对于 S3 兼容存储（MinIO 等）
curl -w "DNS: %{time_namelookup}s, Connect: %{time_connect}s, TTFB: %{time_starttransfer}s, Total: %{time_total}s\n" \
  -o /dev/null -s https://<your-s3-endpoint>
```

**检查 Checkpoint 写入性能：**
```bash
# 监控 Checkpoint 耗时 —— 需要为 CheckpointCoordinator 启用 DEBUG 日志以查看 cost: 字段
# 或者使用健康监控指标来评估 Checkpoint 负载
```

**检查 S3 限流（AWS）：**
```bash
# 检查 S3 是否在限流请求
aws cloudwatch get-metric-statistics \
  --namespace AWS/S3 \
  --metric-name 5xxErrors \
  --dimensions Name=BucketName,Value=<your-bucket> \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 300 \
  --statistics Sum
```

#### 6.2 推荐缓解措施

**1. 使用与集群同区域的 S3 端点：**
```yaml
seatunnel:
  engine:
    checkpoint:
      storage:
        type: hdfs
        plugin-config:
          namespace: /seatunnel/checkpoint/
          s3.bucket: s3a://<your-bucket>
          fs.s3a.endpoint: s3.<region>.amazonaws.com
```

**2. 启用 S3A 快速上传和连接池：**
```yaml
seatunnel:
  engine:
    checkpoint:
      storage:
        type: hdfs
        plugin-config:
          fs.s3a.fast.upload: true
          s3.bucket: s3a://<your-bucket>
          fs.s3a.fast.upload.buffer: disk
          fs.s3a.connection.maximum: 100
          fs.s3a.threads.max: 20
```

**3. 增加 S3A 重试和超时设置：**
```yaml
seatunnel:
  engine:
    checkpoint:
      storage:
        type: hdfs
        plugin-config:
          fs.s3a.attempts.maximum: 10
          s3.bucket: s3a://<your-bucket>
          fs.s3a.connection.timeout: 30000
          fs.s3a.socket.timeout: 60000
          fs.s3a.connection.establish.timeout: 30000
```

**4. 对于高吞吐 Checkpoint 场景，可考虑使用 HDFS 或本地 SSD 存储 Checkpoint，S3 仅用于长期备份。**

**5. 如果集群与 Bucket 不在同一区域，可启用 S3 传输加速。**

### 7. Kubernetes 部署检查清单

在 Kubernetes 上部署 SeaTunnel Zeta 时，以下检查项有助于预防慢操作问题。

#### 7.1 Pod 反亲和性

确保 Master 和 Worker Pod 分散在不同节点，避免资源争用：

```yaml
affinity:
  podAntiAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        podAffinityTerm:
          labelSelector:
            matchLabels:
              app: seatunnel
          topologyKey: kubernetes.io/hostname
```

#### 7.2 资源请求和限制

设置合理的资源请求和限制，避免 CPU 限流：

```yaml
resources:
  requests:
    cpu: "2"
    memory: "4Gi"
  limits:
    cpu: "4"
    memory: "8Gi"
```

**重要提示：** Kubernetes 中的 CPU 限流（CFS 配额）可能导致 Hazelcast 操作超时。如果尽管 CPU 使用率较低但仍然出现 `hazelcast.operation.call.timeout.millis` 超时，请检查 `container_cpu_cfs_throttled_seconds_total` 指标。

#### 7.3 就绪探针

配置验证 SeaTunnel REST API 可达的就绪探针：

```yaml
readinessProbe:
  httpGet:
    path: /running-jobs
    port: 8080
  initialDelaySeconds: 30
  periodSeconds: 10
```

#### 7.4 优雅关闭

确保 Pod 有足够的时间刷写状态并离开集群：

```yaml
terminationGracePeriodSeconds: 60
```

并在 `hazelcast.yaml` 中：
```yaml
hazelcast:
  shutdown-hook:
    enabled: true
    policy: GRACEFUL
```

#### 7.5 日志聚合

确保慢操作日志被日志聚合系统捕获：

```yaml
# 在日志配置中
loggers:
  - name: com.hazelcast.spi.impl.operationexecutor.slowoperationdetector.SlowOperationDetector
    level: WARN
```

#### 7.6 MapStore 和 WAL 的存储

使用持久卷存储 MapStore 和 WAL 目录，以便在 Pod 重启后仍然存在：

```yaml
volumeMounts:
  - name: imap-storage
    mountPath: /tmp/seatunnel/imap
volumes:
  - name: imap-storage
    persistentVolumeClaim:
      claimName: seatunnel-imap-pvc
```

使用以下命令监控 PVC 使用量：
```bash
kubectl exec <pod> -- du -sh /tmp/seatunnel/imap/
```

### 快速参考排查表

| 观察现象 | 最可能原因 | 优先操作 |
|---|---|---|
| 仅作业提交时出现慢操作 | Master CPU 或线程饱和 | 增加 Master 的 `generic.thread.count`，限制提交速率 |
| 慢操作与 Checkpoint 间隔一致 | Checkpoint 存储 I/O 延迟 | 检查 S3/HDFS 延迟，调整 `fs.s3a.*` 设置 |
| 慢操作持续出现，CPU 较低 | 节点间网络延迟 | 检查节点间延迟和网络吞吐 |
| 慢操作持续出现，CPU 较高 | 线程或核数不足 | 增加 `generic.thread.count`，增加节点 |
| 慢操作 + 高 GC | JVM 堆压力 | 增加 `-Xmx`，减少并发任务 |
| `executor.q.operations.size` > 0 | 操作线程池饱和 | 增加 `generic.thread.count` |
| `operations.pending.invocations.percentage` > 10% | 远程调用积压 | 检查网络，增加 `generic.thread.count` |
| WAL 目录持续增长，IMap 操作变慢 | MapStore 写入压力 | 增加 `write-behind-delay-seconds`，增加磁盘 IOPS |
| Checkpoint 耗时 > 60s | 状态数据过大或存储慢 | 减少 Checkpoint 状态大小，优化存储 |