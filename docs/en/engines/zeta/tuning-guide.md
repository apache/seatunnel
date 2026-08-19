---
sidebar_position: 15
---

# Tuning Guide

This article introduces the tuning methods of SeaTunnel Engine to help users optimize the performance and stability of SeaTunnel Engine according to their actual needs.
Before reading this guide, please note that the recommendations here are summarized from real-world usage by most users and may not be suitable for all scenarios. You can adjust them according to your actual situation.

SeaTunnel Engine is a data integration engine running on the [JVM](https://en.wikipedia.org/wiki/Java_virtual_machine), so JVM tuning is also applicable to SeaTunnel Engine and will not be repeated here.

## Cluster Slow Response or Hang

### JVM

If the SeaTunnel Engine cluster responds slowly or hangs, it may be due to insufficient JVM heap memory. You can troubleshoot as follows:

#### Insufficient Heap Memory

##### Troubleshooting Process

1. Check JVM heap memory usage in real time
   Use the `jcmd` command to check JVM heap memory usage, where `<pid>` is the PID of the SeaTunnel Engine process.
   ```bash
   jmap -heap <pid>
   ```
   Example output:
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
   Pay attention to the usage of G1 Old Generation. If the usage rate of Old Generation is close to 100%, it may be caused by insufficient heap memory.
2. Check the logs
   The system will periodically output health monitoring logs. Check the SeaTunnel Engine logs to see if there are frequent Full GCs or long GC pauses, which may be caused by insufficient heap memory.
   Example log:
   ```log
   [] 2025-07-04 16:42:54,818 INFO  [c.h.i.d.HealthMonitor         ] [hz.main.HealthMonitor] - [127.0.0.1]:5801 [seatunnel] [5.1] processors=16, physical.memory.total=31.1G, physical.memory.free=9.7G, swap.space.total=0, swap.space.free=0, heap.memory.used=198.7M, heap.memory.free=15.8G, heap.memory.total=16.0G, heap.memory.max=16.0G, heap.memory.used/total=1.21%, heap.memory.used/max=1.21%, minor.gc.count=2, minor.gc.time=44ms, major.gc.count=0, major.gc.time=0ms, load.process=0.00%, load.system=66.67%, load.systemAverage=5.66, thread.count=118, thread.peakCount=118, cluster.timeDiff=0, event.q.size=0, executor.q.async.size=0, executor.q.client.size=0, executor.q.client.query.size=0, executor.q.client.blocking.size=0, executor.q.query.size=0, executor.q.scheduled.size=0, executor.q.io.size=0, executor.q.system.size=0, executor.q.operations.size=0, executor.q.priorityOperation.size=0, operations.completed.count=13, executor.q.mapLoad.size=0, executor.q.mapLoadAllKeys.size=0, executor.q.cluster.size=0, executor.q.response.size=0, operations.running.count=0, operations.pending.invocations.percentage=0.00%, operations.pending.invocations.count=0, proxy.count=9, clientEndpoint.count=0, connection.active.count=0, client.connection.count=0, connection.count=0
   ```
   Focus on:
    - `heap.memory.used/max`: Heap memory usage rate. If it is close to 100%, it may be due to insufficient heap memory.
    - `major.gc.count` and `major.gc.time`: If Full GC is frequent, it may be caused by insufficient heap memory.
   You can judge whether there are frequent Full GCs or long GC pauses by continuously checking the logs.

##### Solutions

Reduce memory usage at the same time by lowering task concurrency and the number of tasks. If you do need more memory, please refer to [Deployment](deployment.md) for configuring SeaTunnel Engine JVM options to increase memory.

##### Unlimited Memory Usage
1. Generate a memory snapshot

   Sometimes, even with a fixed number of tasks, memory usage keeps increasing, which may be caused by a memory leak in the task. Please dump the corresponding memory snapshot information.
   ```shell
   jmap -dump:live,format=b,file=heap.hprof <pid>
   ```
   Then use tools such as [Eclipse Memory Analyzer](https://www.eclipse.org/mat/) to analyze the memory snapshot and find the cause of the memory leak.
   For users or connectors who are not secondary developers, you can also create an issue and attach the memory snapshot, and we will help you analyze it.

2. Print object occupancy ranking

   Sometimes, generating a memory snapshot may fail due to JVM hang. In this case, you can try to print the object occupancy ranking to check memory usage.
   ```shell
   jmap -histo:live <pid> | head -n 100
   ```
   Similarly, you can analyze the output to find the cause of the memory leak.
   For users or connectors who are not secondary developers, you can also create an issue and attach the object occupancy information, and we will help you analyze it.

#### High CPU Usage

High CPU usage is also a common cause of cluster node hangs, but it is less likely than high memory usage. You can troubleshoot as follows:

##### Troubleshooting Process
1. Check CPU usage
   - Use the `top` or `htop` command to check the CPU usage of the SeaTunnel Engine process.
   - If the CPU usage is close to 100%, it may be due to insufficient CPU resources. If there are multiple cores, consider the usage of all cores.

##### Solutions

If CPU usage is too high, you can try the following solutions:
- Reduce task concurrency and the number of tasks to reduce CPU resource usage.
- Increase the number of cluster nodes to share the CPU resource load.

### Hazelcast

Hazelcast-related configuration is also an important factor affecting the performance of SeaTunnel Engine. You can modify the configuration parameters in the `hazelcast.yaml` series of files. Please refer to [Deployment](deployment.md).
Here are some common tuning parameters:
- `hazelcast.operation.generic.thread.count`: This parameter controls the number of generic operation threads in Hazelcast. SeaTunnel Engine uses this thread for executing RPC requests. You can adjust this parameter according to your actual situation to improve the performance of Hazelcast RPC.
If you frequently see logs like the following and the CPU usage is not very high, try increasing this parameter:
```log
2024-09-03 06:15:45,807 WARN  [.s.i.o.s.SlowOperationDetector] [hz.main.SlowOperationDetectorThread] - [seatunnel-worker-1]:5802 [seatunnel] [5.1] Slow operation detected:
```

## Slow Operation Troubleshooting Cookbook

This section provides a practical step-by-step guide for diagnosing and resolving Hazelcast slow operation warnings in production SeaTunnel Zeta clusters.

### 1. Understanding `SlowOperationDetector` Warnings

Hazelcast's `SlowOperationDetector` monitors the execution time of operations on partition threads. When an operation takes longer than the configured threshold (default: 10 seconds), a warning is logged:

```log
2024-09-03 06:15:45,807 WARN  [.s.i.o.s.SlowOperationDetector] [hz.main.SlowOperationDetectorThread] -
  [seatunnel-worker-1]:5802 [seatunnel] [5.1] Slow operation detected:
  operation=com.hazelcast.map.impl.operation.PutOperation, duration=5234ms, ...
```

**What this means in SeaTunnel Zeta:**
- Hazelcast operations are the backbone of SeaTunnel's distributed coordination — job submission, state synchronization, checkpoint coordination, and IMap read/write all go through Hazelcast operations.
- A slow operation warning indicates that the partition thread is blocked for longer than expected, which can cascade into job submission timeouts, checkpoint failures, or cluster instability.
- The warning itself is a **symptom**, not a root cause. You must identify which layer is causing the delay.

| Symptom | Likely Cause |
|---|---|
| Slow operations during job submission | Master node CPU saturation, insufficient generic operation threads, or large job config serialization |
| Slow operations during checkpoint | Checkpoint storage I/O latency (S3/HDFS), large state size, or network contention |
| Slow operations during IMap access | MapStore disk I/O bottleneck, WAL write pressure, or memory pressure causing GC |
| Slow operations persistent across all workloads | Under-provisioned cluster, network latency between nodes, or JVM GC pauses |

### 2. Diagnosing Latency Sources

Use the following decision tree to narrow down the root cause of slow operations.

#### Step 1: Identify when slow operations occur

```bash
# Check the slow operation log frequency and timing
grep "SlowOperationDetector" $SEATUNNEL_HOME/logs/seatunnel-server.log | tail -50
```

Correlate the timestamps with:
- Job submission events (REST API calls)
- Checkpoint intervals (every 10s by default)
- High-load periods (peak data ingestion)

#### Step 2: Check overall node health

```bash
# Check CPU, memory, and disk I/O
top -bn1 | head -20
iostat -x 1 5
free -h
```

#### Step 3: Isolate the bottleneck layer

**REST submission latency:**
- Symptom: Slow operations appear when jobs are submitted via REST API, and the submitting client experiences long response times.
- Check: `grep "submitJob" $SEATUNNEL_HOME/logs/seatunnel-server.log` — look for elapsed time.
- Common cause: Master node is overloaded with concurrent submissions, or the job configuration is very large (many connectors/transforms).
- Mitigation: Rate-limit concurrent submissions, increase master node resources, or use `hazelcast.operation.generic.thread.count` tuning.

**Master scheduling pressure:**
- Symptom: Slow operations cluster around job lifecycle events (INIT → RUNNING transitions), and the master node CPU is consistently high.
- Check: Health monitor logs on the master node for `executor.q.operations.size` and `operations.pending.invocations.percentage`.
- Common cause: Too many concurrent jobs or pipelines competing for master scheduling threads.
- Mitigation: Reduce concurrent job count, or configure `hazelcast.operation.generic.thread.count` on the master node.

**Worker execution pressure:**
- Symptom: Slow operations on worker nodes, especially during checkpoint coordination.
- Check: Health monitor logs on worker nodes for `executor.q.operations.size` and thread pool saturation.
- Common cause: Workers are CPU-bound or I/O-bound from connector execution, leaving insufficient resources for Hazelcast operations.
- Mitigation: Increase worker nodes, reduce per-worker task concurrency, or tune `hazelcast.operation.generic.thread.count` on worker nodes.

**Checkpoint storage latency:**
- Symptom: Slow operations align with checkpoint intervals, and checkpoint duration exceeds the configured timeout.
- Check: Enable DEBUG logging for `org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinator`, then `grep "pending checkpoint completed" $SEATUNNEL_HOME/logs/seatunnel-server.log | grep -oP 'cost: \d+ms'` to see checkpoint durations.
- If using S3: Run `aws s3api head-object --bucket <bucket> --key <checkpoint-path>` to measure latency, or check CloudWatch S3 metrics (`FirstByteLatency`, `TotalRequestLatency`).
- Common cause: High network latency to S3/HDFS, small files causing many round trips, or S3 throttling.
- Mitigation: See [Section 6](#6-s3-checkpointstate-storage-latency).

**IMap / MapStore latency:**
- Symptom: Slow operations during `PutOperation` or `GetOperation` on IMap keys.
- Check: `du -sh $SEATUNNEL_HOME/imap/wal/` and `du -sh $SEATUNNEL_HOME/imap/maps/` — large WAL directories indicate write pressure.
- Common cause: Disk I/O saturation on the MapStore directory, aggressive WAL write frequency, or disk space exhaustion.
- Mitigation: See [Section 6](#6-s3-checkpointstate-storage-latency), increase `write-behind-delay-seconds`, enable WAL compaction.

### 3. Sizing `hazelcast.operation.generic.thread.count`

The `hazelcast.operation.generic.thread.count` parameter controls the number of threads Hazelcast uses to execute generic operations (including RPC requests, IMap operations, and checkpoint coordination). The correct sizing depends on your deployment mode.

**Configuration location:** `hazelcast.yaml` under the `hazelcast` top-level properties:

```yaml
hazelcast:
  properties:
    hazelcast.operation.generic.thread.count: <number>
```

#### Hybrid Mode (Master + Worker on same node)

In hybrid mode, each node runs both master and worker processes. The generic operation thread pool is shared between master coordination and worker task execution.

| Physical CPU Cores per Node | Recommended `generic.thread.count` |
|---|---|
| 4–8 | 4–8 |
| 8–16 | 8–16 |
| 16–32 | 16–24 |
| 32+ | 24–32 (rarely need more) |

**Rule of thumb:** `generic.thread.count = min(CPU cores, 24)`. Do not exceed the number of physical cores, as oversubscription can cause context switching overhead that worsens latency.

**Warning signs of under-provisioning:**
- `executor.q.operations.size` consistently > 0 in health monitor logs
- `operations.pending.invocations.percentage` > 10%
- Frequent `SlowOperationDetector` warnings during normal job submission

**Warning signs of over-provisioning:**
- CPU usage is high (>80%) but not from application work
- Context switch rate is elevated (`vmstat 1` shows `cs` > 100k/sec)

#### Separated Mode (Master and Worker on separate nodes)

In separated mode, master nodes only handle cluster coordination and job scheduling, while worker nodes only execute tasks. You should size threads differently for each role.

**Master nodes:**
- Master nodes handle job submissions, checkpoint coordination, and IMap operations.
- `generic.thread.count = min(CPU cores, 16)` is usually sufficient.
- Focus on avoiding queuing: if `executor.q.operations.size` grows, increase the thread count.
- Master nodes are typically CPU-light; 4–8 threads on an 8-core master is a reasonable starting point.

**Worker nodes:**
- Worker nodes execute connector tasks and participate in checkpoint coordination via Hazelcast.
- `generic.thread.count = min(CPU cores - reserved_for_connectors, 16)`.
- Reserve at least 2–4 cores for connector execution. For example, on a 16-core worker: `generic.thread.count = 12`.
- Worker nodes are more likely to see slow operation warnings because they are busier with application work.

| Node Role | CPU Cores | Recommended `generic.thread.count` |
|---|---|---|
| Master (separated) | 4–8 | 4–8 |
| Master (separated) | 8–16 | 8–12 |
| Worker (separated) | 8–16 | 4–12 (reserve 2–4 cores for connectors) |
| Worker (separated) | 16–32 | 8–16 (reserve 4–8 cores for connectors) |

### 4. Metrics and Logs to Collect Before Tuning

Before making any configuration changes, collect the following data to establish a baseline.

#### 4.1 Health Monitor Logs

SeaTunnel outputs health monitor logs periodically (every 60 seconds by default). These contain critical metrics:

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

**Key metrics to watch:**
- `executor.q.operations.size`: Pending operations in the generic operation queue. If consistently > 0, increase `generic.thread.count`.
- `operations.pending.invocations.percentage`: Percentage of pending remote invocations. If > 10%, check network latency or increase threads.
- `operations.running.count`: Currently executing operations. High values may indicate long-running operations.
- `heap.memory.used/max`: If > 85%, GC pressure may be causing slow operations.
- `major.gc.count` and `major.gc.time`: Frequent Full GCs cause operation pauses.

#### 4.2 Slow Operation Logs

```bash
# Extract slow operation warnings with their durations
grep "SlowOperationDetector" $SEATUNNEL_HOME/logs/seatunnel-server.log | tail -20
```

#### 4.3 Node Resource Metrics

```bash
# CPU usage per core
mpstat -P ALL 1 5

# Memory usage
free -h

# Disk I/O
iostat -x 1 5

# Network
netstat -i
```

#### 4.4 Cluster Overview

```bash
# Check running jobs
curl http://<master>:8080/running-jobs

# Check finished jobs
curl "http://<master>:8080/finished-jobs/FINISHED?page=1&rows=100"
```

### 5. Configuration Changes: Restart vs. Hot Reload

Not all configuration changes take effect immediately. Use this reference to determine whether a restart is required.

| Configuration | File | Requires Restart? | Notes |
|---|---|---|---|
| `hazelcast.operation.generic.thread.count` | `hazelcast.yaml` | **Yes** (full cluster restart) | Hazelcast thread pools are initialized at startup |
| `hazelcast.operation.call.timeout.millis` | `hazelcast.yaml` | **Yes** (full cluster restart) | Operation timeout is read at member initialization |
| `seatunnel.engine.checkpoint.interval` | `seatunnel.yaml` | **Yes** (node restart) | Takes effect after node restart |
| `seatunnel.engine.checkpoint.timeout` | `seatunnel.yaml` | **Yes** (node restart) | Takes effect after node restart |
| `seatunnel.engine.checkpoint.storage.*` | `seatunnel.yaml` | **Yes** (node restart) | Takes effect after node restart |
| `seatunnel.engine.history-job-expire-minutes` | `seatunnel.yaml` | **Yes** (node restart) | Takes effect after node restart |
| JVM heap size (`-Xmx`, `-Xms`) | JVM options | **Yes** (process restart) | JVM heap is allocated at process start |
| `hazelcast.initial.min.cluster.size` | `hazelcast.yaml` | **Yes** (full cluster restart) | Cluster formation parameters are read at startup |

**Important:** For Hazelcast configuration changes that require a full cluster restart, you must restart all nodes (masters and workers) to ensure consistent configuration across the cluster. Rolling restarts with mixed configurations can cause unpredictable behavior.

### 6. S3 Checkpoint/State Storage Latency

When checkpoint storage is configured with S3, network latency and S3 throttling can become the primary cause of slow operations.

#### 6.1 Diagnosing S3 Latency

**Check S3 endpoint latency from the cluster nodes:**
```bash
# Measure DNS resolution and connection time
curl -w "DNS: %{time_namelookup}s, Connect: %{time_connect}s, TTFB: %{time_starttransfer}s, Total: %{time_total}s\n" \
  -o /dev/null -s https://s3.amazonaws.com

# For S3-compatible storage (MinIO, etc.)
curl -w "DNS: %{time_namelookup}s, Connect: %{time_connect}s, TTFB: %{time_starttransfer}s, Total: %{time_total}s\n" \
  -o /dev/null -s https://<your-s3-endpoint>
```

**Check checkpoint write performance:**
```bash
# Monitor checkpoint duration from logs (requires DEBUG logging for CheckpointCoordinator)
grep "pending checkpoint completed" $SEATUNNEL_HOME/logs/seatunnel-server.log | \
  grep -oP 'cost: \d+ms' | sort -t: -k2 -nr | head -20
```

**Check S3 throttling (AWS):**
```bash
# Check if S3 is throttling requests
aws cloudwatch get-metric-statistics \
  --namespace AWS/S3 \
  --metric-name 5xxErrors \
  --dimensions Name=BucketName,Value=<your-bucket> \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 300 \
  --statistics Sum
```

#### 6.2 Recommended Mitigations

**1. Use S3 endpoint in the same region as your cluster:**
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

**2. Enable S3A fast upload and connection pooling:**
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

**3. Increase S3A retry and timeout settings:**
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

**4. For high-throughput checkpoint workloads, consider using HDFS or local SSD storage for checkpoints and S3 only for long-term backup.**

**5. Enable S3 Transfer Acceleration if cluster is in a different region from the bucket.**

### 7. Kubernetes Deployment Checklist

When deploying SeaTunnel Zeta on Kubernetes, the following checks help prevent slow operation issues.

#### 7.1 Pod Anti-Affinity

Ensure master and worker pods are spread across nodes to avoid resource contention:

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

#### 7.2 Resource Requests and Limits

Set realistic resource requests and limits to avoid CPU throttling:

```yaml
resources:
  requests:
    cpu: "2"
    memory: "4Gi"
  limits:
    cpu: "4"
    memory: "8Gi"
```

**Important:** CPU throttling in Kubernetes (CFS quota) can cause Hazelcast operation timeouts. If you see `hazelcast.operation.call.timeout.millis` exceeded despite low reported CPU usage, check `container_cpu_cfs_throttled_seconds_total` metric.

#### 7.3 Readiness Probes

Configure readiness probes that verify the SeaTunnel REST API is accessible:

```yaml
readinessProbe:
  httpGet:
    path: /running-jobs
    port: 8080
  initialDelaySeconds: 30
  periodSeconds: 10
```

#### 7.4 Graceful Shutdown

Ensure pods have enough time to flush state and leave the cluster:

```yaml
terminationGracePeriodSeconds: 60
```

And in `hazelcast.yaml`:
```yaml
hazelcast:
  shutdown-hook:
    enabled: true
    policy: GRACEFUL
```

#### 7.5 Log Aggregation

Ensure slow operation logs are captured by your log aggregation system:

```yaml
# In your logging configuration
loggers:
  - name: com.hazelcast.spi.impl.operationexecutor.slowoperationdetector.SlowOperationDetector
    level: WARN
```

#### 7.6 Storage for MapStore and WAL

Use persistent volumes for MapStore and WAL directories to survive pod restarts:

```yaml
volumeMounts:
  - name: imap-storage
    mountPath: /tmp/seatunnel/imap
volumes:
  - name: imap-storage
    persistentVolumeClaim:
      claimName: seatunnel-imap-pvc
```

Monitor PVC usage with:
```bash
kubectl exec <pod> -- du -sh /tmp/seatunnel/imap/
```

### Quick Reference Troubleshooting Table

| Observation | Most Likely Cause | First Action |
|---|---|---|
| Slow operations only during job submission | Master CPU or thread saturation | Increase `generic.thread.count` on master, rate-limit submissions |
| Slow operations align with checkpoint intervals | Checkpoint storage I/O latency | Check S3/HDFS latency, adjust `fs.s3a.*` settings |
| Slow operations constant, CPU low | Network latency between nodes | Check inter-node latency, network throughput |
| Slow operations constant, CPU high | Under-provisioned threads or cores | Increase `generic.thread.count`, add nodes |
| Slow operations + high GC | JVM heap pressure | Increase `-Xmx`, reduce concurrent tasks |
| `executor.q.operations.size` > 0 | Operation thread pool saturated | Increase `generic.thread.count` |
| `operations.pending.invocations.percentage` > 10% | Remote invocation backlog | Check network, increase `generic.thread.count` |
| WAL directory growing, slow IMap operations | MapStore write pressure | Increase `write-behind-delay-seconds`, add disk IOPS |
| Checkpoint duration > 60s | Large state or slow storage | Reduce checkpoint state size, optimize storage |