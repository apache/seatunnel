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

### Slow Operation Troubleshooting Cookbook

This cookbook provides a step-by-step troubleshooting guide for diagnosing and resolving slow operations in SeaTunnel Zeta (Hazelcast) deployments. Use this alongside the general tuning guidance above.

---

#### 1. Understanding `SlowOperationDetector` Warnings

Hazelcast's `SlowOperationDetector` monitors the execution time of partition operations (RPC calls) and logs warnings when an operation exceeds a threshold. In SeaTunnel Zeta, these warnings typically indicate one of:

| Symptom | Likely Cause |
|---|---|
| Frequent warnings on **master** node only | Master is overloaded scheduling jobs or handling REST submissions |
| Frequent warnings on **worker** nodes only | Worker execution threads are saturated |
| Warnings on **all nodes** simultaneously | Cluster-wide resource pressure or network latency |
| Warnings mentioning IMap operations | MapStore persistence is slow (disk I/O or S3 latency) |
| Intermittent warnings during checkpoints | Checkpoint storage I/O is bottlenecked |

The default slow operation threshold is `hazelcast.slow.operation.detector.threshold.millis=10000` (10 seconds).

---

#### 2. Diagnosing the Source of Latency

Use the following decision tree to isolate the bottleneck:

##### Step 1: Check REST Submission Latency

```bash
# Time a job submission via REST API
time curl -X POST http://<master>:8080/submit-job \
  -H "Content-Type: application/json" \
  -d @job-config.json
```

- If submission takes > 5 seconds, the master is under scheduling pressure.
- **Mitigation:** Increase `hazelcast.operation.generic.thread.count` on the master node, or reduce concurrent job submissions.

##### Step 2: Check Master Scheduling Pressure

```bash
# Query event queue sizes via health monitor logs or REST API v2
curl "http://<master>:8080/overview"
```

Key indicators in health monitoring logs:

| Metric | Healthy Range | Warning Threshold |
|---|---|---|
| `executor.q.operations.size` | 0–100 | > 500 |
| `executor.q.priorityOperation.size` | 0–50 | > 200 |
| `event.q.size` | 0–10 | > 100 |
| `operations.pending.invocations.count` | 0–50 | > 200 |

##### Step 3: Check Worker Execution Pressure

```bash
# Check worker metrics
curl "http://<worker>:5802/health/check"
```

- High `executor.q.client.size` on a worker indicates that a task on that worker is generating more data than it can process downstream.
- **Mitigation:** Increase worker parallelism, add more worker nodes, or reduce source concurrency.

##### Step 4: Check Checkpoint Storage Latency

```bash
# For S3 checkpoint storage, measure PUT latency
time aws s3 cp test-file s3://seatunnel-checkpoint-bucket/test/
```

- Checkpoint storage latency > 1 second per operation can cascade into cluster-wide slowdowns.
- **Mitigation:** See Section 6 (S3 latency checks).

##### Step 5: Check IMap/MapStore Latency

```bash
# Check MapStore base directory disk usage and I/O
du -sh /tmp/seatunnel/imap/
iostat -x 1 5
```

- If MapStore base directory is on a slow disk or network mount, IMap operations will be slow.
- **Mitigation:** Move MapStore to a fast local SSD or increase `hazelcast.fs.write-behind-delay-seconds`.

---

#### 3. Sizing `hazelcast.operation.generic.thread.count`

This parameter controls the number of threads available for executing Hazelcast RPC operations (job submission, state queries, IMap operations). The correct value depends on your deployment mode.

##### Hybrid Mode (master = worker)

In hybrid mode, each node handles both scheduling and execution. Threads should be split between scheduling and execution tasks.

```yaml
# hazelcast.yaml — Hybrid mode recommendation
hazelcast:
  operation:
    generic:
      thread:
        count: <cores * 2>
```

| CPU Cores per Node | Recommended `thread.count` |
|---|---|
| 4 | 8 |
| 8 | 12–16 |
| 16 | 16–24 |
| 32+ | 24–32 (diminishing returns beyond ~32) |

##### Separated Mode (dedicated master + workers)

In separated mode, masters only handle scheduling and cluster management; workers only execute tasks.

```yaml
# Master node hazelcast.yaml — Separated mode
hazelcast:
  operation:
    generic:
      thread:
        count: <cores * 1>

# Worker node hazelcast.yaml — Separated mode  
hazelcast:
  operation:
    generic:
      thread:
        count: <cores * 2>
```

**Rule of thumb:** Start with `thread.count = CPU cores × 2` for workers, `CPU cores × 1` for masters. Increase only if SlowOperationDetector warnings persist and CPU usage is below 70%.

---

#### 4. Metrics and Logs to Collect Before Tuning

Always collect the following baseline data before making configuration changes:

**A. Health Monitor Logs (most recent 5 minutes)**
```bash
grep "HealthMonitor" $SEATUNNEL_HOME/logs/seatunnel-server.log | tail -20
```

**B. Hazelcast Slow Operation Logs**
```bash
grep "SlowOperationDetector" $SEATUNNEL_HOME/logs/seatunnel-server.log | tail -50
```

**C. Node Resource Metrics (per-node)**
```bash
# CPU and memory
top -bn1 | head -5
# JVM heap
jcmd <pid> GC.heap_info
# Disk I/O
iostat -x 1 3
# Network
netstat -an | grep -c ESTABLISHED
```

**D. Cluster Overview via REST API v2**
```bash
curl "http://<master>:8080/overview" | jq .
```

---

#### 5. Configuration Changes: Restart vs. Hot Reload

Not all Hazelcast changes require a restart. Use this table:

| Configuration Change | Requires Restart? | Notes |
|---|---|---|
| `hazelcast.operation.generic.thread.count` | **Yes — full cluster restart** | Affects thread pool sizing at startup |
| `hazelcast.operation.call.timeout.millis` | **No — hot reload** | Can be changed in running cluster |
| `hazelcast.slow.operation.detector.threshold.millis` | **No — hot reload** | Takes effect within seconds |
| `hazelcast.fs.write-behind-delay-seconds` | **Yes — node restart** | MapStore configuration change |
| `hazelcast.fs.compaction-threshold` | **Yes — node restart** | Requires MapStore re-initialization |
| `seatunnel.engine.checkpoint.interval` | **No — per-job config** | Set in `seatunnel.yaml` per job |
| `seatunnel.engine.checkpoint.storage.*` | **Yes — full cluster restart** | Storage backend changes require restart |
| `hazelcast.initial-cluster-size` | **Yes — full cluster restart** | Cluster formation setting |
| JVM heap settings (`-Xmx`, `-Xms`) | **Yes — process restart** | JVM-level changes |

---

#### 6. S3 Checkpoint and State Storage Latency

When using S3 as the checkpoint or state storage backend, latency is a common source of slow operations.

##### Diagnosis

```bash
# Measure S3 endpoint latency
curl -w "@curl-format.txt" -o /dev/null -s https://s3.<region>.amazonaws.com

# Check checkpoint storage directory size
aws s3 ls --recursive --summarize s3://<bucket>/seatunnel/checkpoint/ | tail -2

# Monitor with CloudWatch metrics for S3 request latency
aws cloudwatch get-metric-statistics \
  --namespace AWS/S3 \
  --metric-name TotalRequestLatency \
  --dimensions Name=BucketName,Value=<bucket> \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 300 \
  --statistics Average
```

##### Mitigations

| Issue | Mitigation |
|---|---|
| High PUT latency | Enable S3 Transfer Acceleration on the bucket |
| High GET latency (checkpoint restore) | Use S3 VPC Endpoint to eliminate internet routing |
| Large checkpoint files | Increase `checkpoint.interval` to reduce frequency; enable compression |
| Cross-region latency | Ensure S3 bucket is in the same region as the cluster |
| Throttling | Distribute checkpoints across prefix partitions; request limit increase |

##### Recommended S3 Configuration

```yaml
seatunnel:
  engine:
    checkpoint:
      storage:
        type: hdfs
        plugin-config:
          namespace: /seatunnel/checkpoint/
          fs.s3a.endpoint: https://s3.<region>.amazonaws.com
          fs.s3a.connection.maximum: 200
          fs.s3a.threads.max: 40
          fs.s3a.connection.timeout: 30000
          fs.s3a.fast.upload: true
          fs.s3a.fast.upload.buffer: disk
```

---

#### 7. Kubernetes Deployment Checklist

When deploying SeaTunnel Zeta on Kubernetes, verify the following:

- [ ] **Resource requests and limits are set** — Ensure CPU and memory requests match your `hazelcast.operation.generic.thread.count` sizing.
  ```yaml
  resources:
    requests:
      cpu: "4"
      memory: "8Gi"
    limits:
      cpu: "8"
      memory: "16Gi"
  ```

- [ ] **Pod anti-affinity is configured** — Prevent multiple master nodes from scheduling on the same physical node.
  ```yaml
  affinity:
    podAntiAffinity:
      requiredDuringSchedulingIgnoredByExecution:
        - labelSelector:
            matchExpressions:
              - key: app
                operator: In
                values: [seatunnel-master]
          topologyKey: kubernetes.io/hostname
  ```

- [ ] **Network policy allows cluster communication** — Ensure all nodes can reach each other on port 5801 (Hazelcast cluster) and 5802 (member endpoint).
  ```yaml
  # Required ports
  # 5801 — Hazelcast cluster communication
  # 5802 — Member REST endpoint
  # 8080 — Master REST API
  ```

- [ ] **Persistent volume for MapStore** — Use a fast `ReadWriteOnce` volume backed by SSD for the MapStore base directory.
  ```yaml
  volumeMounts:
    - name: imap-storage
      mountPath: /tmp/seatunnel/imap
  ```

- [ ] **Readiness probe checks Hazelcast membership** — Do not mark a node as ready until it joins the cluster.
  ```yaml
  readinessProbe:
    httpGet:
      path: /health/check
      port: 5802
    initialDelaySeconds: 30
    periodSeconds: 10
  ```

- [ ] **Graceful shutdown** — Set `terminationGracePeriodSeconds` high enough for checkpoint flush + WAL compaction.
  ```yaml
  terminationGracePeriodSeconds: 120
  ```

- [ ] **Log aggregation** — Ensure Hazelcast SlowOperationDetector logs are captured by your logging stack (ELK, Loki, etc.) for proactive alerting.

---

#### Troubleshooting Quick Reference

| Problem | First Check | Common Fix |
|---|---|---|
| Job submission slow | Master CPU, REST API latency | Increase master thread count, stagger submissions |
| SlowOperationDetector on master | `executor.q.operations.size` in health logs | Increase `thread.count` on master |
| SlowOperationDetector on worker | `executor.q.client.size`, worker CPU | Increase worker threads or add workers |
| High checkpoint duration | Checkpoint storage I/O latency | Switch to faster storage, increase interval |
| S3 checkpoint timeout | S3 endpoint latency, network path | Enable S3 Transfer Acceleration, VPC endpoint |
| IMap operations slow | MapStore disk I/O | Move to SSD, increase write-behind delay |
| Cluster instability after config change | Restart required | Consult the restart vs. hot-reload table above |
