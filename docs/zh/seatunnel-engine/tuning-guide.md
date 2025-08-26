---
sidebar_position: 15
---

# 调优指南

本文为大家介绍 SeaTunnel Engine 的调优方法，帮助用户根据实际需求优化 SeaTunnel Engine 的性能和稳定性。
阅读次篇前请知晓，当前指南结合的是大部分用户的真实使用情况总结而成，可能并不适用于所有场景，用户可以根据实际情况进行调整。

SeaTunnel Engine 是基于 [JVM] (https://zh.wikipedia.org/wiki/Java%E8%99%9A%E6%8B%9F%E6%9C%BA) 运行的数据集成引擎，所以 JVM 部分的调优对 SeaTunnel Engine 同样适用，这里就不再赘述。

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
