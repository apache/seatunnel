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