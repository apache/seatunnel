---
sidebar_position: 13
---

# Telemetry

通过 `Prometheus-exports` 集成 `Metrices` 可以更好地与相关的监控平台（如 Prometheus 和 Grafana）无缝衔接，提高对 SeaTunnel
集群的监控和告警能力。

您可以在 `seatunnel.yaml` 文件中配置监控的相关设置。

以下是一个声明式配置的示例。

```yaml
seatunnel:
  engine:
    telemetry:
      metric:
        enabled: true 
```

## 指标

Prometheus 的[指标文本](./telemetry/metrics.txt)，获取方式为 `http://{instanceHost}:5801/hazelcast/rest/instance/metrics`。

OpenMetrics 的[指标文本](./telemetry/openmetrics.txt)
，获取方式为 `http://{instanceHost}:5801/hazelcast/rest/instance/openmetrics`。

可用的指标包括以下类别。

注意：所有指标都有相同的标签名 `cluster`，其值为 `hazelcast.cluster-name` 的配置。

### 节点指标

| MetricName                                | Type  | Labels                                                                                                     | 描述                                  |
|-------------------------------------------|-------|------------------------------------------------------------------------------------------------------------|-------------------------------------|
| cluster_info                              | Gauge | **hazelcastVersion**，hazelcast 的版本。**master**，seatunnel 主地址。                                               | 集群信息                                |
| cluster_time                              | Gauge | **hazelcastVersion**，hazelcast 的版本。                                                                        | 集群时间                                |
| node_count                                | Gauge | -                                                                                                          | 集群节点总数                              |
| node_state                                | Gauge | **address**，服务器实例地址，例如："127.0.0.1:5801"                                                                    | seatunnel 节点是否正常                    |
| hazelcast_executor_executedCount          | Gauge | **type**，执行器的类型，包括："async" "client" "clientBlocking" "clientQuery" "io" "offloadable" "scheduled" "system" | seatunnel 集群节点的 hazelcast 执行器执行次数   |
| hazelcast_executor_isShutdown             | Gauge | **type**，执行器的类型，包括："async" "client" "clientBlocking" "clientQuery" "io" "offloadable" "scheduled" "system" | seatunnel 集群节点的 hazelcast 执行器是否关闭   |
| hazelcast_executor_isTerminated           | Gauge | **type**，执行器的类型，包括："async" "client" "clientBlocking" "clientQuery" "io" "offloadable" "scheduled" "system" | seatunnel 集群节点的 hazelcast 执行器是否终止   |
| hazelcast_executor_maxPoolSize            | Gauge | **type**，执行器的类型，包括："async" "client" "clientBlocking" "clientQuery" "io" "offloadable" "scheduled" "system" | seatunnel 集群节点的 hazelcast 执行器最大池大小  |
| hazelcast_executor_poolSize               | Gauge | **type**，执行器的类型，包括："async" "client" "clientBlocking" "clientQuery" "io" "offloadable" "scheduled" "system" | seatunnel 集群节点的 hazelcast 执行器当前池大小  |
| hazelcast_executor_queueRemainingCapacity | Gauge | **type**，执行器的类型，包括："async" "client" "clientBlocking" "clientQuery" "io" "offloadable" "scheduled" "system" | seatunnel 集群节点的 hazelcast 执行器剩余队列容量 |
| hazelcast_executor_queueSize              | Gauge | **type**，执行器的类型，包括："async" "client" "clientBlocking" "clientQuery" "io" "offloadable" "scheduled" "system" | seatunnel 集群节点的 hazelcast 执行器当前队列大小 |
| hazelcast_partition_partitionCount        | Gauge | -                                                                                                          | seatunnel 集群节点的分区数量                 |
| hazelcast_partition_activePartition       | Gauge | -                                                                                                          | seatunnel 集群节点的活跃分区数量               |
| hazelcast_partition_isClusterSafe         | Gauge | -                                                                                                          | 分区是否安全                              |
| hazelcast_partition_isLocalMemberSafe     | Gauge | -                                                                                                          | 本地成员是否安全                            |

### 线程池状态

| MetricName                          | Type    | Labels                                  | 描述                             |
|-------------------------------------|---------|-----------------------------------------|--------------------------------|
| job_thread_pool_activeCount         | Gauge   | **address**，服务器实例地址，例如："127.0.0.1:5801" | seatunnel 协调器作业执行器缓存线程池的活动线程数  |
| job_thread_pool_corePoolSize        | Gauge   | **address**，服务器实例地址，例如："127.0.0.1:5801" | seatunnel 协调器作业执行器缓存线程池的核心池大小  |
| job_thread_pool_maximumPoolSize     | Gauge   | **address**，服务器实例地址，例如："127.0.0.1:5801" | seatunnel 协调器作业执行器缓存线程池的最大池大小  |
| job_thread_pool_poolSize            | Gauge   | **address**，服务器实例地址，例如："127.0.0.1:5801" | seatunnel 协调器作业执行器缓存线程池的当前池大小  |
| job_thread_pool_queueTaskCount      | Gauge   | **address**，服务器实例地址，例如："127.0.0.1:5801" | seatunnel 协调器作业执行器缓存线程池的队列任务数  |
| job_thread_pool_completedTask_total | Counter | **address**，服务器实例地址，例如："127.0.0.1:5801" | seatunnel 协调器作业执行器缓存线程池的完成任务数  |
| job_thread_pool_task_total          | Counter | **address**，服务器实例地址，例如："127.0.0.1:5801" | seatunnel 协调器作业执行器缓存线程池的总任务数   |
| job_thread_pool_rejection_total     | Counter | **address**，服务器实例地址，例如："127.0.0.1:5801" | seatunnel 协调器作业执行器缓存线程池的拒绝任务总数 |

### 作业信息详细

| MetricName | Type  | Labels                                                                                                  | 描述                  |
|------------|-------|---------------------------------------------------------------------------------------------------------|---------------------|
| job_count  | Gauge | **type**，作业的类型，包括："canceled" "cancelling" "created" "failed" "failing" "finished" "running" "scheduled" | seatunnel 集群的所有作业计数 |

### JVM 指标

| MetricName                                 | Type    | Labels                                                                                                        | 描述                                     |
|--------------------------------------------|---------|---------------------------------------------------------------------------------------------------------------|----------------------------------------|
| jvm_threads_current                        | Gauge   | -                                                                                                             | JVM 的当前线程数                             |
| jvm_threads_daemon                         | Gauge   | -                                                                                                             | JVM 的守护线程数                             |
| jvm_threads_peak                           | Gauge   | -                                                                                                             | JVM 的峰值线程数                             |
| jvm_threads_started_total                  | Counter | -                                                                                                             | JVM 启动的线程总数                            |
| jvm_threads_deadlocked                     | Gauge   | -                                                                                                             | JVM 线程在等待获取对象监视器或拥有的可拥有同步器时处于死锁状态的周期数  |
| jvm_threads_deadlocked_monitor             | Gauge   | -                                                                                                             | JVM 线程在等待获取对象监视器时处于死锁状态的周期数            |
| jvm_threads_state                          | Gauge   | **state**，JVM 线程的状态，包括："NEW" "TERMINATED" "RUNNABLE" "BLOCKED" "WAITING" "TIMED_WAITING" "UNKNOWN"            | 按状态分类的线程当前计数                           |
| jvm_classes_currently_loaded               | Gauge   | -                                                                                                             | JVM 中当前加载的类的数量                         |
| jvm_classes_loaded_total                   | Counter | -                                                                                                             | 自 JVM 开始执行以来加载的类的总数                    |
| jvm_classes_unloaded_total                 | Counter | -                                                                                                             | 自 JVM 开始执行以来卸载的类的总数                    |
| jvm_memory_pool_allocated_bytes_total      | Counter | **pool**，包括："Code Cache" "PS Eden Space" "PS Old Gen" "PS Survivor Space" "Compressed Class Space" "Metaspace" | 在给定 JVM 内存池中分配的总字节数。仅在垃圾收集后更新，而不是持续更新。 |
| jvm_gc_collection_seconds_count            | Summary | **gc**，包括："PS Scavenge" "PS MarkSweep"                                                                        | 在给定 JVM 垃圾收集器中花费的时间（以秒为单位）             |
| jvm_gc_collection_seconds_sum              | Summary | **gc**，包括："PS Scavenge" "PS MarkSweep"                                                                        | 在给定 JVM 垃圾收集器中花费的时间（以秒为单位）             
| jvm_info                                   | Gauge   | **runtime**，例如：“Java(TM) SE Runtime Environment”。**供应商**，例如：“Oracle Corporation”。**版本**，例如：“1.8.0_212-b10”    | VM 版本信息                                |
| process_cpu_seconds_total                  | Counter | -                                                                                                             | 用户和系统 CPU 时间总计，以秒为单位                   |
| process_start_time_seconds                 | Gauge   | -                                                                                                             | 进程自 Unix 纪元以来的启动时间，以秒为单位               |
| process_open_fds                           | Gauge   | -                                                                                                             | 打开的文件描述符数量                             |
| process_max_fds                            | Gauge   | -                                                                                                             | 最大打开的文件描述符数量                           |
| jvm_memory_objects_pending_finalization    | Gauge   | -                                                                                                             | 等待最终化队列中的对象数量                          |
| jvm_memory_bytes_used                      | Gauge   | **area**，包括： "heap" "noheap"                                                                                  | 给定 JVM 内存区域使用的字节数                      |
| jvm_memory_bytes_committed                 | Gauge   | **area**，包括： "heap" "noheap"                                                                                  | 给定 JVM 内存区域的提交字节数                      |
| jvm_memory_bytes_max                       | Gauge   | **area**，包括： "heap" "noheap"                                                                                  | 给定 JVM 内存区域的最大字节数                      |
| jvm_memory_bytes_init                      | Gauge   | **area**，包括： "heap" "noheap"                                                                                  | 给定 JVM 内存区域的初始字节数                      |
| jvm_memory_pool_bytes_used                 | Gauge   | **pool**，包括："Code Cache" "PS Eden Space" "PS Old Ge" "PS Survivor Space" "Compressed Class Space" "Metaspace" | 给定 JVM 内存池使用的字节数                       |
| jvm_memory_pool_bytes_committed            | Gauge   | **pool**，包括："Code Cache" "PS Eden Space" "PS Old Ge" "PS Survivor Space" "Compressed Class Space" "Metaspace"    | 给定 JVM 内存池的提交字节数                       |
| jvm_memory_pool_bytes_max                  | Gauge   | **pool**，包括："Code Cache" "PS Eden Space" "PS Old Ge" "PS Survivor Space" "Compressed Class Space" "Metaspace"    | 给定 JVM 内存池的最大字节数                       |
| jvm_memory_pool_bytes_init                 | Gauge   | **pool**，包括："Code Cache" "PS Eden Space" "PS Old Ge" "PS Survivor Space" "Compressed Class Space" "Metaspace"    | 给定 JVM 内存池的初始字节数                       |
| jvm_memory_pool_allocated_bytes_created    | Gauge   | **pool**，包括："Code Cache" "PS Eden Space" "PS Old Ge" "PS Survivor Space" "Compressed Class Space" "Metaspace"    | 给定 JVM 内存池中创建的总字节数。仅在 GC 后更新，而不是持续更新   |
| jvm_memory_pool_collection_used_bytes      | Gauge   | **pool**，包括："PS Eden Space" "PS Old Ge" "PS Survivor Space"                                                      | 给定 JVM 内存池在最后一次回收后的使用字节数               |
| jvm_memory_pool_collection_committed_bytes | Gauge   | **pool**，包括："PS Eden Space" "PS Old Ge" "PS Survivor Space"                                                      | 给定 JVM 内存池在最后一次回收后的提交字节数               |
| jvm_memory_pool_collection_max_bytes       | Gauge   | **pool**，包括："PS Eden Space" "PS Old Ge" "PS Survivor Space"                                                      | 给定 JVM 内存池在最后一次回收后的最大字节数               |
| jvm_memory_pool_collection_init_bytes      | Gauge   | **pool**，包括："PS Eden Space" "PS Old Ge" "PS Survivor Space"                                                      | 给定 JVM 内存池在最后一次回收后的初始字节数               |
| jvm_buffer_pool_used_bytes                 | Gauge   | **pool**，包括："direct" "mapped"                                                                                    | 给定 JVM 缓冲池使用的字节数                       |
| jvm_buffer_pool_capacity_bytes             | Gauge   | **pool**，包括："direct" "mapped"                                                                                    | 给定 JVM 缓冲池的字节容量                        |
| jvm_buffer_pool_used_buffers               | Gauge   | **pool**，包括："direct" "mapped"                                                                                     | 给定 JVM 缓冲池使用的缓冲区                       |

## 通过 Prometheus 和 Grafana 进行集群监控

### 安装 Prometheus

有关如何设置 Prometheus 服务器的指南，请访问
[安装](https://prometheus.io/docs/prometheus/latest/installation)

### 配置 Prometheus

将 SeaTunnel 实例指标导出添加到 `/etc/prometheus/prometheus.yaml` 中。例如：

```yaml
global:
  # 从此作业中抓取目标的频率。
  scrape_interval: 15s
scrape_configs:
  # 默认分配给抓取指标的作业名称。
  - job_name: 'seatunnel'
    scrape_interval: 5s
    # 指标导出路径 
    metrics_path: /hazelcast/rest/instance/metrics
    # 此作业静态配置的目标列表。
    static_configs:
      # 静态配置中指定的目标。
      - targets: [ 'localhost:5801' ]
      # 为从目标抓取的所有指标分配的标签。
      # labels: [<labelName>:<labelValue>]
```

### 安装 Grafana

有关如何设置 Grafana 服务器的指南，请访问
[安装](https://grafana.com/docs/grafana/latest/setup-grafana/installation)

#### 监控仪表板

- 在 Grafana 中添加 Prometheus 数据源。
- 将 `Seatunnel Cluster` 监控仪表板导入到 Grafana 中，使用 [仪表板 JSON](./telemetry/grafana-dashboard.json)。

监控[效果图](../../images/grafana.png)