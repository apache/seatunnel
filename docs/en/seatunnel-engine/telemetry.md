---
sidebar_position: 13
---

# Telemetry

Integrating `Metrices` through `Prometheus-exports` can better seamlessly connect to related monitoring platforms such
as Prometheus and Grafana, improving the ability to monitor and alarm of the SeaTunnel cluster.

You can configure telemetry's configurations in the `seatunnel.yaml` file.

The following is an example declarative configuration.

```yaml
seatunnel:
  engine:
    telemetry:
      metric:
        enabled: true # Whether open metrics export
```

## Metrics

The [metric text of prometheus](./telemetry/metrics.txt),which get
from `http://{instanceHost}:5801/hazelcast/rest/instance/metrics`.

The [metric text of openMetrics](./telemetry/openmetrics.txt),which get
from `http://{instanceHost}:5801/hazelcast/rest/instance/openmetrics`.

Available metrics include the following categories.

Note: All metrics both have the same labelName `cluster`, that's value is the config of `hazelcast.cluster-name`.

### Node Metrics

| MetricName                                | Type  | Labels                                                                                                                             | DESCRIPTION                                                             |
|-------------------------------------------|-------|------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------|
| cluster_info                              | Gauge | **hazelcastVersion**, the version of hazelcast. **master**, seatunnel master address.                                              | Cluster info                                                            |
| cluster_time                              | Gauge | **hazelcastVersion**, the version of hazelcast.                                                                                    | Cluster time                                                            |
| node_count                                | Gauge | -                                                                                                                                  | Cluster node total count                                                |
| node_state                                | Gauge | **address**, server instance address,for example: "127.0.0.1:5801"                                                                 | Whether is up of seatunnel node                                         |
| hazelcast_executor_executedCount          | Gauge | **type**, the type of executor, including: "async" "client" "clientBlocking" "clientQuery" "io" "offloadable" "scheduled" "system" | The hazelcast executor executedCount of seatunnel cluster node          |
| hazelcast_executor_isShutdown             | Gauge | **type**, the type of executor, including: "async" "client" "clientBlocking" "clientQuery" "io" "offloadable" "scheduled" "system" | The hazelcast executor isShutdown of seatunnel cluster node             |
| hazelcast_executor_isTerminated           | Gauge | **type**, the type of executor, including: "async" "client" "clientBlocking" "clientQuery" "io" "offloadable" "scheduled" "system" | The hazelcast executor isTerminated of seatunnel cluster node           |
| hazelcast_executor_maxPoolSize            | Gauge | **type**, the type of executor, including: "async" "client" "clientBlocking" "clientQuery" "io" "offloadable" "scheduled" "system" | The hazelcast executor maxPoolSize of seatunnel cluster node            |
| hazelcast_executor_poolSize               | Gauge | **type**, the type of executor, including: "async" "client" "clientBlocking" "clientQuery" "io" "offloadable" "scheduled" "system" | The hazelcast executor poolSize of seatunnel cluster node               |
| hazelcast_executor_queueRemainingCapacity | Gauge | **type**, the type of executor, including: "async" "client" "clientBlocking" "clientQuery" "io" "offloadable" "scheduled" "system" | The hazelcast executor queueRemainingCapacity of seatunnel cluster node |
| hazelcast_executor_queueSize              | Gauge | **type**, the type of executor, including: "async" "client" "clientBlocking" "clientQuery" "io" "offloadable" "scheduled" "system" | The hazelcast executor queueSize of seatunnel cluster node              |
| hazelcast_partition_partitionCount        | Gauge | -                                                                                                                                  | The partitionCount of seatunnel cluster node                            |
| hazelcast_partition_activePartition       | Gauge | -                                                                                                                                  | The activePartition of seatunnel cluster node                           |
| hazelcast_partition_isClusterSafe         | Gauge | -                                                                                                                                  | Weather is cluster safe of partition                                    |
| hazelcast_partition_isLocalMemberSafe     | Gauge | -                                                                                                                                  | Weather is local member safe of partition                               |

### Thread Pool Status

| MetricName                          | Type    | Labels                                                             | DESCRIPTION                                                                    |
|-------------------------------------|---------|--------------------------------------------------------------------|--------------------------------------------------------------------------------|
| job_thread_pool_activeCount         | Gauge   | **address**, server instance address,for example: "127.0.0.1:5801" | The activeCount of seatunnel coordinator job's executor cached thread pool     |
| job_thread_pool_corePoolSize        | Gauge   | **address**, server instance address,for example: "127.0.0.1:5801" | The corePoolSize of seatunnel coordinator job's executor cached thread pool    |
| job_thread_pool_maximumPoolSize     | Gauge   | **address**, server instance address,for example: "127.0.0.1:5801" | The maximumPoolSize of seatunnel coordinator job's executor cached thread pool |
| job_thread_pool_poolSize            | Gauge   | **address**, server instance address,for example: "127.0.0.1:5801" | The poolSize of seatunnel coordinator job's executor cached thread pool        |
| job_thread_pool_queueTaskCount      | Gauge   | **address**, server instance address,for example: "127.0.0.1:5801" | The queueTaskCount of seatunnel coordinator job's executor cached thread pool  |
| job_thread_pool_completedTask_total | Counter | **address**, server instance address,for example: "127.0.0.1:5801" | The completedTask of seatunnel coordinator job's executor cached thread pool   |
| job_thread_pool_task_total          | Counter | **address**, server instance address,for example: "127.0.0.1:5801" | The taskCount of seatunnel coordinator job's executor cached thread pool       |
| job_thread_pool_rejection_total     | Counter | **address**, server instance address,for example: "127.0.0.1:5801" | The rejectionCount of seatunnel coordinator job's executor cached thread pool  |                                                                        |

### Job info detail

| MetricName | Type  | Labels                                                                                                                      | DESCRIPTION                         |
|------------|-------|-----------------------------------------------------------------------------------------------------------------------------|-------------------------------------|
| job_count  | Gauge | **type**, the type of job, including: "canceled" "cancelling" "created" "failed" "failing" "finished" "running" "scheduled" | All job counts of seatunnel cluster |

### JVM Metrics

| MetricName                                 | Type    | Labels                                                                                                                                                | DESCRIPTION                                                                                            |
|--------------------------------------------|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------|
| jvm_threads_current                        | Gauge   | -                                                                                                                                                     | Current thread count of a JVM                                                                          |
| jvm_threads_daemon                         | Gauge   | -                                                                                                                                                     | Daemon thread count of a JVM                                                                           |
| jvm_threads_peak                           | Gauge   | -                                                                                                                                                     | Peak thread count of a JVM                                                                             |
| jvm_threads_started_total                  | Counter | -                                                                                                                                                     | Started thread count of a JVM                                                                          |
| jvm_threads_deadlocked                     | Gauge   | -                                                                                                                                                     | Cycles of JVM-threads that are in deadlock waiting to acquire object monitors or ownable synchronizers |
| jvm_threads_deadlocked_monitor             | Gauge   | -                                                                                                                                                     | Cycles of JVM-threads that are in deadlock waiting to acquire object monitors                          |
| jvm_threads_state                          | Gauge   | **state**, the state of jvm thread, including: "NEW" "TERMINATED" "RUNNABLE" "BLOCKED" "WAITING" "TIMED_WAITING" "UNKNOWN"                            | Current count of threads by state                                                                      |
| jvm_classes_currently_loaded               | Gauge   | -                                                                                                                                                     | The number of classes that are currently loaded in the JVM                                             |
| jvm_classes_loaded_total                   | Counter | -                                                                                                                                                     | The total number of classes that have been loaded since the JVM has started execution                  |
| jvm_classes_unloaded_total                 | Counter | -                                                                                                                                                     | The total number of classes that have been unloaded since the JVM has started execution                |
| jvm_memory_pool_allocated_bytes_total      | Counter | **pool**,including: "Code Cache" "PS Eden Space" "PS Old Ge" "PS Survivor Space" "Compressed Class Space" "Metaspace"                                 | Total bytes allocated in a given JVM memory pool. Only updated after GC, not continuously              |
| jvm_gc_collection_seconds_count            | Summary | **gc**,including: "PS Scavenge" "PS MarkSweep"                                                                                                        | Time spent in a given JVM garbage collector in seconds                                                 |
| jvm_gc_collection_seconds_sum              | Summary | **gc**,including: "PS Scavenge" "PS MarkSweep"                                                                                                        | Time spent in a given JVM garbage collector in seconds                                                 |
| jvm_info                                   | Gauge   | **runtime**, for example: "Java(TM) SE Runtime Environment". **vendor**, for example: "Oracle Corporation". **version** ,for example: "1.8.0_212-b10" | VM version info                                                                                        |
| process_cpu_seconds_total                  | Counter | -                                                                                                                                                     | Total user and system CPU time spent in seconds                                                        |
| process_start_time_seconds                 | Gauge   | -                                                                                                                                                     | Start time of the process since unix epoch in seconds                                                  |
| process_open_fds                           | Gauge   | -                                                                                                                                                     | Number of open file descriptors                                                                        |
| process_max_fds                            | Gauge   | -                                                                                                                                                     | Maximum number of open file descriptors                                                                |
| jvm_memory_objects_pending_finalization    | Gauge   | -                                                                                                                                                     | The number of objects waiting in the finalizer queue                                                   |
| jvm_memory_bytes_used                      | Gauge   | **area**, including: "heap" "noheap"                                                                                                                  | Used bytes of a given JVM memory area                                                                  |
| jvm_memory_bytes_committed                 | Gauge   | **area**, including: "heap" "noheap"                                                                                                                  | Committed (bytes) of a given JVM memory area                                                           |
| jvm_memory_bytes_max                       | Gauge   | **area**, including:"heap" "noheap"                                                                                                                   | Max (bytes) of a given JVM memory area                                                                 |
| jvm_memory_bytes_init                      | Gauge   | **area**, including:"heap" "noheap"                                                                                                                   | Initial bytes of a given JVM memory area                                                               |
| jvm_memory_pool_bytes_used                 | Gauge   | **pool**, including: "Code Cache" "PS Eden Space" "PS Old Ge" "PS Survivor Space" "Compressed Class Space" "Metaspace"                                | Used bytes of a given JVM memory pool                                                                  |
| jvm_memory_pool_bytes_committed            | Gauge   | **pool**, including: "Code Cache" "PS Eden Space" "PS Old Ge" "PS Survivor Space" "Compressed Class Space" "Metaspace"                                | Committed bytes of a given JVM memory pool                                                             |
| jvm_memory_pool_bytes_max                  | Gauge   | **pool**, including: "Code Cache" "PS Eden Space" "PS Old Ge" "PS Survivor Space" "Compressed Class Space" "Metaspace"                                | Max bytes of a given JVM memory pool                                                                   |
| jvm_memory_pool_bytes_init                 | Gauge   | **pool**, including: "Code Cache" "PS Eden Space" "PS Old Ge" "PS Survivor Space" "Compressed Class Space" "Metaspace"                                | Initial bytes of a given JVM memory pool                                                               |
| jvm_memory_pool_allocated_bytes_created    | Gauge   | **pool**, including: "Code Cache" "PS Eden Space" "PS Old Ge" "PS Survivor Space" "Compressed Class Space" "Metaspace"                                | Total bytes allocated in a given JVM memory pool. Only updated after GC, not continuously              |
| jvm_memory_pool_collection_used_bytes      | Gauge   | **pool**, including: "PS Eden Space" "PS Old Ge" "PS Survivor Space"                                                                                  | Used bytes after last collection of a given JVM memory pool                                            |
| jvm_memory_pool_collection_committed_bytes | Gauge   | **pool**, including: "PS Eden Space" "PS Old Ge" "PS Survivor Space"                                                                                  | Committed after last collection bytes of a given JVM memory pool                                       |
| jvm_memory_pool_collection_max_bytes       | Gauge   | **pool**, including: "PS Eden Space" "PS Old Ge" "PS Survivor Space"                                                                                  | Max bytes after last collection of a given JVM memory pool                                             |
| jvm_memory_pool_collection_init_bytes      | Gauge   | **pool**, including: "PS Eden Space" "PS Old Ge" "PS Survivor Space"                                                                                  | Initial after last collection bytes of a given JVM memory pool                                         |
| jvm_buffer_pool_used_bytes                 | Gauge   | **pool**, including: "direct" "mapped"                                                                                                                | Used bytes of a given JVM buffer pool                                                                  |
| jvm_buffer_pool_capacity_bytes             | Gauge   | **pool**, including: "direct" "mapped"                                                                                                                | Bytes capacity of a given JVM buffer pool                                                              |
| jvm_buffer_pool_used_buffers               | Gauge   | **pool**, including: "direct" "mapped"                                                                                                                | Used buffers of a given JVM buffer pool                                                                |

## Cluster Monitoring By Prometheus & Grafana

### Install Prometheus

For a guide on how to set up Prometheus server go to
the [Installation](https://prometheus.io/docs/prometheus/latest/installation)

### Configuration Prometheus

Add seatunnel instance metric exports into `/etc/prometheus/prometheus.yaml`. For example:

```yaml
global:
  # How frequently to scrape targets from this job.
  scrape_interval: 15s
scrape_configs:
  # The job name assigned to scraped metrics by default.
  - job_name: 'seatunnel'
    scrape_interval: 5s
    # Metrics export path 
    metrics_path: /hazelcast/rest/instance/metrics
    # List of labeled statically configured targets for this job.
    static_configs:
      # The targets specified by the static config.
      - targets: [ 'localhost:5801' ]
      # Labels assigned to all metrics scraped from the targets.
      # labels: [<labelName>:<labelValue>]
```

### Install Grafana

For a guide on how to set up Grafana server go to
the [Installation](https://grafana.com/docs/grafana/latest/setup-grafana/installation)

### Monitoring Dashboard

- Add Prometheus DataSource on Grafana.
  - Import `Seatunnel Cluster` monitoring dashboard by [Dashboard JSON](./telemetry/grafana-dashboard.json) into Grafana.

The [effect image](../../images/grafana.png) of the dashboard