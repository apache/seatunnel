---

sidebar_position: 8
-------------------

# Telemetry

Integrating `OpenTelemetry` through `Prometheus-exports` can better seamlessly connect to related monitoring platforms such as Prometheus and Grafana, improving the ability to monitor and alarm of the Seatunnel cluster.

You can configure telemetry's configurations in the `seatunnel.yaml` file.

The following is an example declarative configuration.

```yaml
seatunnel:
  engine:
    telemetry: 
      http-port: 9090 # The port exposed by the telemetry server, default is 9090.
      load-default-exports: true # Whether to load default jvm exports, default is true.
```

## Metrics

Available metrics include the following categories.

### Thread Pool Status

|             MetricName              |  Type   |                                Labels                                |
|-------------------------------------|---------|----------------------------------------------------------------------|
| job_thread_pool_activeCount         | Gauge   | **address**, server instance address,for example: "[localhost]:5801" |
| job_thread_pool_corePoolSize        | Gauge   | **address**, server instance address,for example: "[localhost]:5801" |
| job_thread_pool_maximumPoolSize     | Gauge   | **address**, server instance address,for example: "[localhost]:5801" |
| job_thread_pool_poolSize            | Gauge   | **address**, server instance address,for example: "[localhost]:5801" |
| job_thread_pool_completedTask_total | Counter | **address**, server instance address,for example: "[localhost]:5801" |
| job_thread_pool_task_total          | Counter | **address**, server instance address,for example: "[localhost]:5801" |

### Job info detail

| MetricName | Type  |                                                                               Labels                                                                               |
|------------|-------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| job_count  | Gauge | **type**, the type of job, including: "canceled" "cancelling" "created" "failed" "failing" "finished" "reconciling" "restarting" "running" "scheduled" "suspended" |

### JVM Metrics

|                 MetricName                 |  Type   |                                                                           Labels                                                                            |
|--------------------------------------------|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| jvm_threads_current                        | Gauge   | -                                                                                                                                                           |
| jvm_threads_daemon                         | Gauge   | -                                                                                                                                                           |
| jvm_threads_daemon                         | Gauge   | -                                                                                                                                                           |
| jvm_threads_peak                           | Gauge   | -                                                                                                                                                           |
| jvm_threads_started_total                  | Counter | -                                                                                                                                                           |
| jvm_threads_deadlocked                     | Gauge   | -                                                                                                                                                           |
| jvm_threads_deadlocked_monitor             | Gauge   | -                                                                                                                                                           |
| jvm_threads_state                          | Gauge   | **state**, the state of jvm thread, including: "NEW" "TERMINATED" "RUNNABLE" "BLOCKED" "WAITING" "TIMED_WAITING" "UNKNOWN"                                  |
| jvm_classes_currently_loaded               | Gauge   | -                                                                                                                                                           |
| jvm_classes_loaded_total                   | Counter | -                                                                                                                                                           |
| jvm_classes_unloaded_total                 | Counter | -                                                                                                                                                           |
| jvm_memory_pool_allocated_bytes_total      | Counter | **pool**,including: "Code Cache" "PS Eden Space" "PS Old Ge" "PS Survivor Space" "Compressed Class Space" "Metaspace"                                       |
| jvm_gc_collection_seconds_count            | Summary | **gc**,including: "PS Scavenge" "PS MarkSweep"                                                                                                              |
| jvm_gc_collection_seconds_sum              | Summary | **gc**,including: "PS Scavenge" "PS MarkSweep"                                                                                                              |
| jvm_info                                   | Gauge   | **runtime**, for example: "Java(TM) SE Runtime Environment"<br> **vendor**, for example: "Oracle Corporation"<br> **version** ,for example: "1.8.0_212-b10" |
| process_cpu_seconds_total                  | Counter | -                                                                                                                                                           |
| process_start_time_seconds                 | Gauge   | -                                                                                                                                                           |
| process_open_fds                           | Gauge   | -                                                                                                                                                           |
| process_max_fds                            | Gauge   | -                                                                                                                                                           |
| jvm_memory_objects_pending_finalization    | Gauge   | -                                                                                                                                                           |
| jvm_memory_bytes_used                      | Gauge   | **area**, including: "heap" "noheap"                                                                                                                        |
| jvm_memory_bytes_committed                 | Gauge   | **area**, including: "heap" "noheap"                                                                                                                        |
| jvm_memory_bytes_max                       | Gauge   | **area**, including:"heap" "noheap"                                                                                                                         |
| jvm_memory_bytes_init                      | Gauge   | **area**, including:"heap" "noheap"                                                                                                                         |
| jvm_memory_pool_bytes_used                 | Gauge   | **pool**, including: "Code Cache" "PS Eden Space" "PS Old Ge" "PS Survivor Space" "Compressed Class Space" "Metaspace"                                      |
| jvm_memory_pool_bytes_committed            | Gauge   | **pool**, including: "Code Cache" "PS Eden Space" "PS Old Ge" "PS Survivor Space" "Compressed Class Space" "Metaspace"                                      |
| jvm_memory_pool_bytes_max                  | Gauge   | **pool**, including: "Code Cache" "PS Eden Space" "PS Old Ge" "PS Survivor Space" "Compressed Class Space" "Metaspace"                                      |
| jvm_memory_pool_bytes_init                 | Gauge   | **pool**, including: "Code Cache" "PS Eden Space" "PS Old Ge" "PS Survivor Space" "Compressed Class Space" "Metaspace"                                      |
| jvm_memory_pool_allocated_bytes_created    | Gauge   | **pool**, including: "Code Cache" "PS Eden Space" "PS Old Ge" "PS Survivor Space" "Compressed Class Space" "Metaspace"                                      |
| jvm_memory_pool_collection_used_bytes      | Gauge   | **pool**, including: "PS Eden Space" "PS Old Ge" "PS Survivor Space"                                                                                        |
| jvm_memory_pool_collection_committed_bytes | Gauge   | **pool**, including: "PS Eden Space" "PS Old Ge" "PS Survivor Space"                                                                                        |
| jvm_memory_pool_collection_max_bytes       | Gauge   | **pool**, including: "PS Eden Space" "PS Old Ge" "PS Survivor Space"                                                                                        |
| jvm_memory_pool_collection_init_bytes      | Gauge   | **pool**, including: "PS Eden Space" "PS Old Ge" "PS Survivor Space"                                                                                        |
| jvm_buffer_pool_used_bytes                 | Gauge   | **pool**, including: "direct" "mapped"                                                                                                                      |
| jvm_buffer_pool_capacity_bytes             | Gauge   | **pool**, including: "direct" "mapped"                                                                                                                      |
| jvm_buffer_pool_used_buffers               | Gauge   | **pool**, including: "direct" "mapped"                                                                                                                      |

### 实现方案

