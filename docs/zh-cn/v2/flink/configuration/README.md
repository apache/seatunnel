## Waterdrop v2.x For Flink 配置文件

# 通用配置


## 配置文件

一个完整的Waterdrop配置包含`env`, `source`, `transform`, `sink`, 即：

```
    env {
        ...
    }
    
    source {
        ...
    }
    
    transform {
        ...
    }
    
    sink {
        ...
    }

```

* `env`是flink任务的相关的配置，例如设置时间为event-time还是process-time

```
 env {
    execution.parallelism = 1 #设置任务的整体并行度为1
    execution.checkpoint.interval = 10000 #设置任务checkpoint的频率
    execution.checkpoint.data-uri = "hdfs://localhost:9000/checkpoint" #设置checkpoint的路径
  }
```

* 以下是env可配置项，具体含义可以参照[flink官网配置](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/execution_configuration.html)
```java
    public class ConfigKeyName {
        //the time characteristic for all streams create from this environment, e.g., processing-time,event-time,ingestion-time
        public final static String TIME_CHARACTERISTIC = "execution.time-characteristic";
        //the maximum time frequency (milliseconds) for the flushing of the output buffers
        public final static String BUFFER_TIMEOUT_MILLIS = "execution.buffer.timeout";
        //the parallelism for operations executed through this environment
        public final static String PARALLELISM = "execution.parallelism";
        //the maximum degree of parallelism to be used for the program
        public final static String MAX_PARALLELISM = "execution.max-parallelism";
        //enables checkpointing for the streaming job,time interval between state checkpoints in milliseconds
        public final static String CHECKPOINT_INTERVAL = "execution.checkpoint.interval";
        //the checkpointing mode (exactly-once vs. at-least-once)
        public final static String CHECKPOINT_MODE = "execution.checkpoint.mode";
        //the maximum time that a checkpoint may take before being discarded
        public final static String CHECKPOINT_TIMEOUT = "execution.checkpoint.timeout";
        //a file system for the file system scheme in the URI (e.g., 'file://', 'hdfs://', or 'S3://')
        public final static String CHECKPOINT_DATA_URI = "execution.checkpoint.data-uri";
        //the maximum number of checkpoint attempts that may be in progress at the same time
        public final static String MAX_CONCURRENT_CHECKPOINTS = "execution.max-concurrent-checkpoints";
        //enables checkpoints to be persisted externally,delete externalized checkpoints on job cancellation (e.g., true,false)
        public final static String CHECKPOINT_CLEANUP_MODE = "execution.checkpoint.cleanup-mode";
        //the minimal pause before the next checkpoint is triggere
        public final static String MIN_PAUSE_BETWEEN_CHECKPOINTS = "execution.checkpoint.min-pause";
        //the tolerable checkpoint failure number
        public final static String FAIL_ON_CHECKPOINTING_ERRORS = "execution.checkpoint.fail-on-error";
        //the restart strategy to be used for recovery (e.g., 'no' , 'fixed-delay', 'failure-rate')
        //no -> no restart strategy
        //fixed-delay -> fixed delay restart strategy
        //failure-rate -> failure rate restart strategy
        public final static String RESTART_STRATEGY = "execution.restart.strategy";
        //number of restart attempts for the fixed delay restart strategy
        public final static String RESTART_ATTEMPTS = "execution.restart.attempts";
        //delay in-between restart attempts for the delay restart strategy
        public final static String RESTART_DELAY_BETWEEN_ATTEMPTS = "execution.restart.delayBetweenAttempts";
        //time interval for failures
        public final static String RESTART_FAILURE_INTERVAL = "execution.restart.failureInterval";
        //maximum number of restarts in given interval for the failure rate restart strategy
        public final static String RESTART_FAILURE_RATE = "execution.restart.failureRate";
        //delay in-between restart attempts for the failure rate restart strategy
        public final static String RESTART_DELAY_INTERVAL = "execution.restart.delayInterval";
        //the maximum time interval for which idle state is retained
        public final static String MAX_STATE_RETENTION_TIME = "execution.query.state.max-retention";
        //the minimum time interval for which idle state is retained
        public final static String MIN_STATE_RETENTION_TIME = "execution.query.state.min-retention";
        //the state backend ('rocksdb','fs')
        public final static String STATE_BACKEND = "execution.state.backend";
    
    }
```


* `source`可配置任意的source插件及其参数，具体参数随不同的source插件而变化。

* `transform`可配置任意的transform插件及其参数，具体参数随不同的transform插件而变化。transform中的多个插件按配置顺序形成了数据处理的pipeline, 默认上一个transform的输出是下一个transform的输入,但也可以通过source_table_name控制。
* `transform`处理完的数据，会发送给`sink`中配置的每个插件。
* `sink`可配置任意的sink插件及其参数，具体参数随不同的sink插件而变化。

