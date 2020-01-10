## Waterdrop v2.x For Flink 配置文件

# 通用配置

## 核心概念


---

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

* `env`是运行环境相关的配置，以下是env可配置项，具体含义可以参照[flink官网配置](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/execution_configuration.html)
```java
public class ConfigKeyName {
    public final static String TIME_CHARACTERISTIC = "execution.time-characteristic";
    public final static String BUFFER_TIMEOUT_MILLIS = "execution.buffer.timeout";
    public final static String PARALLELISM = "execution.parallelism";
    public final static String MAX_PARALLELISM = "execution.max-parallelism";
    public final static String CHECKPOINT_INTERVAL = "execution.checkpoint.interval";
    public final static String CHECKPOINT_MODE = "execution.checkpoint.mode";
    public final static String CHECKPOINT_TIMEOUT = "execution.checkpoint.timeout";
    public final static String CHECKPOINT_DATA_URI = "execution.checkpoint.data-uri";
    public final static String MAX_CONCURRENT_CHECKPOINTS = "execution.max-concurrent-checkpoints";
    public final static String CHECKPOINT_CLEANUP_MODE = "execution.checkpoint.cleanup-mode";
    public final static String MIN_PAUSE_BETWEEN_CHECKPOINTS = "execution.checkpoint.min-pause";
    public final static String FAIL_ON_CHECKPOINTING_ERRORS = "execution.checkpoint.fail-on-error";
    public final static String RESTART_STRATEGY = "execution.restart.strategy";
    public final static String RESTART_ATTEMPTS = "execution.restart.attempts";
    public final static String RESTART_DELAY_BETWEEN_ATTEMPTS = "execution.restart.delayBetweenAttempts";
    public final static String RESTART_FAILURE_INTERVAL = "execution.restart.failureInterval";
    public final static String RESTART_FAILURE_RATE = "execution.restart.failureRate";
    public final static String RESTART_DELAY_INTERVAL = "execution.restart.delayInterval";
    public final static String MAX_STATE_RETENTION_TIME = "execution.query.state.max-retention";
    public final static String MIN_STATE_RETENTION_TIME = "execution.query.state.min-retention";
    public final static String STATE_BACKEND = "execution.state.backend";

}
```


* `source`可配置任意的input插件及其参数，具体参数随不同的input插件而变化。

* `transform`可配置任意的filter插件及其参数，具体参数随不同的filter插件而变化。

transform中的多个插件按配置顺序形成了数据处理的pipeline, 默认上一个transform的输出是下一个transform的输入。但也可以通过source_table_name控制。

* `sink`可配置任意的output插件及其参数，具体参数随不同的output插件而变化。

* `transform`处理完的数据，会发送给`sink`中配置的每个插件。