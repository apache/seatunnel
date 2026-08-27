# 速度控制

## 介绍

SeaTunnel提供了强大的速度控制功能允许你管理数据同步的速率。当你需要确保在系统之间数据传输的高效和可控这个功能是至关重要的。
速度控制主要由两个关键参数控制：`read_limit.rows_per_second` 和 `read_limit.bytes_per_second`。
本文档将指导您如何使用这些参数以及如何有效地利用它们。

## 支持这些引擎

> SeaTunnel Zeta<br/>
> Flink<br/>
> Spark<br/>

## 配置

要使用速度控制功能，你需要在job配置中设置`read_limit.rows_per_second` 或 `read_limit.bytes_per_second`参数。

配置文件中env配置示例：

```hocon
env {
    job.mode=STREAMING
    job.name=SeaTunnel_Job
    read_limit.bytes_per_second=7000000
    read_limit.rows_per_second=400
}
source {
    MySQL-CDC {
      // ignore...
    }
}
transform {
}
sink {
    Console {
    }
}
```

我们在`env`参数中放了`read_limit.bytes_per_second` 和 `read_limit.rows_per_second`来完成速度控制的配置。
你可以同时配置这两个参数，或者只配置其中一个。每个`value`的值代表每个线程被限制的最大速率。
因此，在配置各个值时，还需要同时考虑你任务的并行性。
