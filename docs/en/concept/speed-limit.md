# Speed Control

## Introduction

The SeaTunnel provides a powerful speed control feature that allows you to manage the rate at which data is synchronized.
This functionality is essential when you need to ensure efficient and controlled data transfer between systems.
The speed control is primarily governed by two key parameters: `read_limit.rows_per_second` and `read_limit.bytes_per_second`.
This document will guide you through the usage of these parameters and how to leverage them effectively.

## Support Those Engines

> SeaTunnel Zeta<br/>
> Flink<br/>
> Spark<br/>

## Configuration

To use the speed control feature, you need to configure the `read_limit.rows_per_second` or `read_limit.bytes_per_second` parameters in your job config.

Example env config in your config file:

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

We have placed `read_limit.bytes_per_second` and `read_limit.rows_per_second` in the `env` parameters, completing the speed control configuration.
You can configure both of these parameters simultaneously or choose to configure only one of them. The value of each `value` represents the maximum rate at which each thread is restricted.
Therefore, when configuring the respective values, please take into account the parallelism of your tasks.
