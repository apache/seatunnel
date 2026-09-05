# JobEnvConfig

本文档描述了env的配置信息，公共参数可以在所有引擎中使用。为了更好的区分引擎参数，其他引擎的附加参数需要携带前缀。
在flink引擎中，我们使用`flink.`作为前缀。在spark引擎中，我们不使用任何前缀来修改参数，因为官方的spark参数本身就是以`spark.`开头。

## 公共参数

以下配置参数对所有引擎通用：

### job.name

该参数配置任务名称。

### jars

第三方包可以通过`jars`加载，例如：`jars="file://local/jar1.jar;file://local/jar2.jar"`

### job.mode

通过`job.mode`你可以配置任务是在批处理模式还是流处理模式。例如：`job.mode = "BATCH"` 或者 `job.mode = "STREAMING"`

### checkpoint.interval

获取定时调度检查点的时间间隔(毫秒)。

在`STREAMING`模式下，检查点是必须的，如果不设置，将从应用程序配置文件`seatunnel.yaml`中获取。 在`BATCH`模式下，您可以通过不设置此参数来禁用检查点。在Zeta `STREAMING`模式下，默认值为30000毫秒。

:::note

在 **Spark 引擎**上，SeaTunnel env 中的检查点相关配置（例如 `checkpoint.interval`）**不会被 Spark starter 应用**。对于 Spark Structured Streaming sink，请在 connector 支持的情况下使用 Spark 原生的检查点配置（例如 `checkpointLocation`）。

:::

### checkpoint.timeout

检查点的超时时间(毫秒)。如果检查点在超时之前没有完成，作业将失败。在Zeta中，默认值为30000毫秒。

### parallelism

该参数配置source和sink的并行度。

### shade.identifier

指定加密方式，如果您没有加密或解密配置文件的需求，此选项可以忽略。

更多详细信息，您可以参考文档 [Config Encryption Decryption](./config-encryption-decryption.md)

## Zeta 引擎参数

### job.retry.times

用于控制作业失败时的默认重试次数。默认值为3，并且仅适用于Zeta引擎。

该计数器会在整个 pipeline 生命周期内持续累加，中途一次成功恢复并不会将其重置。例如设置 `job.retry.times = 5` 时：如果 pipeline 失败后经过重试，在第 3 次尝试时恢复成功，之后又再次失败，此时只剩下 2 次重试机会（第 4、5 次尝试），用完后作业会被标记为永久失败，重试额度不会恢复到 5 次。唯一的例外是 Zeta 集群发生 active master 切换时，pipeline 执行计划（以及其重试计数器）会被重新构建。

### job.retry.interval.seconds

用于控制作业失败时的默认重试间隔。默认值为3秒，并且仅适用于Zeta引擎。

### savemode.execute.location

此参数用于指定在Zeta引擎中执行作业时SaveMode执行的时机。
默认值为`CLUSTER`，这意味着SaveMode在作业提交到集群上之后在集群上执行。
当值为`CLIENT`时，SaveMode操作在作业提交的过程中执行，使用shell脚本提交作业时，该过程在提交作业的shell进程中执行。使用rest api提交作业时，该过程在http请求的处理线程中执行。
请尽量使用`CLUSTER`模式，因为当`CLUSTER`模式没有问题时，我们将删除`CLIENT`模式。

### sink.flush.interval

定时向数据流中注入 `FlushSignal` 的间隔（毫秒），驱动 Sink 刷写缓冲数据。设置为 `0` 或不配置（默认）时不生效。仅适用于 Zeta 引擎。

建议不低于 100ms。过于频繁会产生大量无效空刷信号占用数据流队列容量，挤压正常数据记录的传输空间，并在缓冲区尚无数据时触发无意义的写出，增加 Sink I/O 开销。

## Flink 引擎参数

这里列出了一些与 Flink 中名称相对应的 SeaTunnel 参数名称，并非全部，更多内容请参考官方 [Flink Documentation](https://flink.apache.org/) for more.

|           Flink 配置名称            |            SeaTunnel 配置名称             |
|---------------------------------|---------------------------------------|
| pipeline.max-parallelism        | flink.pipeline.max-parallelism        |
| execution.checkpointing.mode    | flink.execution.checkpointing.mode    |
| execution.checkpointing.timeout | flink.execution.checkpointing.timeout |
| ...                             | ...                                   |

## Spark 引擎参数

由于Spark配置项并无调整，这里就不列出来了，请参考官方 [Spark Documentation](https://spark.apache.org/).
