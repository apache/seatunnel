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

### checkpoint.timeout

检查点的超时时间(毫秒)。如果检查点在超时之前没有完成，作业将失败。在Zeta中，默认值为30000毫秒。

### parallelism

该参数配置source和sink的并行度。

### shade.identifier

指定加密方式，如果您没有加密或解密配置文件的需求，此选项可以忽略。

更多详细信息，您可以参考文档 [Config Encryption Decryption](../../en/connector-v2/Config-Encryption-Decryption.md)

## Zeta 引擎参数

### job.retry.times

用于控制作业失败时的默认重试次数。默认值为3，并且仅适用于Zeta引擎。

### job.retry.interval.seconds

用于控制作业失败时的默认重试间隔。默认值为3秒，并且仅适用于Zeta引擎。

### savemode.execute.location

此参数用于指定在Zeta引擎中执行作业时SaveMode执行的时机。
默认值为`CLUSTER`，这意味着SaveMode在作业提交到集群上之后在集群上执行。
当值为`CLIENT`时，SaveMode操作在作业提交的过程中执行，使用shell脚本提交作业时，该过程在提交作业的shell进程中执行。使用rest api提交作业时，该过程在http请求的处理线程中执行。
请尽量使用`CLUSTER`模式，因为当`CLUSTER`模式没有问题时，我们将删除`CLIENT`模式。

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

