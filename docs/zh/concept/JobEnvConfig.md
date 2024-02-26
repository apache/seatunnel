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

获取定时调度检查点的时间间隔。

在`STREAMING`模式下，检查点是必须的，如果不设置，将从应用程序配置文件`seatunnel.yaml`中获取。 在`BATCH`模式下，您可以通过不设置此参数来禁用检查点。

### parallelism

该参数配置source和sink的并行度。

### shade.identifier

指定加密方式，如果您没有加密或解密配置文件的需求，此选项可以忽略。

更多详细信息，您可以参考文档 [config-encryption-decryption](../../en/connector-v2/Config-Encryption-Decryption.md)

## Flink 引擎参数

这里列出了一些与 Flink 中名称相对应的 SeaTunnel 参数名称，并非全部，更多内容请参考官方 [flink documentation](https://flink.apache.org/) for more.

|           Flink 配置名称            |            SeaTunnel 配置名称             |
|---------------------------------|---------------------------------------|
| pipeline.max-parallelism        | flink.pipeline.max-parallelism        |
| execution.checkpointing.mode    | flink.execution.checkpointing.mode    |
| execution.checkpointing.timeout | flink.execution.checkpointing.timeout |
| ...                             | ...                                   |

## Spark 引擎参数

由于spark配置项并无调整，这里就不列出来了，请参考官方 [spark documentation](https://spark.apache.org/).

