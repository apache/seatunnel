# Job Env Config

This document describes env configuration information. The common parameters can be used in all engines. In order to better distinguish between engine parameters, the additional parameters of other engine need to carry a prefix.
In flink engine, we use `flink.` as the prefix. In the spark engine, we do not use any prefixes to modify parameters, because the official spark parameters themselves start with `spark.`

## Common Parameter

The following configuration parameters are common to all engines.

### job.name

This parameter configures the task name.

### jars

Third-party packages can be loaded via `jars`, like `jars="file://local/jar1.jar;file://local/jar2.jar"`.

### job.mode

You can configure whether the task is in batch or stream mode through `job.mode`, like `job.mode = "BATCH"` or `job.mode = "STREAMING"`

### checkpoint.interval

Gets the interval (milliseconds) in which checkpoints are periodically scheduled.

In `STREAMING` mode, checkpoints is required, if you do not set it, it will be obtained from the application configuration file `seatunnel.yaml`. In `BATCH` mode, you can disable checkpoints by not setting this parameter. In Zeta `STREAMING` mode, the default value is 30000 milliseconds.

### checkpoint.timeout

The timeout (in milliseconds) for a checkpoint. If the checkpoint is not completed before the timeout, the job will fail. In Zeta, the default value is 30000 milliseconds.

### parallelism

This parameter configures the parallelism of source and sink.

### shade.identifier

Specify the method of encryption, if you didn't have the requirement for encrypting or decrypting config files, this option can be ignored.

For more details, you can refer to the documentation [Config Encryption Decryption](../connector-v2/Config-Encryption-Decryption.md)

## Zeta Engine Parameter

### job.retry.times

Used to control the default retry times when a job fails. The default value is 3, and it only works in the Zeta engine.

### job.retry.interval.seconds

Used to control the default retry interval when a job fails. The default value is 3 seconds, and it only works in the Zeta engine.

### savemode.execute.location

This parameter is used to specify the location of the savemode when the job is executed in the Zeta engine.
The default value is `CLUSTER`, which means that the savemode is executed on the cluster. If you want to execute the savemode on the client,
you can set it to `CLIENT`. Please use `CLUSTER` mode as much as possible, because when there are no problems with `CLUSTER` mode, we will remove `CLIENT` mode.

## Flink Engine Parameter

Here are some SeaTunnel parameter names corresponding to the names in Flink, not all of them. Please refer to the official [Flink Documentation](https://flink.apache.org/).

|    Flink Configuration Name     |     SeaTunnel Configuration Name      |
|---------------------------------|---------------------------------------|
| pipeline.max-parallelism        | flink.pipeline.max-parallelism        |
| execution.checkpointing.mode    | flink.execution.checkpointing.mode    |
| execution.checkpointing.timeout | flink.execution.checkpointing.timeout |
| ...                             | ...                                   |

## Spark Engine Parameter

Because Spark configuration items have not been modified, they are not listed here, please refer to the official [Spark Documentation](https://spark.apache.org/).
