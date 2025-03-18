---
sidebar_position: 4
---

# Run Jobs In Local Mode

Only for testing.

In local mode, each task will start a separate process, and the process will exit when the task is completed. There are the following limitations in this mode:

1. Pausing and resuming tasks are not supported.
2. Viewing the task list is not supported.
3. Jobs cannot be cancelled via commands, only by killing the process.
4. REST API is not supported.

The [Separated Cluster Mode](separated-cluster-deployment.md) of SeaTunnel Engine is recommended for use in production environments.

## Deploying SeaTunnel Engine In Local Mode

In local mode, there is no need to deploy a SeaTunnel Engine cluster. You only need to use the following command to submit jobs. The system will start the SeaTunnel Engine (Zeta) service in the process that submitted the job to run the submitted job, and the process will exit after the job is completed.

In this mode, you only need to copy the downloaded and created installation package to the server where you need to run it. If you need to adjust the JVM parameters for job execution, you can modify the `$SEATUNNEL_HOME/config/jvm_client_options` file.

## Submitting Jobs

```shell
$SEATUNNEL_HOME/bin/seatunnel.sh --config $SEATUNNEL_HOME/config/v2.batch.config.template -m local
```

### Configure The JVM Options For Local Mode

Local Mode supports two methods for setting JVM options:

1. Add the JVM options to `$SEATUNNEL_HOME/config/jvm_client_options`.

   Modify the JVM parameters in the `$SEATUNNEL_HOME/config/jvm_client_options` file. Please note that the JVM parameters in this file will be applied to all jobs submitted using `seatunnel.sh`, including Local Mode and Cluster Mode.

2. Add JVM options when starting the Local Mode. For example, `$SEATUNNEL_HOME/bin/seatunnel.sh --config $SEATUNNEL_HOME/config/v2.batch.config.template -m local -DJvmOption="-Xms2G -Xmx2G"`

## Job Operations

Jobs submitted in local mode will run in the process that submitted the job, and the process will exit when the job is completed. If you want to abort the job, you only need to exit the process that submitted the job. The job's runtime logs will be output to the standard output of the process that submitted the job.

Other operation and maintenance operations are not supported.
