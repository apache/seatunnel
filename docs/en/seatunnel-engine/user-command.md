---
sidebar_position: 13
---

# Client Command Line Tool

The SeaTunnel Engine provides a command line tool for managing the jobs of the SeaTunnel Engine. You can use the command line tool to submit, stop, pause, resume, delete jobs, view job status and monitoring metrics, etc.

You can obtain the help information of the command line tool through the following command:

```shell
sh bin/seatunnel.sh -h
```

The output is as follows:

```

Usage: seatunnel.sh [options]
  Options:
    --async                         Run the job asynchronously. When the job is submitted, the client will exit (default: false).
    -can, --cancel-job              Cancel the job by JobId.
    --check                         Whether to check the config (default: false).
    -cj, --close-job                Close the client and the task will also be closed (default: true).
    -cn, --cluster                  The name of the cluster.
    -c, --config                    Config file.
    --decrypt                       Decrypt the config file. When both --decrypt and --encrypt are specified, only --encrypt will take effect (default: false). 
    -m, --master, -e, --deploy-mode SeaTunnel job submit master, support [local, cluster] (default: cluster).
    --encrypt                       Encrypt the config file. When both --decrypt and --encrypt are specified, only --encrypt will take effect (default: false). 
    --get_running_job_metrics       Get metrics for running jobs (default: false).
    -h, --help                      Show the usage message.
    -j, --job-id                    Get the job status by JobId.
    -l, --list                      List the job status (default: false).
    --metrics                       Get the job metrics by JobId.
    -n, --name                      The SeaTunnel job name (default: SeaTunnel).
    -r, --restore                   Restore with savepoint by jobId.
    -s, --savepoint                 Savepoint the job by jobId.
    -i, --variable                  Variable substitution, such as -i city=beijing, or -i date=20190318. We use ',' as a separator. When inside "", ',' are treated as normal characters instead of delimiters. (default: []).

```

## Submitting Jobs

```shell
sh bin/seatunnel.sh --config $SEATUNNEL_HOME/config/v2.batch.config.template
```

The **--async** parameter allows the job to run in the background. When the job is submitted, the client will exit.

```shell
sh bin/seatunnel.sh --config $SEATUNNEL_HOME/config/v2.batch.config.template --async
```

The **-n** or **--name** parameter can specify the name of the job.

```shell
sh bin/seatunnel.sh --config $SEATUNNEL_HOME/config/v2.batch.config.template --async -n myjob
```

## Viewing The Job List

```shell
sh bin/seatunnel.sh -l
```

This command will output the list of all jobs in the current cluster (including completed historical jobs and running jobs).

## Viewing The Job Status

```shell
sh bin/seatunnel.sh -j <jobId>
```

This command will output the status information of the specified job.

## Getting The Monitoring Information Of Running Jobs

```shell
sh bin/seatunnel.sh --get_running_job_metrics
```

This command will output the monitoring information of running jobs.

## Getting the Monitoring Information of a Specified Job

The --metrics parameter can get the monitoring information of a specified job.

```shell
sh bin/seatunnel.sh --metrics <jobId>
```

## Pausing Jobs

```shell
sh bin/seatunnel.sh -s <jobId>
```

This command will pause the specified job. Note that only jobs with checkpoints enabled support pausing jobs (real-time synchronization jobs have checkpoints enabled by default, and batch jobs do not have checkpoints enabled by default and need to configure checkpoint.interval in `env` to enable checkpoints).

Pausing a job is in the smallest unit of split. That is, after pausing a job, it will wait for the currently running split to finish running and then pause. After the task is resumed, it will continue to run from the paused split.

## Resuming Jobs

```shell
sh bin/seatunnel.sh -r <jobId> -c $SEATUNNEL_HOME/config/v2.batch.config.template
```

This command will resume the specified job. Note that only jobs with checkpoints enabled support resuming jobs (real-time synchronization jobs have checkpoints enabled by default, and batch jobs do not have checkpoints enabled by default and need to configure checkpoint.interval in `env` to enable checkpoints).

Resuming a job requires the jobId and the configuration file of the job.

Both failed jobs and jobs paused by seatunnel.sh -s &lt;jobId&gt; can be resumed by this command.

## Canceling Jobs

```shell
sh bin/seatunnel.sh -can <jobId1> [<jobId2> <jobId3> ...]
```

This command will cancel the specified job. After canceling the job, the job will be stopped and its status will become `CANCELED`.

Supports batch cancellation of jobs, and can cancel multiple jobs at one time.

All breakpoint information of the canceled job will be deleted and cannot be resumed by seatunnel.sh -r &lt;jobId&gt;.

## Configure The JVM Options

We can configure the JVM options for the SeaTunnel Engine client in the following ways:

1. Add the JVM options to `$SEATUNNEL_HOME/config/jvm_client_options`.

   Modify the JVM parameters in the `$SEATUNNEL_HOME/config/jvm_client_options` file. Please note that the JVM parameters in this file will be applied to all jobs submitted using `seatunnel.sh`, including Local Mode and Cluster Mode.

2. Add JVM options when submitting jobs. For example, `sh bin/seatunnel.sh --config $SEATUNNEL_HOME/config/v2.batch.config.template -DJvmOption="-Xms2G -Xmx2G"`

# Server Command Line Tool

SeaTunnel Engine provides server management commands for starting, stopping, and managing SeaTunnel Engine cluster nodes.

```shell
sh bin/seatunnel-cluster.sh -h
```

Server commands support the following parameters:

```shell
Usage: seatunnel-cluster.sh [options]
  Options:
    -cn, --cluster      The name of cluster.
    -d, --daemon        The cluster daemon mode.
    -r, --role          The cluster node role, support [master, worker, master_and_worker] (default: master_and_worker).
    -m, --member        Show cluster members information.
    -h, --help          Show the usage message.
```

## Start cluster

You can get help information for server commands with the following command:

```shell
# Start in foreground
sh bin/seatunnel-cluster.sh

# Start in daemon mode
sh bin/seatunnel-cluster.sh -d
```

## Show cluster members information

You can view cluster members information using the following command:

```shell
sh bin/seatunnel-cluster.sh -m -cn my_cluster
```

This command will output detailed information about all members in the cluster, including:
- **Member ID**: Unique identifier for each cluster member
- **Address**: IP address and port of the member
- **Role**: Member role (ACTIVE MASTER, MASTER, or WORKER)
- **Version**: Hazelcast version running on the member

**Example output:**
```
Member ID                            Address              Role                 Version
a1b2c3d4-e5f6-7890-abcd-ef1234567890 192.168.1.100:5701  ACTIVE MASTER        5.3.0
b2c3d4e5-f6g7-8901-bcde-f23456789012 192.168.1.101:5701  MASTER               5.3.0
c3d4e5f6-g7h8-9012-cdef-345678901234 192.168.1.102:5701  WORKER               5.3.0
```

**Note**: You must specify the cluster name with the `-cn` parameter. The cluster must be running for this command to work.

## Stop cluster

SeaTunnel provides a dedicated stop script to shut down cluster nodes:

```shell
sh bin/stop-seatunnel-cluster.sh -h
```

The stop command supports the following parameters:

```shell
Usage: stop-seatunnel-cluster.sh [options]
  Options:
    -cn, --cluster      The name of the cluster to shut down (default: seatunnel_default_cluster)
    -h, --help          Show the usage message
```

### Stop default cluster

```shell
# Stop the default cluster (seatunnel_default_cluster)
sh bin/stop-seatunnel-cluster.sh
```

### Stop specified cluster

```shell
# Stop a cluster with specified name
sh bin/stop-seatunnel-cluster.sh -cn my_cluster
```