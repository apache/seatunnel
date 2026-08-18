# Web UI

## Access

Before accessing the Web UI, enable the HTTP REST API in `seatunnel.yaml`:

```
seatunnel:
  engine:
    http:
      enable-http: true
      port: 8080

```

Then visit `http://ip:8080/#/overview`

For production deployments, consider enabling Basic authentication or HTTPS as described in [security](security.md).

## Scope And Deployment Mode

The current Web UI is primarily an observability interface for SeaTunnel Engine (Zeta). It lets you inspect cluster
overview data, running jobs, finished jobs, worker metrics, master metrics, job DAGs, and job logs.

Job lifecycle actions such as submit, cancel, stop, savepoint, or restore are not exposed in the current UI. Use the
CLI or [REST API V2](rest-api-v2.md) for those operations.

For the full Jobs, Workers, and Master experience, deploy SeaTunnel Engine in hybrid cluster mode or separated cluster
mode. Local mode exits after the submitted job completes and does not support viewing the job list, so it is not a
good fit for the full Web UI workflow.

## Overview

The Web UI of Apache SeaTunnel offers a user-friendly interface for monitoring SeaTunnel jobs and cluster status.
Through the Web UI, users can view real-time information on currently running jobs, finished jobs, and the status of
worker and master nodes within the cluster. The main functional modules include Jobs, Workers, and Master, each
providing detailed status information to help users troubleshoot and optimize data processing workflows.
![overview.png](../../../images/ui/overview.png)

## Jobs

### Running Jobs

The "Running Jobs" section lists all SeaTunnel jobs that are currently in execution. Users can view basic information for each job, including Job ID, submission time, status, execution time, and more. By clicking on a specific job, users can access detailed information such as task distribution, resource utilization, and log outputs, allowing for real-time monitoring of job progress and timely handling of potential issues.
![running.png](../../../images/ui/running.png)
![detail.png](../../../images/ui/detail.png)

### Finished Jobs

The "Finished Jobs" section displays all SeaTunnel jobs that have either successfully completed or failed. This
section provides execution results, completion times, durations, and failure reasons (if any) for each job. Users can
review past job records through this module to analyze job performance and troubleshoot issues.
![finished.png](../../../images/ui/finished.png)

## Workers

### Workers Information

The "Workers" section displays detailed information about all worker nodes in the cluster, including each worker's address, running status, CPU and memory usage, number of tasks being executed, and more. Through this module, users can monitor the health of each worker node, promptly identify and address resource bottlenecks or node failures, ensuring the stable operation of the SeaTunnel cluster.
![workers.png](../../../images/ui/workers.png)

## Master

### Master Information

The "Master" section provides the status and configuration information of the master node in the SeaTunnel cluster. Users can view the master's address, running status, job scheduling responsibilities, and overall resource allocation within the cluster. This module helps users gain a comprehensive understanding of the cluster's core management components, facilitating cluster configuration optimization and troubleshooting.
![master.png](../../../images/ui/master.png)
