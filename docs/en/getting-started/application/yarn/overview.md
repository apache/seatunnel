---
sidebar_position: 1
title: YARN Overview
---

# YARN Application Mode

YARN Application Mode starts an independent YARN application for one SeaTunnel job. The ApplicationMaster runs the only Zeta master, requests a fixed number of worker containers, submits the job after every worker registers, and releases resources when the job terminates.

Use it when YARN already manages compute resources and you do not want to deploy a long-running Zeta cluster. Batch resources are reclaimed after completion. A streaming job remains an independent application until it is canceled or fails.

## Runtime layout

| Component | YARN resource | Responsibility |
| --- | --- | --- |
| Application master | One AM container | Start the Zeta master, request workers, submit the job, and coordinate cleanup |
| Worker | N regular containers | Join the isolated Hazelcast cluster and provide fixed task slots |
| Staging | HDFS or another shared filesystem | Store the archive manifest, job and Hadoop configuration, and any application-private archive copy |
| Checkpoint | HDFS or a supported object store | Preserve recoverable job state outside the application |

Losing the ApplicationMaster or any worker fails the current application. The YARN application has one attempt and does not replace the master or workers. A new application can restore from a durable checkpoint.

## Capabilities

| Area | YARN behavior |
| --- | --- |
| Submission | Submit, inspect, wait for and cancel one job by its application ID. |
| Job configuration | Submit a local job with `-c`, or use a local, HDFS or inline job file for the AM. |
| Runtime files | The AM and workers share a localized distribution, shipped files, environment and logging. JVM options and Hazelcast files can differ by role. |
| Scheduling | Select the queue, priority, tags, AM and worker node labels, and application lifetime. |
| Authentication | Use simple authentication, an existing Kerberos ticket or a submitter-local keytab according to the Hadoop cluster settings. |
| Recovery | Submit a new application using an earlier Zeta job ID and durable checkpoint storage. |

The optional AM Web UI and REST API are available for the lifetime of a running application. The port belongs to the chosen NodeManager host. See [Master HTTP endpoint](configuration.md#master-http-endpoint) for direct and proxied access.

## Prerequisites

- Reachable YARN ResourceManager and NodeManagers, plus HDFS using simple or Kerberos authentication.
- Permission to submit to the target queue and write to the staging directory.
- Linux or Unix NodeManagers with a compatible Java version.
- Hazelcast TCP and Zeta execution connectivity between AM and worker containers.
- Hadoop configuration containing `core-site.xml`, `hdfs-site.xml`, and `yarn-site.xml`.
- A distribution containing the YARN provider and every connector, format, transform, and driver required by the job.

A multi-node YARN deployment requires a shared staging filesystem such as HDFS. For secure clusters, see [Kerberos configuration](configuration.md#kerberos).

## Recommended reading order

| Phase | Document | Content |
| --- | --- | --- |
| Understand deployment | [Cluster architecture](architecture.md) | AM, workers, staging, networking, and resource lifecycle |
| Run the first job | [Quick start](quick-start.md) | Build a distribution and submit a job |
| Operate an application | [Commands](commands.md) | Submit, inspect, wait for and cancel an application |
| Tune deployment | [Configuration](configuration.md) | Complete YARN and shared application options |
| Configure recovery | [Checkpoint recovery](checkpoint-recovery.md) | HDFS or object storage and explicit recovery submission |
| Troubleshoot | [FAQ](faq.md) | Status, cancellation, logs, cleanup, and common failures |

See the shared [Application Mode overview](../overview.md) for common behavior.
