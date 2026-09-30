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
| Staging | HDFS or another shared filesystem | Store the distribution, job configuration, and Hadoop configuration for this application |
| Checkpoint | HDFS or a supported object store | Preserve recoverable job state outside the application |

Losing the ApplicationMaster or any worker fails the current application. The YARN application has one attempt and does not replace the master or workers. A new application can restore from a durable checkpoint.

## Prerequisites

- Reachable YARN ResourceManager and NodeManagers, plus HDFS using simple authentication.
- Permission to submit to the target queue and write to the staging directory.
- Linux or Unix NodeManagers with a compatible Java version.
- Hazelcast TCP and Zeta execution connectivity between AM and worker containers.
- Hadoop configuration containing `core-site.xml`, `hdfs-site.xml`, and `yarn-site.xml`.
- A distribution containing the YARN provider and every connector, format, transform, and driver required by the job.

A multi-node YARN deployment requires a shared staging filesystem such as HDFS. Kerberos, delegation tokens, and keytabs are not supported in this release.

## Recommended reading order

| Phase | Document | Content |
| --- | --- | --- |
| Understand deployment | [Cluster architecture](architecture.md) | AM, workers, staging, networking, and resource lifecycle |
| Run the first job | [Quick start](quick-start.md) | Build a distribution and submit a job |
| Tune deployment | [Configuration](configuration.md) | Complete YARN and shared application options |
| Configure recovery | [Checkpoint recovery](checkpoint-recovery.md) | HDFS or object storage and explicit recovery submission |
| Troubleshoot | [FAQ](faq.md) | Status, cancellation, logs, cleanup, and common failures |

See the shared [Application Mode overview](../overview.md) and [architecture](../architecture.md) for common behavior.
