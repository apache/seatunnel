---
sidebar_position: 1
---

# SeaTunnel Engine

SeaTunnel Engine is the native execution engine of SeaTunnel and the default recommendation for new deployments. It is built for data integration and synchronization workloads, with a focus on high throughput, low latency, strong consistency, and lower operational overhead than a full big data platform.

Use this page as the entry point if you want to understand why many new users start with SeaTunnel Engine, what it is optimized for, and which pages to read next.

## When SeaTunnel Engine Is The Right First Choice

SeaTunnel Engine is usually the best starting point when:

- you do not already operate Flink or Spark infrastructure
- you want the shortest path from installation to a working job
- your main scenarios are CDC, multi-table synchronization, or database migration
- you need lower resource consumption for many small or medium synchronization pipelines

If your team already has a stable Flink or Spark deployment that you want to reuse, read [Engine Overview](../overview.md) first and then jump to the matching engine guide.

## Why Many New Users Start Here

- **No external dependencies**: cluster management, snapshot storage, and HA do not depend on services such as Zookeeper or HDFS.
- **Pipeline-level fault tolerance**: failures are isolated at the pipeline level instead of forcing a larger rollback scope.
- **Lower runtime overhead**: dynamic thread sharing, reduced JDBC connection count, and reuse of CDC log-reading resources help reduce resource consumption.
- **One engine for common synchronization workloads**: batch, streaming, and CDC-oriented jobs can share the same operational model.

The overall design of the SeaTunnel Engine follows the path below:

- Faster, SeaTunnel Engine’s execution plan optimizer aims to reduce data network transmission, thereby reducing the loss of overall synchronization performance caused by data serialization and de-serialization, allowing users to complete data synchronization operations faster. At the same time, a speed limit is supported to synchronize data at a reasonable speed.
- More stable, SeaTunnel Engine uses Pipeline as the minimum granularity of checkpoint and fault tolerance for data synchronization tasks. The failure of a task will only affect its upstream and downstream tasks, which avoids task failures that cause the entire job to fail or rollback. At the same time, SeaTunnel Engine also supports data cache for scenarios where the source data has a storage time limit. When the cache is enabled, the data read from the source will be automatically cached, then read by the downstream task and written to the target. Under this condition, even if the data cannot be written due to the failure of the target, it will not affect the regular reading of the source, preventing the data from the source is deleted when expired.
- Space-saving, SeaTunnel Engine uses Dynamic Thread Sharing technology internally. In the real-time synchronization scenario, for the tables with a large amount but small data sizes per table, SeaTunnel Engine will run these synchronization tasks in shared threads to reduce unnecessary thread creation and save system space. On the reading and data writing side, the design goal of SeaTunnel Engine is to minimize the amount of JDBC connections; in CDC scenarios, SeaTunnel Engine will reuse log reading and parsing resources.
- Simple and easy to use, SeaTunnel Engine reduces the dependence on third-party services and can implement cluster management, snapshot storage, and cluster HA functions independently of big data components such as Zookeeper and HDFS. This is very useful for users who currently lack a big data platform, or are unwilling to rely on a big data platform for data synchronization.

In the future, SeaTunnel Engine will further optimize its functions to support full synchronization and incremental synchronization of offline batch synchronization, real-time synchronization, and CDC.

### Cluster Management

- Support standalone operation;
- Support cluster operation;
- Support autonomous cluster (decentralized), which saves the users from specifying a master node for the SeaTunnel Engine cluster, because it can select a master node by itself during operation, and a new master node will be chosen automatically when the master node fails.
- Autonomous Cluster nodes-discovery and nodes with the same cluster_name will automatically form a cluster.

### Core functions

- Support running jobs in local mode, and the cluster is automatically destroyed after the job once completed;
- Support running jobs in cluster mode (single machine or cluster), submitting jobs to the SeaTunnel Engine service through the SeaTunnel client, and the service continues to run after the job is completed and waits for the next job submission;
- Support offline batch synchronization;
- Support real-time synchronization;
- Batch-stream integration, all SeaTunnel V2 connectors can run in SeaTunnel Engine;
- Support distributed snapshot algorithm, and supports two-stage submission with SeaTunnel V2 connector, ensuring that data is executed only once.
- Support job invocation at the pipeline level to ensure that it can be started even when resources are limited;
- Support fault tolerance for jobs at the Pipeline level. Task failure only affects the pipeline where it is located, and only the task under the Pipeline needs to be rolled back;
- Support dynamic thread sharing to synchronize a large number of small data sets in real-time.

## Recommended Reading Path

If you are starting from zero, read these pages in order:

1. [Getting Started Overview](../../getting-started/overview.md)
2. [Download & Install](download-seatunnel.md)
3. [Quick Start With SeaTunnel Engine](../../getting-started/locally/quick-start-seatunnel-engine.md)
4. [Job Configuration Guide](../../getting-started/job-configuration-guide.md)
5. [REST API And Web UI](rest-api-and-web-ui.md)

If you want to understand the internals after your first successful run, continue with:

- [Architecture Overview](../../architecture/overview.md)
- [Checkpoint Mechanism](../../architecture/fault-tolerance/checkpoint-mechanism.md)
- [Resource Management](../../architecture/engine/resource-management.md)

## Download & Install

[Download & Install](download-seatunnel.md)
