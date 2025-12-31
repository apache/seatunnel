---
sidebar_position: 1
---

# SeaTunnel Engine

SeaTunnel Engine is a community-developed data synchronization engine designed for data synchronization scenarios debuts. As the default engine of SeaTunnel, it supports high-throughput, low-latency, and strong-consistent synchronous job operation, which is faster, more stable, more resource-saving, and easy to use.

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

### Quick Start

https://seatunnel.apache.org/docs/start-v2/locally/quick-start-seatunnel-engine

### Download & Install

[Download & Install](download-seatunnel.md)
