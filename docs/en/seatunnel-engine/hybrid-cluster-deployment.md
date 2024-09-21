---
sidebar_position: 5
---

# Deploy SeaTunnel Engine Hybrid Mode Cluster

The Master service and Worker service of SeaTunnel Engine are mixed in the same process, and all nodes can run jobs and participate in the election to become master. The master node is also running synchronous tasks simultaneously. In this mode, the Imap (which saves the status information of the task to provide support for the task's fault tolerance) data will be distributed across all nodes.

Usage Recommendation: It is recommended to use the [Separated Cluster Mode](separated-cluster-deployment.md). In the hybrid cluster mode, the Master node needs to run tasks synchronously. When the task scale is large, it will affect the stability of the Master node. Once the Master node crashes or the heartbeat times out, it will cause the Master node to switch, and the Master node switch will cause all running tasks to perform fault tolerance, further increasing the load on the cluster. Therefore, we recommend using the [Separated Cluster Mode](separated-cluster-deployment.md).

## 1. Download

[Download And Create The SeaTunnel Installation Package](download-seatunnel.md)

## 2. Configure SEATUNNEL_HOME

You can configure `SEATUNNEL_HOME` by adding the `/etc/profile.d/seatunnel.sh` file. The content of `/etc/profile.d/seatunnel.sh` is as follows:

```
export SEATUNNEL_HOME=${seatunnel install path}
export PATH=$PATH:$SEATUNNEL_HOME/bin
```

## 3. Configure The JVM Options For The SeaTunnel Engine

The SeaTunnel Engine supports two methods for setting JVM options:

1. Add the JVM options to `$SEATUNNEL_HOME/config/jvm_options`.

   Modify the JVM parameters in the `$SEATUNNEL_HOME/config/jvm_options` file.

2. Add JVM options when starting the SeaTunnel Engine. For example, `seatunnel-cluster.sh -DJvmOption="-Xms2G -Xmx2G"`

## 4. Configure The SeaTunnel Engine

The SeaTunnel Engine provides many functions that need to be configured in the `seatunnel.yaml` file.

### 4.1 Backup Count Setting For Data In Imap

The SeaTunnel Engine implements cluster management based on [Hazelcast IMDG](https://docs.hazelcast.com/imdg/4.1/). The cluster's status data (job running status, resource status) is stored in the [Hazelcast IMap](https://docs.hazelcast.com/imdg/4.1/data-structures/map).
The data stored in the Hazelcast IMap is distributed and stored on all nodes in the cluster. Hazelcast partitions the data stored in the Imap. Each partition can specify the number of backups.
Therefore, the SeaTunnel Engine can implement cluster HA without using other services (such as Zookeeper).

`backup count` is a parameter that defines the number of synchronous backups. For example, if it is set to 1, the backup of the partition will be placed on one other member. If it is set to 2, it will be placed on two other members.

We recommend that the value of `backup count` be `min(1, max(5, N/2))`. `N` is the number of cluster nodes.

```yaml
seatunnel:
    engine:
        backup-count: 1
        # Other configurations
```

### 4.2 Slot Configuration

The number of slots determines the number of task groups that the cluster node can run in parallel. The formula for the number of slots required for a task is N = 2 + P (the parallelism configured by the task). By default, the number of slots in the SeaTunnel Engine is dynamic, that is, there is no limit on the number. We recommend that the number of slots be set to twice the number of CPU cores on the node.

Configuration of dynamic slot number (default):

```yaml
seatunnel:
    engine:
        slot-service:
            dynamic-slot: true
        # Other configurations
```

Configuration of static slot number:

```yaml
seatunnel:
    engine:
        slot-service:
            dynamic-slot: false
            slot-num: 20
```

### 4.3 Checkpoint Manager

Like Flink, the SeaTunnel Engine supports the Chandy–Lamport algorithm. Therefore, it is possible to achieve data synchronization without data loss and duplication.

**interval**

The interval between two checkpoints, in milliseconds. If the `checkpoint.interval` parameter is configured in the job configuration file's `env`, the one set in the job configuration file will be used.

**timeout**

The timeout for checkpoints. If the checkpoint cannot be completed within the timeout, a checkpoint failure will be triggered and the job will fail. If the `checkpoint.timeout` parameter is configured in the job configuration file's `env`, the one set in the job configuration file will be used.

Example

```yaml
seatunnel:
    engine:
        backup-count: 1
        print-execution-info-interval: 10
        slot-service:
            dynamic-slot: true
        checkpoint:
            interval: 300000
            timeout: 10000
```

**checkpoint storage**

Checkpoints are a fault-tolerant recovery mechanism. This mechanism ensures that the program can recover on its own even if an exception occurs suddenly during operation. Checkpoints are triggered at regular intervals. Each time a checkpoint is performed, each task is required to report its own status information (such as which offset was read when reading from Kafka) to the checkpoint thread, which writes it to a distributed storage (or shared storage). When a task fails and is automatically fault-tolerant and restored, or when a previously suspended task is restored using the seatunnel.sh -r command, the status information of the corresponding job will be loaded from the checkpoint storage and the job will be restored based on this status information.

If the cluster has more than one node, the checkpoint storage must be a distributed storage or shared storage so that the task status information in the storage can be loaded on another node in case of a node failure.

For information about checkpoint storage, you can refer to [Checkpoint Storage](checkpoint-storage.md)

### 4.4 Expiration Configuration For Historical Jobs

The information of each completed job, such as status, counters, and error logs, is stored in the IMap object. As the number of running jobs increases, the memory usage will increase, and eventually, the memory will overflow. Therefore, you can adjust the `history-job-expire-minutes` parameter to address this issue. The time unit for this parameter is minutes. The default value is 1440 minutes, which is one day.

Example

```yaml
seatunnel:
  engine:
    history-job-expire-minutes: 1440
```

### 4.5 Class Loader Cache Mode

This configuration primarily addresses the issue of resource leakage caused by constantly creating and attempting to destroy the class loader.
If you encounter exceptions related to metaspace overflow, you can try enabling this configuration.
To reduce the frequency of class loader creation, after enabling this configuration, SeaTunnel will not attempt to release the corresponding class loader when a job is completed, allowing it to be used by subsequent jobs. This is more effective when the number of Source/Sink connectors used in the running job is not excessive.
The default value is false.
Example

```yaml
seatunnel:
  engine:
    classloader-cache-mode: true
```

### 4.6 Job Scheduling Strategy

When resources are insufficient, the job scheduling strategy can be configured in the following two modes:

1. `WAIT`: Wait for resources to be available.

2. `REJECT`: Reject the job, default value.

Example

```yaml
seatunnel:
  engine:
    job-schedule-strategy: WAIT
```

## 5. Configure The SeaTunnel Engine Network Service

All SeaTunnel Engine network-related configurations are in the `hazelcast.yaml` file.

### 5.1 Cluster Name

The SeaTunnel Engine node uses the `cluster-name` to determine if another node is in the same cluster as itself. If the cluster names of the two nodes are different, the SeaTunnel Engine will reject the service request.

### 5.2 Network

Based on [Hazelcast](https://docs.hazelcast.com/imdg/4.1/clusters/discovery-mechanisms), a SeaTunnel Engine cluster is a network composed of cluster members running the SeaTunnel Engine server. Cluster members automatically join together to form a cluster. This automatic joining occurs through various discovery mechanisms used by cluster members to detect each other.

Please note that once the cluster is formed, communication between cluster members always occurs via TCP/IP, regardless of the discovery mechanism used.

The SeaTunnel Engine utilizes the following discovery mechanisms:

#### TCP

You can configure the SeaTunnel Engine as a full TCP/IP cluster. For detailed configuration information, please refer to the [Discovering Members by TCP section](tcp.md).

An example `hazelcast.yaml` file is as follows:

```yaml
hazelcast:
  cluster-name: seatunnel
  network:
    join:
      tcp-ip:
        enabled: true
        member-list:
          - hostname1
    port:
      auto-increment: false
      port: 5801
  properties:
    hazelcast.logging.type: log4j2
```

TCP is the recommended method for use in a standalone SeaTunnel Engine cluster.

Alternatively, Hazelcast provides several other service discovery methods. For more details, please refer to [Hazelcast Network](https://docs.hazelcast.com/imdg/4.1/clusters/setting-up-clusters)

### 5.3 IMap Persistence Configuration

In SeaTunnel, we use IMap (a distributed Map that enables the writing and reading of data across nodes and processes. For more information, please refer to [hazelcast map](https://docs.hazelcast.com/imdg/4.2/data-structures/map)) to store the status of each task and task, allowing us to recover tasks and achieve task fault tolerance in the event of a node failure.

By default, the information in Imap is only stored in memory. We can set the replica count for Imap data. For more details, please refer to (4.1 Backup count setting for data in Imap). If the replica count is set to 2, it means that each data will be stored in two different nodes simultaneously. In the event of a node failure, the data in Imap will be automatically replenished to the set replica count on other nodes. However, when all nodes are stopped, the data in Imap will be lost. When the cluster nodes are restarted, all previously running tasks will be marked as failed, and users will need to manually resume them using the seatunnel.sh -r command.

To address this issue, we can persist the data in Imap to an external storage such as HDFS or OSS. This way, even if all nodes are stopped, the data in Imap will not be lost. When the cluster nodes are restarted, all previously running tasks will be automatically restored.

The following describes how to use the MapStore persistence configuration. For more details, please refer to [hazelcast map](https://docs.hazelcast.com/imdg/4.2/data-structures/map)

**type**

The type of IMap persistence, currently only supporting `hdfs`.

**namespace**

It is used to distinguish the storage location of different business data, such as the name of an OSS bucket.

**clusterName**

This parameter is mainly used for cluster isolation, allowing you to distinguish between different clusters, such as cluster1 and cluster2, and can also be used to distinguish different business data.

**fs.defaultFS**

We use the hdfs api to read and write files, so providing the hdfs configuration is required for using this storage.

If using HDFS, you can configure it as follows:

```yaml
map:
    engine*:
       map-store:
         enabled: true
         initial-mode: EAGER
         factory-class-name: org.apache.seatunnel.engine.server.persistence.FileMapStoreFactory
         properties:
           type: hdfs
           namespace: /tmp/seatunnel/imap
           clusterName: seatunnel-cluster
           storage.type: hdfs
           fs.defaultFS: hdfs://localhost:9000
```

If there is no HDFS and the cluster has only one node, you can configure it to use local files as follows:

```yaml
map:
    engine*:
       map-store:
         enabled: true
         initial-mode: EAGER
         factory-class-name: org.apache.seatunnel.engine.server.persistence.FileMapStoreFactory
         properties:
           type: hdfs
           namespace: /tmp/seatunnel/imap
           clusterName: seatunnel-cluster
           storage.type: hdfs
           fs.defaultFS: file:///
```

If using OSS, you can configure it as follows:

```yaml
map:
    engine*:
       map-store:
         enabled: true
         initial-mode: EAGER
         factory-class-name: org.apache.seatunnel.engine.server.persistence.FileMapStoreFactory
         properties:
           type: hdfs
           namespace: /tmp/seatunnel/imap
           clusterName: seatunnel-cluster
           storage.type: oss
           block.size: block size(bytes)
           oss.bucket: oss://bucket name/
           fs.oss.accessKeyId: OSS access key id
           fs.oss.accessKeySecret: OSS access key secret
           fs.oss.endpoint: OSS endpoint
```

Notice: When using OSS, make sure that the following jars are in the lib directory.

```
aliyun-sdk-oss-3.13.2.jar
hadoop-aliyun-3.3.6.jar
jdom2-2.0.6.jar
netty-buffer-4.1.89.Final.jar 
netty-common-4.1.89.Final.jar
seatunnel-hadoop3-3.1.4-uber.jar
```

## 6. Configure The SeaTunnel Engine Client

All SeaTunnel Engine client configurations are in the `hazelcast-client.yaml`.

### 6.1 cluster-name

The client must have the same `cluster-name` as the SeaTunnel Engine. Otherwise, the SeaTunnel Engine will reject the client's request.

### 6.2 network

**cluster-members**

You need to add the addresses of all SeaTunnel Engine server nodes here.

```yaml
hazelcast-client:
  cluster-name: seatunnel
  properties:
      hazelcast.logging.type: log4j2
  network:
    cluster-members:
      - hostname1:5801
```

## 7. Start The SeaTunnel Engine Server Node

It can be started with the `-d` parameter through the daemon.

```shell
mkdir -p $SEATUNNEL_HOME/logs
./bin/seatunnel-cluster.sh -d
```

The logs will be written to `$SEATUNNEL_HOME/logs/seatunnel-engine-server.log`

## 8. Install The SeaTunnel Engine Client

You only need to copy the `$SEATUNNEL_HOME` directory on the SeaTunnel Engine node to the client node and configure `SEATUNNEL_HOME` in the same way as the SeaTunnel Engine server node.

## 9. Submit And Manage Jobs

Now that the cluster is deployed, you can complete the submission and management of jobs through the following tutorials: [Submit And Manage Jobs](user-command.md)
