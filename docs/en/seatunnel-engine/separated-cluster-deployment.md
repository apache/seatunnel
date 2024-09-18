---
sidebar_position: 6
---

# Deploy SeaTunnel Engine In Separated Cluster Mode

The Master service and Worker service of SeaTunnel Engine are separated, and each service is a separate process. The Master node is only responsible for job scheduling, RESTful API, task submission, etc., and the Imap data is only stored on the Master node. The Worker node is only responsible for the execution of tasks and does not participate in the election to become the master nor stores Imap data.

Among all the Master nodes, only one Master node works at the same time, and the other Master nodes are in the standby state. When the current Master node fails or the heartbeat times out, a new Master Active node will be elected from the other Master nodes.

This is the most recommended usage method. In this mode, the load on the Master will be very low, and the Master has more resources for job scheduling, task fault tolerance index monitoring, and providing RESTful API services, etc., and will have higher stability. At the same time, the Worker node does not store Imap data. All Imap data is stored on the Master node. Even if the Worker node has a high load or crashes, it will not cause the Imap data to be redistributed.

## 1. Download

[Download And Make SeaTunnel Installation Package](download-seatunnel.md)

## 2. Configure SEATUNNEL_HOME

You can configure `SEATUNNEL_HOME` by adding the `/etc/profile.d/seatunnel.sh` file. The content of `/etc/profile.d/seatunnel.sh` is as follows:

```
export SEATUNNEL_HOME=${seatunnel install path}
export PATH=$PATH:$SEATUNNEL_HOME/bin
```

## 3. Configure JVM Options For Master Nodes

The JVM parameters of the Master node are configured in the `$SEATUNNEL_HOME/config/jvm_master_options` file.

```shell
# JVM Heap
-Xms2g
-Xmx2g

# JVM Dump
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/tmp/seatunnel/dump/zeta-server

# Metaspace
-XX:MaxMetaspaceSize=2g

# G1GC
-XX:+UseG1GC
```

The JVM parameters of the Worker node are configured in the `$SEATUNNEL_HOME/config/jvm_worker_options` file.

```shell
# JVM Heap
-Xms2g
-Xmx2g

# JVM Dump
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/tmp/seatunnel/dump/zeta-server

# Metaspace
-XX:MaxMetaspaceSize=2g

# G1GC
-XX:+UseG1GC
```

## 4. Configure SeaTunnel Engine

SeaTunnel Engine provides many functions and needs to be configured in `seatunnel.yaml`.

### 4.1 Setting the backup number of data in Imap (this parameter is not effective on the Worker node)

SeaTunnel Engine implements cluster management based on [Hazelcast IMDG](https://docs.hazelcast.com/imdg/4.1/). The status data of the cluster (job running status, resource status) is stored in [Hazelcast IMap](https://docs.hazelcast.com/imdg/4.1/data-structures/map). The data stored in Hazelcast IMap will be distributed and stored on all nodes of the cluster. Hazelcast partitions the data stored in Imap. Each partition can specify the number of backups. Therefore, SeaTunnel Engine can achieve cluster HA without using other services (such as zookeeper).

The `backup count` is a parameter that defines the number of synchronous backups. For example, if it is set to 1, the backup of the partition will be placed on one other member. If it is set to 2, it will be placed on two other members.

We recommend that the value of `backup-count` be `min(1, max(5, N/2))`. `N` is the number of cluster nodes.

```yaml
seatunnel:
    engine:
        backup-count: 1
        # other configurations
```

:::tip

Since in the separated cluster mode, the Worker node does not store Imap data, the `backup-count` configuration of the Worker node is not effective. If the Master and Worker processes are started on the same machine, the Master and Worker will share the `seatunnel.yaml` configuration file. At this time, the Worker node service will ignore the `backup-count` configuration.

:::

### 4.2 Slot configuration (this parameter is not effective on the Master node)

The number of Slots determines the number of task groups that can be run in parallel on the cluster node. The number of Slots required by a task is formulated as N = 2 + P (parallelism configured by the task). By default, the number of Slots of SeaTunnel Engine is dynamic, that is, there is no limit on the number. We recommend that the number of Slots be set to twice the number of CPU cores of the node.

The configuration of dynamic slot number (default) is as follows:

```yaml
seatunnel:
    engine:
        slot-service:
            dynamic-slot: true
        # other configurations
```

The configuration of static slot number is as follows:

```yaml
seatunnel:
    engine:
        slot-service:
            dynamic-slot: false
            slot-num: 20
```

:::tip

Since in the separated cluster mode, the Master node does not run tasks, so the Master service will not start the Slot service, and the `slot-service` configuration of the Master node is not effective. If the Master and Worker processes are started on the same machine, the Master and Worker will share the `seatunnel.yaml` configuration file. At this time, the Master node service will ignore the `slot-service` configuration.

:::

### 4.3 Checkpoint Manager (This parameter is invalid on the Worker node)

Just like Flink, the SeaTunnel Engine supports the Chandy–Lamport algorithm. Therefore, data synchronization without data loss and duplication can be achieved.

**interval**

The interval between two checkpoints, in milliseconds. If the `checkpoint.interval` parameter is configured in the `env` of the job configuration file, it will be subject to the setting in the job configuration file.

**timeout**

The timeout time of the checkpoint. If the checkpoint cannot be completed within the timeout time, it will trigger a checkpoint failure and the job fails. If the `checkpoint.timeout` parameter is configured in the `env` of the job configuration file, it will be subject to the setting in the job configuration file.

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

The checkpoint is a fault-tolerant recovery mechanism. This mechanism ensures that when the program is running, even if it suddenly encounters an exception, it can recover by itself. The checkpoints are triggered regularly, and when each checkpoint is performed, each Task will be required to report its own state information (such as which offset has been read when reading Kafka) to the checkpoint thread, which writes it into a distributed storage (or shared storage). When the task fails and then automatically recovers from fault tolerance, or when recovering a previously paused task through the seatunnel.sh -r instruction, the state information of the corresponding job will be loaded from the checkpoint storage, and the job will be recovered based on these state information.

If the number of nodes in the cluster is greater than 1, the checkpoint storage must be a distributed storage or a shared storage, so as to ensure that the task state information stored in it can still be loaded on another node after any node fails.

:::tip

The checkpoint configuration is only read by the Master service, and the Worker service will not read the checkpoint configuration. If the Master and Worker processes are started on the same machine, the Master and Worker will share the `seatunnel.yaml` configuration file, and at this time the Worker node service will ignore the `checkpoint` configuration.

:::

For information about checkpoint storage, you can view [checkpoint storage](checkpoint-storage.md).

### 4.4 History Job Expiry Configuration

The information of each completed job, such as status, counters, and error logs, is stored in an IMap object. As the number of running jobs increases, the memory will increase, and eventually the memory will overflow. Therefore, you can adjust the `history-job-expire-minutes` parameter to solve this problem. The time unit of this parameter is minutes. The default value is 1440 minutes, that is, one day.

Example

```yaml
seatunnel:
  engine:
    history-job-expire-minutes: 1440
```

### 4.5 Class Loader Cache Mode

This configuration mainly solves the problem of resource leakage caused by continuously creating and attempting to destroy class loaders.
If you encounter an exception related to metaspace space overflow, you can try to enable this configuration.
In order to reduce the frequency of creating class loaders, after enabling this configuration, SeaTunnel will not try to release the corresponding class loader when the job is completed, so that it can be used by subsequent jobs, that is to say, when not too many types of Source/Sink connector are used in the running job, it is more effective.
The default value is false.
Example

```yaml
seatunnel:
  engine:
    classloader-cache-mode: true
```

### 4.6 Persistence Configuration of IMap (This parameter is invalid on the Worker node)

:::tip

Since in the separated cluster mode, only the Master node stores IMap data and the Worker node does not store IMap data, the Worker service will not read this parameter item.

:::

In SeaTunnel, we use IMap (a distributed Map that can implement the writing and reading of data across nodes and processes. For detailed information, please refer to [hazelcast map](https://docs.hazelcast.com/imdg/4.2/data-structures/map)) to store the state of each task and its task, so that after the node where the task is located fails, the state information of the task before can be obtained on other nodes, thereby recovering the task and realizing the fault tolerance of the task.

By default, the information of IMap is only stored in the memory, and we can set the number of replicas of IMap data. For specific reference (4.1 Setting the number of backups of data in IMap), if the number of replicas is 2, it means that each data will be simultaneously stored in 2 different nodes. Once the node fails, the data in IMap will be automatically replenished to the set number of replicas on other nodes. But when all nodes are stopped, the data in IMap will be lost. When the cluster nodes are started again, all previously running tasks will be marked as failed and need to be recovered manually by the user through the seatunnel.sh -r instruction.

To solve this problem, we can persist the data in IMap to an external storage such as HDFS, OSS, etc. In this way, even if all nodes are stopped, the data in IMap will not be lost, and when the cluster nodes are started again, all previously running tasks will be automatically recovered.

The following describes how to use the MapStore persistence configuration. For detailed information, please refer to [hazelcast map](https://docs.hazelcast.com/imdg/4.2/data-structures/map)

**type**

The type of IMap persistence, currently only supports `hdfs`.

**namespace**

It is used to distinguish the data storage locations of different businesses, such as the OSS bucket name.

**clusterName**

This parameter is mainly used for cluster isolation. We can use it to distinguish different clusters, such as cluster1, cluster2, which is also used to distinguish different businesses.

**fs.defaultFS**

We use the hdfs api to read and write files, so providing the hdfs configuration is required for using this storage.

If you use HDFS, you can configure it like this:

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

If there is no HDFS and your cluster has only one node, you can configure it like this to use local files:

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

If you use OSS, you can configure it like this:

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

### 4.7 Job Scheduling Strategy

When resources are insufficient, the job scheduling strategy can be configured in the following two modes:

1. `WAIT`: Wait for resources to be available.

2. `REJECT`: Reject the job, default value.

Example

```yaml
seatunnel:
engine:
job-schedule-strategy: WAIT
```

## 5. Configuring SeaTunnel Engine Network Services

All network-related configurations of the SeaTunnel Engine are in the `hazelcast-master.yaml` and `hazelcast-worker.yaml` files.

### 5.1 cluster-name

SeaTunnel Engine nodes use the `cluster-name` to determine whether another node is in the same cluster as themselves. If the cluster names between two nodes are different, the SeaTunnel Engine will reject service requests.

### 5.2 network

Based on [Hazelcast](https://docs.hazelcast.com/imdg/4.1/clusters/discovery-mechanisms), a SeaTunnel Engine cluster is a network composed of cluster members running the SeaTunnel Engine server. Cluster members automatically join together to form a cluster. This automatic joining is through the various discovery mechanisms used by cluster members to discover each other.

Please note that after the cluster is formed, the communication between cluster members is always through TCP/IP regardless of the discovery mechanism used.

The SeaTunnel Engine uses the following discovery mechanisms.

#### tcp-ip

You can configure the SeaTunnel Engine as a complete TCP/IP cluster. For configuration details, please refer to the [Discovering Members by TCP section](tcp.md).

In the separated cluster mode, the Master and Worker services use different ports.

Master node network configuration `hazelcast-master.yaml`

```yaml
hazelcast:
  cluster-name: seatunnel
  network:
    rest-api:
      enabled: true
      endpoint-groups:
        CLUSTER_WRITE:
          enabled: true
        DATA:
          enabled: true
    join:
      tcp-ip:
        enabled: true
        member-list:
          - master-node-1:5801
          - master-node-2:5801
          - worker-node-1:5802
          - worker-node-2:5802
    port:
      auto-increment: false
      port: 5801
  properties:
    hazelcast.heartbeat.failuredetector.type: phi-accrual
    hazelcast.heartbeat.interval.seconds: 2
    hazelcast.max.no.heartbeat.seconds: 180
    hazelcast.heartbeat.phiaccrual.failuredetector.threshold: 10
    hazelcast.heartbeat.phiaccrual.failuredetector.sample.size: 200
    hazelcast.heartbeat.phiaccrual.failuredetector.min.std.dev.millis: 100
```

Worker node network configuration `hazelcast-worker.yaml`

```yaml
hazelcast:
  cluster-name: seatunnel
  network:
    join:
      tcp-ip:
        enabled: true
        member-list:
          - master-node-1:5801
          - master-node-2:5801
          - worker-node-1:5802
          - worker-node-2:5802
    port:
      auto-increment: false
      port: 5802
  properties:
    hazelcast.heartbeat.failuredetector.type: phi-accrual
    hazelcast.heartbeat.interval.seconds: 2
    hazelcast.max.no.heartbeat.seconds: 180
    hazelcast.heartbeat.phiaccrual.failuredetector.threshold: 10
    hazelcast.heartbeat.phiaccrual.failuredetector.sample.size: 200
    hazelcast.heartbeat.phiaccrual.failuredetector.min.std.dev.millis: 100
```

TCP is the way we recommend to use in a standalone SeaTunnel Engine cluster.

On the other hand, Hazelcast provides some other service discovery methods. For details, please refer to [hazelcast network](https://docs.hazelcast.com/imdg/4.1/clusters/setting-up-clusters).

## 6. Starting the SeaTunnel Engine Master Node

It can be started using the `-d` parameter through the daemon.

```shell
mkdir -p $SEATUNNEL_HOME/logs
./bin/seatunnel-cluster.sh -d -r master
```

The logs will be written to `$SEATUNNEL_HOME/logs/seatunnel-engine-master.log`.

## 7. Starting The SeaTunnel Engine Worker Node

It can be started using the `-d` parameter through the daemon.

```shell
mkdir -p $SEATUNNEL_HOME/logs
./bin/seatunnel-cluster.sh -d -r worker
```

The logs will be written to `$SEATUNNEL_HOME/logs/seatunnel-engine-worker.log`.

## 8. Installing The SeaTunnel Engine Client

### 8.1 Setting the `SEATUNNEL_HOME` the same as the server

You can configure the `SEATUNNEL_HOME` by adding the `/etc/profile.d/seatunnel.sh` file. The content of `/etc/profile.d/seatunnel.sh` is as follows:

```
export SEATUNNEL_HOME=${seatunnel install path}
export PATH=$PATH:$SEATUNNEL_HOME/bin
```

### 8.2 Configuring The SeaTunnel Engine Client

All configurations of the SeaTunnel Engine client are in the `hazelcast-client.yaml`.

**cluster-name**

The client must have the same `cluster-name` as the SeaTunnel Engine. Otherwise, the SeaTunnel Engine will reject the client's request.

**network**

All addresses of the SeaTunnel Engine Master nodes need to be added here.

```yaml
hazelcast-client:
  cluster-name: seatunnel
  properties:
    hazelcast.logging.type: log4j2
  network:
    cluster-members:
      - master-node-1:5801
      - master-node-2:5801
```

# 9 Submitting And Managing Jobs

Now that the cluster has been deployed, you can complete the job submission and management through the following tutorial: [Submitting And Managing Jobs](user-command.md).
