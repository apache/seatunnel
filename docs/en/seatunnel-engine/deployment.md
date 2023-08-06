---

sidebar_position: 4
-------------------

# Deployment SeaTunnel Engine

## 1. Download

SeaTunnel Engine is the default engine of SeaTunnel. The installation package of SeaTunnel already contains all the contents of SeaTunnel Engine.

## 2 Config SEATUNNEL_HOME

You can config `SEATUNNEL_HOME` by add `/etc/profile.d/seatunnel.sh` file. The content of `/etc/profile.d/seatunnel.sh` are

```
export SEATUNNEL_HOME=${seatunnel install path}
export PATH=$PATH:$SEATUNNEL_HOME/bin
```

## 3. Config SeaTunnel Engine JVM options

SeaTunnel Engine supported two ways to set jvm options.

1. Add JVM Options to `$SEATUNNEL_HOME/bin/seatunnel-cluster.sh`.

   Modify the `$SEATUNNEL_HOME/bin/seatunnel-cluster.sh` file and add `JAVA_OPTS="-Xms2G -Xmx2G"` in the first line.

2. Add JVM Options when start SeaTunnel Engine. For example `seatunnel-cluster.sh -DJvmOption="-Xms2G -Xmx2G"`

## 4. Config SeaTunnel Engine

SeaTunnel Engine provides many functions, which need to be configured in seatunnel.yaml.

### 4.1 Backup count

SeaTunnel Engine implement cluster management based on [Hazelcast IMDG](https://docs.hazelcast.com/imdg/4.1/). The state data of cluster(Job Running State, Resource State) are storage is [Hazelcast IMap](https://docs.hazelcast.com/imdg/4.1/data-structures/map).
The data saved in Hazelcast IMap will be distributed and stored in all nodes of the cluster. Hazelcast will partition the data stored in Imap. Each partition can specify the number of backups.
Therefore, SeaTunnel Engine can achieve cluster HA without using other services(for example zookeeper).

The `backup count` is to define the number of synchronous backups. For example, if it is set to 1, backup of a partition will be placed on one other member. If it is 2, it will be placed on two other members.

We suggest the value of `backup-count` is the `min(1, max(5, N/2))`. `N` is the number of the cluster node.

```
seatunnel:
    engine:
        backup-count: 1
        # other config
```

### 4.2 Slot service

The number of Slots determines the number of TaskGroups the cluster node can run in parallel. SeaTunnel Engine is a data synchronization engine and most jobs are IO intensive.

Dynamic Slot is suggest.

```
seatunnel:
    engine:
        slot-service:
            dynamic-slot: true
        # other config
```

### 4.3 Checkpoint Manager

Like Flink, SeaTunnel Engine support Chandy–Lamport algorithm. Therefore, SeaTunnel Engine can realize data synchronization without data loss and duplication.

**interval**

The interval between two checkpoints, unit is milliseconds. If the `checkpoint.interval` parameter is configured in the `env` of the job config file, the value set here will be overwritten.

**timeout**

The timeout of a checkpoint. If a checkpoint cannot be completed within the timeout period, a checkpoint failure will be triggered. Therefore, Job will be restored.

**max-concurrent**

How many checkpoints can be performed simultaneously at most.

**tolerable-failure**

Maximum number of retries after checkpoint failure.

Example

```
seatunnel:
    engine:
        backup-count: 1
        print-execution-info-interval: 10
        slot-service:
            dynamic-slot: true
        checkpoint:
            interval: 300000
            timeout: 10000
            max-concurrent: 1
            tolerable-failure: 2
```

**checkpoint storage**

About the checkpoint storage, you can see [checkpoint storage](checkpoint-storage.md)

## 5. Config SeaTunnel Engine Server

All SeaTunnel Engine Server config in `hazelcast.yaml` file.

### 5.1 cluster-name

The SeaTunnel Engine nodes use the cluster name to determine whether the other is a cluster with themselves. If the cluster names between the two nodes are different, the SeaTunnel Engine will reject the service request.

### 5.2 Network

Base on [Hazelcast](https://docs.hazelcast.com/imdg/4.1/clusters/discovery-mechanisms), A SeaTunnel Engine cluster is a network of cluster members that run SeaTunnel Engine Server. Cluster members automatically join together to form a cluster. This automatic joining takes place with various discovery mechanisms that the cluster members use to find each other.

Please note that, after a cluster is formed, communication between cluster members is always via TCP/IP, regardless of the discovery mechanism used.

SeaTunnel Engine uses the following discovery mechanisms.

#### TCP

You can configure SeaTunnel Engine to be a full TCP/IP cluster. See the [Discovering Members by TCP section](tcp.md) for configuration details.

An example is like this `hazelcast.yaml`

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

TCP is our suggest way in a standalone SeaTunnel Engine cluster.

On the other hand, Hazelcast provides some other service discovery methods. For details, please refer to [hazelcast network](https://docs.hazelcast.com/imdg/4.1/clusters/setting-up-clusters)

### 5.3 Map

MapStores connect to an external data store only when they are configured on a map. This topic explains how to configure a map with a MapStore. For details, please refer to [hazelcast map](https://docs.hazelcast.com/imdg/4.2/data-structures/map)

**type**

The type of imap persistence, currently only supports `hdfs`.

**namespace**

It is used to distinguish data storage locations of different business, like OSS bucket name.

**clusterName**

This parameter is primarily used for cluster isolation, we can use this to distinguish different cluster, like cluster1,
cluster2 and this is also used to distinguish different business

**fs.defaultFS**

We used hdfs api read/write file, so used this storage need provide hdfs configuration

if you used HDFS, you can config like this:

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

If there is no HDFS and your cluster only have one node, you can config to use local file like this:

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

if you used OSS, you can config like this:

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
           fs.oss.credentials.provider: org.apache.hadoop.fs.aliyun.oss.AliyunCredentialsProvider
```

## 6. Config SeaTunnel Engine Client

All SeaTunnel Engine Client config in `hazelcast-client.yaml`.

### 6.1 cluster-name

The Client must have the same `cluster-name` with the SeaTunnel Engine. Otherwise, SeaTunnel Engine will reject the client request.

### 6.2 Network

**cluster-members**

All SeaTunnel Engine Server Node address need add to here.

```yaml
hazelcast-client:
  cluster-name: seatunnel
  properties:
      hazelcast.logging.type: log4j2
  network:
    cluster-members:
      - hostname1:5801
```

## 7. Start SeaTunnel Engine Server Node

Can be started by a daemon with `-d`.

```shell
mkdir -p $SEATUNNEL_HOME/logs
./bin/seatunnel-cluster.sh -d
```

The logs will write in `$SEATUNNEL_HOME/logs/seatunnel-engine-server.log`

## 8. Install SeaTunnel Engine Client

You only need to copy the `$SEATUNNEL_HOME` directory on the SeaTunnel Engine node to the Client node and config the `SEATUNNEL_HOME` like SeaTunnel Engine Server Node.

