---

sidebar_position: 2
-------------------

# Run Job With Cluster Mode

This is the most recommended way to use SeaTunnel Engine in the production environment. Full functionality of SeaTunnel Engine is supported in this mode and the cluster mode will have better performance and stability.

In the cluster mode, the SeaTunnel Engine cluster needs to be deployed first, and the client will submit the job to the SeaTunnel Engine cluster for running.

## Deploy SeaTunnel Engine Cluster

### 1. Download

SeaTunnel Engine is the default engine of SeaTunnel. The installation package of SeaTunnel already contains all the contents of SeaTunnel Engine.

### 2 Config SEATUNNEL_HOME

You can config `SEATUNNEL_HOME` by add `/etc/profile.d/seatunnel.sh` file. The content of `/etc/profile.d/seatunnel.sh` are

```
export SEATUNNEL_HOME=${seatunnel install path}
export PATH=$PATH:$SEATUNNEL_HOME/bin
```

### 3. Config SeaTunnel Engine JVM options

SeaTunnel Engine supported two ways to set jvm options.

1. Add JVM Options to `$SEATUNNEL_HOME/bin/seatunnel-cluster.sh`.

   Modify the `$SEATUNNEL_HOME/bin/seatunnel-cluster.sh` file and add `JAVA_OPTS="-Xms2G -Xmx2G"` in the first line.

2. Add JVM Options when start SeaTunnel Engine. For example `seatunnel-cluster.sh -DJvmOption="-Xms2G -Xmx2G"`

### 4. Config SeaTunnel Engine

SeaTunnel Engine provides many functions, which need to be configured in seatunnel.yaml.

#### 4.1 Backup count

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

#### 4.2 Slot service

The number of Slots determines the number of TaskGroups the cluster node can run in parallel. SeaTunnel Engine is a data synchronization engine and most jobs are IO intensive.

Dynamic Slot is suggest.

```
seatunnel:
    engine:
        slot-service:
            dynamic-slot: true
        # other config
```

#### 4.3 Checkpoint Manager

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

#### Introduction

Checkpoint is a fault-tolerant recovery mechanism. This mechanism ensures that when the program is running, it can recover itself even if it suddenly encounters an exception.

##### Checkpoint Storage

Checkpoint Storage is a storage mechanism for storing checkpoint data.

SeaTunnel Engine supports the following checkpoint storage types:

- HDFS (OSS,S3,HDFS,LocalFile)
- LocalFile (native), (it's deprecated: use Hdfs(LocalFile) instead.

We used the microkernel design pattern to separate the checkpoint storage module from the engine. This allows users to implement their own checkpoint storage modules.

`checkpoint-storage-api` is the checkpoint storage module API, which defines the interface of the checkpoint storage module.

if you want to implement your own checkpoint storage module, you need to implement the `CheckpointStorage` and provide the corresponding `CheckpointStorageFactory` implementation.

##### Checkpoint Storage Configuration

The configuration of the `seatunnel-server` module is in the `seatunnel.yaml` file.

```yaml

seatunnel:
    engine:
        checkpoint:
            storage:
                type: hdfs #plugin name of checkpoint storage, we support hdfs(S3, local, hdfs), localfile (native local file) is the default, but this plugin is de
              # plugin configuration
                plugin-config: 
                  namespace: #checkpoint storage parent path, the default value is /seatunnel/checkpoint/
                  K1: V1 # plugin other configuration
                  K2: V2 # plugin other configuration   
```

Notice: namespace must end with "/".

###### OSS

Aliyun oss base on hdfs-file, so you can refer [hadoop oss docs](https://hadoop.apache.org/docs/stable/hadoop-aliyun/tools/hadoop-aliyun/index.html) to config oss.

Except when interacting with oss buckets, the oss client needs the credentials needed to interact with buckets.
The client supports multiple authentication mechanisms and can be configured as to which mechanisms to use, and their order of use. Custom implementations of org.apache.hadoop.fs.aliyun.oss.AliyunCredentialsProvider may also be used.
if you used AliyunCredentialsProvider (can be obtained from the Aliyun Access Key Management), these consist of an access key, a secret key.
you can config like this:

```yaml
seatunnel:
  engine:
    checkpoint:
      interval: 6000
      timeout: 7000
      max-concurrent: 5
      tolerable-failure: 2
      storage:
        type: hdfs
        max-retained: 3
        plugin-config:
          storage.type: oss
          oss.bucket: your-bucket
          fs.oss.accessKeyId: your-access-key
          fs.oss.accessKeySecret: your-secret-key
          fs.oss.endpoint: endpoint address
          fs.oss.credentials.provider: org.apache.hadoop.fs.aliyun.oss.AliyunCredentialsProvider
```

For additional reading on the Hadoop Credential Provider API see: [Credential Provider API](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/CredentialProviderAPI.html).

Aliyun oss Credential Provider implements see: [Auth Credential Providers](https://github.com/aliyun/aliyun-oss-java-sdk/tree/master/src/main/java/com/aliyun/oss/common/auth)

###### S3

S3 base on hdfs-file, so you can refer [hadoop s3 docs](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html) to config s3.

Except when interacting with public S3 buckets, the S3A client needs the credentials needed to interact with buckets.
The client supports multiple authentication mechanisms and can be configured as to which mechanisms to use, and their order of use. Custom implementations of com.amazonaws.auth.AWSCredentialsProvider may also be used.
if you used SimpleAWSCredentialsProvider (can be obtained from the Amazon Security Token Service), these consist of an access key, a secret key.
you can config like this:

```yaml
``` yaml

seatunnel:
    engine:
        checkpoint:
            interval: 6000
            timeout: 7000
            max-concurrent: 5
            tolerable-failure: 2
            storage:
                type: hdfs
                max-retained: 3
                plugin-config:
                    storage.type: s3
                    s3.bucket: your-bucket
                    fs.s3a.access.key: your-access-key
                    fs.s3a.secret.key: your-secret-key
                    fs.s3a.aws.credentials.provider: org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
                    

```

if you used `InstanceProfileCredentialsProvider`, this supports use of instance profile credentials if running in an EC2 VM, you could check [iam-roles-for-amazon-ec2](https://docs.aws.amazon.com/zh_cn/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html).
you can config like this:

```yaml

seatunnel:
  engine:
    checkpoint:
      interval: 6000
      timeout: 7000
      max-concurrent: 5
      tolerable-failure: 2
      storage:
        type: hdfs
        max-retained: 3
        plugin-config:
          storage.type: s3
          s3.bucket: your-bucket
          fs.s3a.endpoint: your-endpoint
          fs.s3a.aws.credentials.provider: org.apache.hadoop.fs.s3a.InstanceProfileCredentialsProvider
```

For additional reading on the Hadoop Credential Provider API see: [Credential Provider API](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/CredentialProviderAPI.html).

###### HDFS

if you used HDFS, you can config like this:

```yaml
seatunnel:
  engine:
    checkpoint:
      storage:
        type: hdfs
        max-retained: 3
        plugin-config:
          storage.type: hdfs
          fs.defaultFS: hdfs://localhost:9000
          // if you used kerberos, you can config like this:
          kerberosPrincipal: your-kerberos-principal
          kerberosKeytab: your-kerberos-keytab  
```

###### LocalFile

```yaml
seatunnel:
  engine:
    checkpoint:
      interval: 6000
      timeout: 7000
      max-concurrent: 5
      tolerable-failure: 2
      storage:
        type: hdfs
        max-retained: 3
        plugin-config:
          storage.type: hdfs
          fs.defaultFS: file:/// # Ensure that the directory has written permission 

```



### 5. Config SeaTunnel Engine Server

All SeaTunnel Engine Server config in `hazelcast.yaml` file.

#### 5.1 cluster-name

The SeaTunnel Engine nodes use the cluster name to determine whether the other is a cluster with themselves. If the cluster names between the two nodes are different, the SeaTunnel Engine will reject the service request.

#### 5.2 Network

Base on [Hazelcast](https://docs.hazelcast.com/imdg/4.1/clusters/discovery-mechanisms), A SeaTunnel Engine cluster is a network of cluster members that run SeaTunnel Engine Server. Cluster members automatically join together to form a cluster. This automatic joining takes place with various discovery mechanisms that the cluster members use to find each other.

Please note that, after a cluster is formed, communication between cluster members is always via TCP/IP, regardless of the discovery mechanism used.

SeaTunnel Engine uses the following discovery mechanisms.

##### TCP NetWork

If multicast is not the preferred way of discovery for your environment, then you can configure SeaTunnel Engine to be a full TCP/IP cluster. When you configure SeaTunnel Engine to discover members by TCP/IP, you must list all or a subset of the members' host names and/or IP addresses as cluster members. You do not have to list all of these cluster members, but at least one of the listed members has to be active in the cluster when a new member joins.

To configure your Hazelcast to be a full TCP/IP cluster, set the following configuration elements. See the tcp-ip element section for the full descriptions of the TCP/IP discovery configuration elements.

- Set the enabled attribute of the tcp-ip element to true.
- Provide your member elements within the tcp-ip element.

The following is an example declarative configuration.

```yaml
hazelcast:
  network:
    join:
      tcp-ip:
        enabled: true
        member-list:
          - machine1
          - machine2
          - machine3:5799
          - 192.168.1.0-7
          - 192.168.1.21
```

As shown above, you can provide IP addresses or host names for member elements. You can also give a range of IP addresses, such as `192.168.1.0-7`.

Instead of providing members line-by-line as shown above, you also have the option to use the members element and write comma-separated IP addresses, as shown below.

`<members>192.168.1.0-7,192.168.1.21</members>`

If you do not provide ports for the members, Hazelcast automatically tries the ports `5701`, `5702` and so on.


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

#### 5.3 Map

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
           fs.defaultFS: file:///
```

### 6. Config SeaTunnel Engine Client

All SeaTunnel Engine Client config in `hazelcast-client.yaml`.

#### 6.1 cluster-name

The Client must have the same `cluster-name` with the SeaTunnel Engine. Otherwise, SeaTunnel Engine will reject the client request.

#### 6.2 Network

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

### 7. Start SeaTunnel Engine Server Node

Can be started by a daemon with `-d`.

```shell
mkdir -p $SEATUNNEL_HOME/logs
./bin/seatunnel-cluster.sh -d
```

The logs will write in `$SEATUNNEL_HOME/logs/seatunnel-engine-server.log`

### 8. Install SeaTunnel Engine Client

You only need to copy the `$SEATUNNEL_HOME` directory on the SeaTunnel Engine node to the Client node and config the `SEATUNNEL_HOME` like SeaTunnel Engine Server Node.




## Submit Job

```shell
$SEATUNNEL_HOME/bin/seatunnel.sh --config $SEATUNNEL_HOME/config/v2.batch.config.template
```
