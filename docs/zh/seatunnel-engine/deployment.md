---

sidebar_position: 4
-------------------

# 部署 SeaTunnel Engine

## 1. 下载

SeaTunnel Engine 是 SeaTunnel 的默认引擎。SeaTunnel 的安装包已经包含了 SeaTunnel Engine 的所有内容。

## 2 配置 SEATUNNEL_HOME

您可以通过添加 `/etc/profile.d/seatunnel.sh` 文件来配置 `SEATUNNEL_HOME` 。`/etc/profile.d/seatunnel.sh` 的内容如下：

```
export SEATUNNEL_HOME=${seatunnel install path}
export PATH=$PATH:$SEATUNNEL_HOME/bin
```

## 3. 配置 SeaTunnel Engine JVM 选项

SeaTunnel Engine 支持两种设置 JVM 选项的方法。

1. 将 JVM 选项添加到 `$SEATUNNEL_HOME/bin/seatunnel-cluster.sh`.

   修改 `$SEATUNNEL_HOME/bin/seatunnel-cluster.sh` 文件，并在第一行添加 `JAVA_OPTS="-Xms2G -Xmx2G"`.

2. 在启动 SeaTunnel Engine 时添加 JVM 选项。例如 `seatunnel-cluster.sh -DJvmOption="-Xms2G -Xmx2G"`

## 4. 配置 SeaTunnel Engine

SeaTunnel Engine 提供许多功能，需要在 `seatunnel.yaml` 中进行配置。.

### 4.1 备份计数

SeaTunnel Engine 基于 [Hazelcast IMDG](https://docs.hazelcast.com/imdg/4.1/) 实现集群管理。集群的状态数据（作业运行状态、资源状态）存储在 [Hazelcast IMap](https://docs.hazelcast.com/imdg/4.1/data-structures/map)。
存储在 Hazelcast IMap 中的数据将在集群的所有节点上分布和存储。Hazelcast 会分区存储在 Imap 中的数据。每个分区可以指定备份数量。
因此，SeaTunnel Engine 可以实现集群 HA，无需使用其他服务（例如 zookeeper）。

`backup count` 是定义同步备份数量的参数。例如，如果设置为 1，则分区的备份将放置在一个其他成员上。如果设置为 2，则将放置在两个其他成员上。

我们建议 `backup-count` 的值为 `min(1, max(5, N/2))`。 `N` 是集群节点的数量。

```yaml
seatunnel:
    engine:
        backup-count: 1
        # 其他配置
```

### 4.2 插槽服务

插槽数量决定了集群节点可以并行运行的任务组数量。SeaTunnel Engine 是一个数据同步引擎，大多数作业都是 IO 密集型的。
建议使用动态插槽。

```yaml
seatunnel:
    engine:
        slot-service:
            dynamic-slot: true
        # 其他配置
```

### 4.3 检查点管理器

与 Flink 一样，SeaTunnel Engine 支持 Chandy–Lamport 算法。因此，可以实现无数据丢失和重复的数据同步。

**interval**

两个检查点之间的间隔，单位是毫秒。如果在作业配置文件的 `env` 中配置了 `checkpoint.interval` 参数，则此处设置的值将覆盖它。

**timeout**

检查点的超时时间。如果在超时时间内无法完成检查点，则会触发检查点失败。因此，作业将被恢复。

示例

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

有关检查点存储的信息，您可以查看 [checkpoint storage](checkpoint-storage.md)

### 4.4 历史作业过期配置

每个完成的作业的信息，如状态、计数器和错误日志，都存储在 IMap 对象中。随着运行作业数量的增加，内存会增加，最终内存将溢出。因此，您可以调整 `history-job-expire-minutes` 参数来解决这个问题。此参数的时间单位是分钟。默认值是 1440 分钟，即一天。

示例

```yaml
seatunnel:
  engine:
    history-job-expire-minutes: 1440
```

### 4.5 类加载器缓存模式

此配置主要解决不断创建和尝试销毁类加载器所导致的资源泄漏问题。
如果您遇到与元空间溢出相关的异常，您可以尝试启用此配置。
为了减少创建类加载器的频率，在启用此配置后，SeaTunnel 在作业完成时不会尝试释放相应的类加载器，以便它可以被后续作业使用，也就是说，当运行作业中使用的 Source/Sink 连接器类型不是太多时，它更有效。
默认值是 false。
示例

```yaml
seatunnel:
  engine:
    classloader-cache-mode: true
```

## 5. 配置 SeaTunnel Engine 服务

所有 SeaTunnel Engine 服务配置都在 `hazelcast.yaml` 文件中.

### 5.1 集群名称

SeaTunnel Engine 节点使用 `cluster-name` 来确定另一个节点是否与自己在同一集群中。如果两个节点之间的集群名称不同，SeaTunnel 引擎将拒绝服务请求。

### 5.2 网络

基于 [Hazelcast](https://docs.hazelcast.com/imdg/4.1/clusters/discovery-mechanisms), 一个 SeaTunnel Engine 集群是由运行 SeaTunnel Engine 服务器的集群成员组成的网络。 集群成员自动加入一起形成集群。这种自动加入是通过集群成员使用的各种发现机制来相互发现的。

请注意，集群形成后，集群成员之间的通信始终通过 TCP/IP 进行，无论使用的发现机制如何。

SeaTunnel Engine 使用以下发现机制。

#### TCP

您可以将 SeaTunnel Engine 配置为完整的 TCP/IP 集群。有关配置详细信息，请参阅 [Discovering Members by TCP section](tcp.md)。

一个示例如下 `hazelcast.yaml`

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

TCP 是我们建议在独立 SeaTunnel Engine 集群中使用的方式。

另一方面，Hazelcast 提供了一些其他的服务发现方法。有关详细信息，请参阅  [hazelcast network](https://docs.hazelcast.com/imdg/4.1/clusters/setting-up-clusters)

### 5.3 映射

仅在映射上配置时，MapStores 才会连接到外部数据存储。本主题解释了如何使用 MapStore 配置映射。有关详细信息，请参阅 [hazelcast map](https://docs.hazelcast.com/imdg/4.2/data-structures/map)

**type**

imap 持久化的类型，目前仅支持 `hdfs`。

**namespace**

它用于区分不同业务的数据存储位置，如 OSS 存储桶名称。

**clusterName**

此参数主要用于集群隔离， 我们可以使用它来区分不同的集群，如 cluster1、cluster2，这也用于区分不同的业务。

**fs.defaultFS**

我们使用 hdfs api 读写文件，因此使用此存储需要提供 hdfs 配置。

如果您使用 HDFS，可以像这样配置：

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

如果没有 HDFS，并且您的集群只有一个节点，您可以像这样配置使用本地文件：

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

如果您使用 OSS，可以像这样配置：

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

## 6. 配置 SeaTunnel Engine 客户端

所有 SeaTunnel Engine 客户端的配置都在 `hazelcast-client.yaml` 里。

### 6.1 cluster-name

客户端必须与 SeaTunnel Engine 具有相同的 `cluster-name`。否则，SeaTunnel Engine 将拒绝客户端的请求。

### 6.2 网络

**cluster-members**

需要将所有 SeaTunnel Engine 服务器节点的地址添加到这里。

```yaml
hazelcast-client:
  cluster-name: seatunnel
  properties:
      hazelcast.logging.type: log4j2
  network:
    cluster-members:
      - hostname1:5801
```

## 7. 启动 SeaTunnel Engine 服务器节点

可以通过守护进程使用 `-d` 参数启动。

```shell
mkdir -p $SEATUNNEL_HOME/logs
./bin/seatunnel-cluster.sh -d
```

日志将写入 `$SEATUNNEL_HOME/logs/seatunnel-engine-server.log`

## 8. 安装 SeaTunnel Engine 客户端

您只需将 SeaTunnel Engine 节点上的 `$SEATUNNEL_HOME` 目录复制到客户端节点，并像 SeaTunnel Engine 服务器节点一样配置 `SEATUNNEL_HOME`。

