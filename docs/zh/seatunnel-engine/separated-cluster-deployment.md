---
sidebar_position: 6
---

# 部署 SeaTunnel Engine 分离模式集群

SeaTunnel Engine 的Master服务和Worker服务分离，每个服务单独一个进程。Master节点只负责作业调度，RESTful API，任务提交等，Imap数据只存储在Master节点中。Worker节点只负责任务的执行，不参与选举成为master，也不存储Imap数据。

在所有Master节点中，同一时间只有一个Master节点工作，其他Master节点处于standby状态。当当前Master节点宕机或心跳超时，会从其它Master节点中选举出一个新的Master Active节点。

这是最推荐的一种使用方式，在该模式下Master的负载会很小，Master有更多的资源用来进行作业的调度，任务的容错指标监控以及提供rest api服务等，会有更高的稳定性。同时Worker节点不存储Imap的数据，所有的Imap数据都存储在Master节点中，即使Worker节点负载高或者挂掉，也不会导致Imap数据重新分布。

## 1. 下载

[下载和制作SeaTunnel安装包](download-seatunnel.md)

## 2 配置 SEATUNNEL_HOME

您可以通过添加 `/etc/profile.d/seatunnel.sh` 文件来配置 `SEATUNNEL_HOME` 。`/etc/profile.d/seatunnel.sh` 的内容如下：

```
export SEATUNNEL_HOME=${seatunnel install path}
export PATH=$PATH:$SEATUNNEL_HOME/bin
```

## 3. 配置 Master 节点 JVM 选项

Master节点的JVM参数在`$SEATUNNEL_HOME/config/jvm_master_options`文件中配置。

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

Worker节点的JVM参数在`$SEATUNNEL_HOME/config/jvm_worker_options`文件中配置。

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

## 4. 配置 SeaTunnel Engine

SeaTunnel Engine 提供许多功能，需要在 `seatunnel.yaml` 中进行配置。.

### 4.1 Imap中数据的备份数设置（该参数在Worker节点无效）

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

:::tip

由于在分离集群模式下，Worker节点不存储Imap数据，因此Worker节点的`backup-count`配置无效。如果Master和Worker进程在同一个机器上启动，Master和Worker会共用`seatunnel.yaml`配置文件，此时Worker节点服务会忽略`backup-count`配置。

:::

### 4.2 Slot配置（该参数在Master节点无效）

Slot数量决定了集群节点可以并行运行的任务组数量。一个任务需要的Slot的个数公式为 N = 2 + P(任务配置的并行度)。 默认情况下SeaTunnel Engine的slot个数为动态，即不限制个数。我们建议slot的个数设置为节点CPU核心数的2倍。

动态slot个数（默认）配置如下：

```yaml
seatunnel:
    engine:
        slot-service:
            dynamic-slot: true
        # 其他配置
```

静态slot个数配置如下：

```yaml
seatunnel:
    engine:
        slot-service:
            dynamic-slot: false
            slot-num: 20
```

:::tip

由于在分离集群模式下，Master节点不运行任务，所以Master服务不会启动Slot服务，因此Master节点的`slot-service`配置无效。如果Master和Worker进程在同一个机器上启动，Master和Worker会共用`seatunnel.yaml`配置文件，此时Master节点服务会忽略`slot-service`配置。

:::

### 4.3 检查点管理器（该参数在Worker节点无效）

与 Flink 一样，SeaTunnel Engine 支持 Chandy–Lamport 算法。因此，可以实现无数据丢失和重复的数据同步。

**interval**

两个检查点之间的间隔，单位是毫秒。如果在作业配置文件的 `env` 中配置了 `checkpoint.interval` 参数，将以作业配置文件中设置的为准。

**timeout**

检查点的超时时间。如果在超时时间内无法完成检查点，则会触发检查点失败，作业失败。如果在作业的配置文件的`env`中配置了`checkpoint.timeout`参数，将以作业配置文件中设置的为准。

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

检查点是一种容错恢复机制。这种机制确保程序在运行时，即使突然遇到异常，也能自行恢复。检查点定时触发，每次检查点进行时每个Task都会被要求将自身的状态信息（比如读取kafka时读取到了哪个offset）上报给检查点线程，由该线程写入一个分布式存储（或共享存储）。当任务失败然后自动容错恢复时，或者通过seatunnel.sh -r 指令恢复之前被暂停的任务时，会从检查点存储中加载对应作业的状态信息，并基于这些状态信息进行作业的恢复。

如果集群的节点大于1，检查点存储必须是一个分布式存储，或者共享存储，这样才能保证任意节点挂掉后依然可以在另一个节点加载到存储中的任务状态信息。

:::tip

检查点配置只有Master服务才会读取，Worker服务不会读取检查点配置。如果Master和Worker进程在同一个机器上启动，Master和Worker会共用`seatunnel.yaml`配置文件，此时Worker节点服务会忽略`checkpoint`配置。

:::

有关检查点存储的信息，您可以查看 [Checkpoint Storage](checkpoint-storage.md)

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
如果您遇到与metaspace空间溢出相关的异常，您可以尝试启用此配置。
为了减少创建类加载器的频率，在启用此配置后，SeaTunnel 在作业完成时不会尝试释放相应的类加载器，以便它可以被后续作业使用，也就是说，当运行作业中使用的 Source/Sink 连接器类型不是太多时，它更有效。
默认值是 false。
示例

```yaml
seatunnel:
  engine:
    classloader-cache-mode: true
```

### 4.6 IMap持久化配置(该参数在Worker节点无效)

:::tip

由于在分离集群模式下，只有Master节点存储Imap数据，Worker节点不存储Imap数据，所以Worker服务不会读取该参数项。

:::

在SeaTunnel中，我们使用IMap(一种分布式的Map，可以实现数据跨节点跨进程的写入的读取 有关详细信息，请参阅 [Hazelcast Map](https://docs.hazelcast.com/imdg/4.2/data-structures/map)) 来存储每个任务及其task的状态，以便在任务所在节点宕机后，可以在其他节点上获取到任务之前的状态信息，从而恢复任务实现任务的容错。

默认情况下Imap的信息只是存储在内存中，我们可以设置Imap数据的复本数，具体可参考(4.1 Imap中数据的备份数设置)，如果复本数是2，代表每个数据会同时存储在2个不同的节点中。一旦节点宕机，Imap中的数据会重新在其它节点上自动补充到设置的复本数。但是当所有节点都被停止后，Imap中的数据会丢失。当集群节点再次启动后，所有之前正在运行的任务都会被标记为失败，需要用户手工通过seatunnel.sh -r 指令恢复运行。

为了解决这个问题，我们可以将Imap中的数据持久化到外部存储中，如HDFS、OSS等。这样即使所有节点都被停止，Imap中的数据也不会丢失，当集群节点再次启动后，所有之前正在运行的任务都会被自动恢复。

下面介绍如何使用 MapStore 持久化配置。有关详细信息，请参阅 [Hazelcast Map](https://docs.hazelcast.com/imdg/4.2/data-structures/map)

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
```

注意：使用OSS 时，确保 lib目录下有这几个jar.

```
aliyun-sdk-oss-3.13.2.jar
hadoop-aliyun-3.3.6.jar
jdom2-2.0.6.jar
netty-buffer-4.1.89.Final.jar 
netty-common-4.1.89.Final.jar
seatunnel-hadoop3-3.1.4-uber.jar
```

### 4.7 作业调度策略

当资源不足时，作业调度策略可以配置为以下两种模式：

1. `WAIT`：等待资源可用。
2. `REJECT`：拒绝作业，默认值。

示例

```yaml
seatunnel:
  engine:
    job-schedule-strategy: WAIT
```

当`dynamic-slot: ture`时，`job-schedule-strategy: WAIT` 配置会失效，将被强制修改为`job-schedule-strategy: REJECT`，因为动态Slot时该参数没有意义，可以直接提交。

## 5. 配置 SeaTunnel Engine 网络服务

所有 SeaTunnel Engine 网络相关的配置都在 `hazelcast-master.yaml`和`hazelcast-worker.yaml` 文件中.

### 5.1 集群名称

SeaTunnel Engine 节点使用 `cluster-name` 来确定另一个节点是否与自己在同一集群中。如果两个节点之间的集群名称不同，SeaTunnel 引擎将拒绝服务请求。

### 5.2 网络

基于 [Hazelcast](https://docs.hazelcast.com/imdg/4.1/clusters/discovery-mechanisms), 一个 SeaTunnel Engine 集群是由运行 SeaTunnel Engine 服务器的集群成员组成的网络。 集群成员自动加入一起形成集群。这种自动加入是通过集群成员使用的各种发现机制来相互发现的。

请注意，集群形成后，集群成员之间的通信始终通过 TCP/IP 进行，无论使用的发现机制如何。

SeaTunnel Engine 使用以下发现机制。

#### TCP

您可以将 SeaTunnel Engine 配置为完整的 TCP/IP 集群。有关配置详细信息，请参阅 [Discovering Members by TCP section](tcp.md)。

在分离集群模式下，Master和Worker服务使用不同的端口。

Master节点网络配置 `hazelcast-master.yaml`

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

Worker节点网络配置 `hazelcast-worker.yaml`

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

TCP 是我们建议在独立 SeaTunnel Engine 集群中使用的方式。

另一方面，Hazelcast 提供了一些其他的服务发现方法。有关详细信息，请参阅  [Hazelcast Network](https://docs.hazelcast.com/imdg/4.1/clusters/setting-up-clusters)

## 6. 启动 SeaTunnel Engine Master 节点

可以通过守护进程使用 `-d` 参数启动。

```shell
mkdir -p $SEATUNNEL_HOME/logs
./bin/seatunnel-cluster.sh -d -r master
```

日志将写入 `$SEATUNNEL_HOME/logs/seatunnel-engine-master.log`

## 7. 启动 SeaTunnel Engine Worker 节点

可以通过守护进程使用 `-d` 参数启动。

```shell
mkdir -p $SEATUNNEL_HOME/logs
./bin/seatunnel-cluster.sh -d -r worker
```

日志将写入 `$SEATUNNEL_HOME/logs/seatunnel-engine-worker.log`

## 8. 安装 SeaTunnel Engine 客户端

### 8.1 和服务端一样设置`SEATUNNEL_HOME`

您可以通过添加 `/etc/profile.d/seatunnel.sh` 文件来配置 `SEATUNNEL_HOME` 。`/etc/profile.d/seatunnel.sh` 的内容如下：

```
export SEATUNNEL_HOME=${seatunnel install path}
export PATH=$PATH:$SEATUNNEL_HOME/bin
```

## 8. 提交作业和管理作业

### 8.1 使用 SeaTunnel Engine 客户端提交作业

#### 安装 SeaTunnel Engine 客户端

##### 设置和服务器一样的`SEATUNNEL_HOME`

您可以通过添加 `/etc/profile.d/seatunnel.sh` 文件来配置 `SEATUNNEL_HOME` 。`/etc/profile.d/seatunnel.sh` 的内容如下：

```
export SEATUNNEL_HOME=${seatunnel install path}
export PATH=$PATH:$SEATUNNEL_HOME/bin
```

##### 配置 SeaTunnel Engine 客户端

所有 SeaTunnel Engine 客户端的配置都在 `hazelcast-client.yaml` 里。

**cluster-name**

客户端必须与 SeaTunnel Engine 具有相同的 `cluster-name`。否则，SeaTunnel Engine 将拒绝客户端的请求。

**network**

需要将所有 SeaTunnel Engine Master节点的地址添加到这里。

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

#### 提交作业和管理作业

现在集群部署完成了，您可以通过以下教程完成作业的提交和管理：[提交和管理作业](user-command.md)

### 8.2 使用 REST API 提交作业

SeaTunnel Engine 提供了 REST API 用于提交作业。有关详细信息，请参阅 [REST API V2](rest-api-v2.md)
