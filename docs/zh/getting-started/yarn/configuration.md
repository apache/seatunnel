---
sidebar_position: 5
---

# YARN 配置

<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

将发行包上传到 HDFS 前，先在提交机上配置 SeaTunnel。下面的命令从发行包的解压目录执行。

```bash
cd "$SEATUNNEL_HOME"
```

## YARN Service 服务定义

运行 `yarn app -launch` 前，按所选部署模式修改对应的 JSON 文件。

| 字段 | 配置说明 |
| --- | --- |
| `queue` | 填写提交用户有权使用的 YARN 队列，同时为服务的 ApplicationMaster 和作业或集群容器预留资源。 |
| `configuration.files[].src_file` | 填写要分发的 HDFS 归档路径，使用带版本的文件名。`dest_file` 必须与组件启动命令中使用的归档文件名一致。 |
| `components[].number_of_containers`、`components[].resource` | 分别设置每个角色的实例数和单实例资源。在分离模式下，调整 `worker` 组件的实例数即可改变执行容量。 |
| `components[].restart_policy` | 一次性本地作业使用 `NEVER`。混合与分离模式示例沿用默认值 `ALWAYS`，成员退出后由服务启动替代实例。 |
| `components[].dependencies`、`components[].readiness_check` | 分离模式下 Worker 依赖 Master，Master 的 `5801` 端口通过 `PORT` 检查后才启动 Worker。提交作业前还需确认 Worker 已注册。 |

## 成员发现与客户端连接

[本地模式](local-mode.md)在作业容器中启动内嵌引擎。以下成员发现和客户端设置适用于两种常驻集群模式。

示例使用提交用户 `etl` 和 Registry DNS 域名 `example.net`。成员和客户端配置都应使用所选模式对应的集群名和发现地址。

| 模式 | 集群名 | `config/` 下的成员配置 | 成员发现地址 |
| --- | --- | --- | --- |
| [混合模式](hybrid-cluster-mode.md) | `zeta-hybrid` | `hazelcast.yaml` | `hybrid-0.zeta-hybrid.etl.example.net:5801` |
| [分离模式](separated-cluster-mode.md) | `zeta-session` | `hazelcast-master.yaml`、`hazelcast-worker.yaml` | `master-0.zeta-session.etl.example.net:5801` |

容器和客户端都需要能解析成员发现地址，并能访问各成员通告的地址。如果使用固定主机名，请配置 YARN 调度约束，将对应成员调度到该主机。[YARN 服务发现](https://hadoop.apache.org/docs/r3.3.6/hadoop-yarn/hadoop-yarn-site/yarn-service/ServiceDiscovery.html)

将下列设置合并到发行包的 `config/hazelcast-master.yaml`。在 `config/hazelcast-worker.yaml` 中使用相同的集群名和发现地址，并将成员监听端口设为 `5802`。两个文件都保留发行包的心跳和故障检测配置，启动命令通过 `-r master` 或 `-r worker` 选择角色。混合模式使用 `config/hazelcast.yaml`，将集群名和地址换成表中的混合模式值。

```yaml
hazelcast:
  cluster-name: zeta-session
  network:
    port:
      port: 5801
      auto-increment: false
    join:
      tcp-ip:
        enabled: true
        member-list:
          - master-0.zeta-session.etl.example.net:5801
```

在每台提交机的 `config/hazelcast-client.yaml` 中设置以下配置，CLI 将通过这个地址连接常驻集群。

```yaml
hazelcast-client:
  cluster-name: zeta-session
  network:
    cluster-members:
      - master-0.zeta-session.etl.example.net:5801
```

### YARN 重新调度后如何找到 Master

使用普通进程容器时，YARN Registry DNS 会将组件实例名解析到当前所在 NodeManager 主机的 IP 地址。请 Hadoop 管理员在 `core-site.xml` 中设置 `hadoop.registry.dns.enabled=true`、集群的 `hadoop.registry.zk.quorum` 和域名 `hadoop.registry.dns.domain-name`，启动 Registry DNS 服务，并让提交机和 NodeManager 使用的 DNS 解析器把该域的查询转发给 Registry DNS。[Registry DNS 配置](https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-common/registry/registry-dns.html) · [YARN 服务发现](https://hadoop.apache.org/docs/r3.3.6/hadoop-yarn/hadoop-yarn-site/yarn-service/ServiceDiscovery.html)

Master 启动后，在提交机和 Worker 所在主机运行 `getent hosts master-0.zeta-session.etl.example.net`，确认两端都能访问解析结果的 5801 端口。Hazelcast 通过该地址加入集群，随后直接连接成员通告的地址。请确保成员之间能够访问各自配置的 Hazelcast 端口。

### 将使用固定端口的 Worker 分配到不同主机

示例使用普通进程容器。如果两个 Worker 分配到同一台 NodeManager，它们会共用主机 IP，并争用 5802 和 8081 端口。若计划扩容，请在启动服务前将以下字段加入 `zeta-session.json` 中现有的 `worker` 组件。目标标签取组件名。[YARN Service 放置策略](https://hadoop.apache.org/docs/r3.3.6/hadoop-yarn/hadoop-yarn-site/yarn-service/YarnServiceAPI.html)

```json
{
  "placement_policy": {
    "constraints": [
      {
        "type": "ANTI_AFFINITY",
        "scope": "NODE",
        "target_tags": ["worker"]
      }
    ]
  }
}
```

请 Hadoop 管理员在 ResourceManager 上启用放置约束。Hadoop 3.3.6 的 `yarn.resourcemanager.placement-constraints.handler` 默认为 `disabled`，可以设为 `placement-processor`，使用 CapacityScheduler 时也可以设为 `scheduler`。启用该规则后，本服务在每个符合条件的节点最多放置一个 Worker。两个 Worker 需要两台满足资源要求且端口空闲的主机。主机不足时，YARN 无法将组件扩至目标数量。[Hadoop 放置约束](https://hadoop.apache.org/docs/r3.3.6/hadoop-yarn/hadoop-yarn-site/PlacementConstraints.html)

## 容器资源与端口

YARN 为每个容器申请一组 vcore 和内存配额。示例取值与 Kubernetes [本地模式](../kubernetes/local-mode.md)、[混合模式](../kubernetes/hybrid-cluster-mode.md)和[分离模式](../kubernetes/separated-cluster-mode.md)示例中的容器资源上限一致。

| 组件 | vcore 数 | 内存 | Hazelcast / HTTP 端口 | `config/` 下的 JVM 配置 | 示例堆大小 |
| --- | --- | --- | --- | --- | --- |
| 本地作业 | 1 | 4 GiB | 无需客户端连接地址 | `jvm_client_options` | — |
| 混合节点 | 2 | 4 GiB | 5801 / 8080 | `jvm_options` | `-Xmx2g` |
| 分离模式 Master | 8 | 24 GiB | 5801 / 8080 | `jvm_master_options` | `-Xms16g -Xmx16g` |
| 分离模式 Worker | 8 | 24 GiB | 5802 / 8081 | `jvm_worker_options` | `-Xms16g -Xmx16g` |

混合节点的 Master 和 Worker 共用一个 JVM 堆。分离模式需要在各角色的 JVM 配置文件中逐行写入堆参数。容器内的剩余空间供元空间、直接缓冲区和线程栈使用。

## 引擎与 checkpoint 存储配置

打包发行目录前，将下面的配置合并到 `config/seatunnel.yaml`。在分离模式下，示例为每个 8 vcore 的 Worker 配置 8 个固定 slot。可用 slot 不足时，`WAIT` 会让作业等待资源。Master 负责调度和 checkpoint，Worker 提供执行 slot。

```yaml
seatunnel:
  engine:
    backup-count: 0
    slot-service:
      dynamic-slot: false
      slot-num: 8
    job-schedule-strategy: WAIT
    checkpoint:
      interval: 180000
      timeout: 30000
      storage:
        type: hdfs
        max-retained: 3
        plugin-config:
          storage.type: hdfs
          fs.defaultFS: hdfs://namenode.example.net:8020
          namespace: /user/etl/seatunnel/checkpoints/zeta-session/
    http:
      enable-http: true
      port: 8080
```

间隔和超时的单位都是毫秒。示例每三分钟触发一次 checkpoint，超时时间为 30 秒，每条 pipeline 保留的已完成 checkpoint 目标数为 3。根据作业状态大小和存储延迟调整超时。将 `fs.defaultFS` 替换为实际 HDFS URI。使用服务用户执行以下命令，或授予它这两个目录的读写权限。

```bash
hdfs dfs -mkdir -p /user/etl/seatunnel/checkpoints/zeta-session /user/etl/seatunnel/imap
```

对于 `zeta-hybrid`，使用 `/user/etl/seatunnel/checkpoints/zeta-hybrid/` 作为 checkpoint 的命名空间。2 vcore 的混合成员由调度和执行任务共享 CPU 和内存。试运行时可保留 `dynamic-slot: true`，并移除 `slot-num` 和 `job-schedule-strategy`。需要限制并发时，使用足以容纳作业的较小固定 slot 数，并设置 `WAIT`。[Slot 分配](../../engines/zeta/slot-allocation-strategy.md)

**仅分离模式需要此步骤。** 完成引擎与存储配置后，再复制 Master 的配置文件，并将副本中的 `seatunnel.engine.http.port` 设为 `8081`。服务定义通过 `SEATUNNEL_CONFIG` 为 Worker 指定该文件。

```bash
cp config/seatunnel.yaml config/seatunnel-worker.yaml
```

## 持久化整集群重启所需的作业元数据

Checkpoint 保存源端读取位点等算子状态。要在所有成员停止后自动恢复运行中的作业，Zeta 还需要 Hazelcast IMap 中的作业元数据。在分离模式下，将以下 `map` 配置合并到每个 Master 的 `config/hazelcast-master.yaml` 的 `hazelcast` 节点下，保留原有的网络与服务发现配置。分离模式的 Worker 不持有 IMap 分区。

```yaml
hazelcast:
  map:
    engine*:
      map-store:
        enabled: true
        initial-mode: EAGER
        factory-class-name: org.apache.seatunnel.engine.server.persistence.FileMapStoreFactory
        properties:
          type: hdfs
          storage.type: hdfs
          fs.defaultFS: hdfs://namenode.example.net:8020
          namespace: /user/etl/seatunnel/imap
          clusterName: zeta-session
```

混合模式下，把相同的 `map` 配置放入每个混合成员的 `config/hazelcast.yaml`，并设置 `clusterName: zeta-hybrid`。MapStore 和 checkpoint 使用独立的命名空间。服务重启前后应保持 MapStore 的文件系统、命名空间和 `clusterName` 一致，以便找到已有的作业元数据。不同集群使用不同的 `clusterName`。

单 Master 和单混合成员示例均使用 `backup-count: 0`。如果需要在其他主机保存一份内存备份，至少在两台主机上各部署一个持有 IMap 数据的成员，并设置 `backup-count: 1`。分离模式的 Worker 不计入备份成员。YARN 重新启动服务时会重新创建进程。从已有 checkpoint 恢复作业还需要作业元数据、可用的已完成 checkpoint，以及访问原作业依赖和外部系统的权限。[状态存储与恢复](../../engines/zeta/state-storage-and-recovery.md)

### 使用 S3 或 OSS

存储插件的 `type` 保持为 `hdfs`，在 checkpoint 的 `plugin-config` 和 MapStore 的 `properties` 中配置 `storage.type: s3` 或 `storage.type: oss`。S3 使用 `s3.bucket`（`s3a://bucket`）和 `fs.s3a.*` 配置项，OSS 使用 `oss.bucket`（`oss://bucket`）和 `fs.oss.*` 配置项。将文件系统依赖加入上传到 YARN 的发行包，为需要访问存储桶的进程提供凭据，并为 checkpoint 和 IMap 数据使用不同的路径前缀。[Checkpoint 存储](../../engines/zeta/checkpoint-storage.md) · [IMap 存储示例](../../engines/zeta/separated-cluster-deployment.md)

## Kerberos 和 HDFS 访问

对于启用 Kerberos 的 YARN 集群，将下面的字段合并到服务 JSON 顶层。使用分配给该服务的 principal，并在所有可运行 ApplicationMaster 的主机上预置 keytab。Hadoop 3.3.6 会将 `_HOST` 替换为 AM 的主机名。[Hadoop YARN Service 安全配置](https://hadoop.apache.org/docs/r3.3.6/hadoop-yarn/hadoop-yarn-site/yarn-service/QuickStart.html#Run_with_security)

```json
{
  "kerberos_principal": {
    "principal_name": "seatunnel/_HOST@EXAMPLE.COM",
    "keytab": "file:///etc/security/keytabs/seatunnel-am.keytab"
  }
}
```

如果使用 keytab 登录 HDFS，将下列设置合并到 checkpoint 的 `plugin-config`，以及已启用的 HDFS MapStore 的 `properties`。确保各候选主机上的相关 Zeta 进程都能读取指定的 keytab，并将其独立于发行包部署。

```yaml
kerberosPrincipal: etl@EXAMPLE.COM
kerberosKeytabFilePath: /etc/security/keytabs/etl.keytab
```

如果 HDFS 使用 HA nameservice，在两处存储配置中将 `fs.defaultFS` 设为逻辑 URI，并用 `hdfs_site_path` 指向进程可读取的 `hdfs-site.xml`，其中应包含 nameservice 和故障转移配置。[HDFS checkpoint 存储](../../engines/zeta/checkpoint-storage.md#hdfs)

## 打包连接器、驱动和配置

在提交机的 `config/plugin_config` 中选择作业所需的连接器。示例批作业使用 `connector-fake` 和 `connector-console`。为示例版本安装其他连接器时，执行以下命令。

```bash
cd "$SEATUNNEL_HOME"
bash bin/install-plugin.sh 3.1.0
```

| 文件 | 在发行包中的位置 |
| --- | --- |
| 连接器实现 JAR | 放入 `connectors/`。对于自定义连接器，还要在 `connectors/plugin-mapping.properties` 中添加对应条目。 |
| 连接器专用依赖 | 放入 `plugins/<connector-artifact>/`。目录名取自 `connectors/plugin-mapping.properties` 中映射的构件名，Zeta 会在加载对应连接器时加载这些 JAR。[连接器依赖隔离](../../connectors/connector-isolated-dependency.md) |
| Zeta JDBC 连接器的数据库驱动 | 按 [JDBC 连接器文档](../../connectors/sink/Jdbc.md#zeta-引擎)，将兼容的数据库驱动 JAR 放入 `lib/`。其他连接器的驱动位置请参照相应文档。 |
| Transform JAR 和进程级依赖 | 客户端和服务端 JVM 都会将 `lib/` 加入类路径。保留发行包自带的 `seatunnel-transforms-v2.jar`，自定义 Transform JAR 也放在这里。 |

完成所选模式的配置后，将 `bin/`、`config/`、`connectors/`、`lib/`、`plugins/` 连同发行包的其他文件一起打包。提交客户端与各组件应使用相同版本的 JAR。将 keytab 等凭据单独部署到容器进程配置的路径。

更新配置或 JAR 时，使用新名称上传归档，例如 `zeta-hybrid-3.1.0-r2.tgz`，并更新本次部署所用服务定义中的 `configuration.files[].src_file`。只要还有容器可能按旧定义重启，就要保留原有归档。替换容器会从 HDFS 重新获取自己的副本。

## 收集容器日志

混合节点以及分离模式下的 Master 和 Worker 都使用 `config/log4j2.properties`，默认将滚动日志写到解压后发行目录下的 `logs/`。要同时把引擎日志写到容器的标准输出和标准错误，请在打包前取消以下两行的注释。

```properties
rootLogger.appenderRef.consoleStdout.ref = consoleStdoutAppender
rootLogger.appenderRef.consoleStderr.ref = consoleStderrAppender
```

提交作业的 CLI 和本地模式作业使用 `config/log4j2_client.properties`，默认已启用这两个控制台 appender。启用 `yarn.log-aggregation-enable=true` 后，可按 YARN application ID 查看容器输出。在提交机上运行 CLI 的输出保留在提交机上。[查看容器日志](operations.md#查看容器日志)
