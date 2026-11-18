---
sidebar_position: 4
---

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

# YARN 配置

将发行包上传到 HDFS 前，先在提交机上配置 SeaTunnel。下面的命令从发行包的解压目录执行。

```bash
cd "$SEATUNNEL_HOME"
```

## 成员发现与客户端连接

示例使用提交用户 `etl` 和 Registry DNS 域名 `example.net`。成员和客户端配置都应使用所选模式对应的集群名和发现地址。

| 模式 | 集群名 | `config/` 下的成员配置 | 成员发现地址 |
| --- | --- | --- | --- |
| [混合模式](hybrid-cluster-mode.md) | `zeta-hybrid` | `hazelcast.yaml` | `hybrid-0.zeta-hybrid.etl.example.net:5801` |
| [分离模式](separated-cluster-mode.md) | `zeta-session` | `hazelcast-master.yaml`、`hazelcast-worker.yaml` | `master-0.zeta-session.etl.example.net:5801` |

容器和客户端都需要能解析成员发现地址，并能访问各成员通告的地址。如果使用固定主机名，请配置 YARN 调度约束，将对应成员调度到该主机。[YARN 服务发现](https://hadoop.apache.org/docs/r3.3.6/hadoop-yarn/hadoop-yarn-site/yarn-service/ServiceDiscovery.html)

在对应的成员配置文件中设置 `hazelcast.cluster-name`，替换 `hazelcast.network.join`，并保留发行包中的成员端口。以下成员与客户端配置展示分离模式；使用混合模式时，将集群名和地址替换为表中第一行的值。

```yaml
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

## 容器端口与内存

下表每行对应一个 YARN 容器。混合节点在同一 JVM 和堆内存中运行 Master 和 Worker。分离模式为两种角色使用不同端口，便于一个 Master 和一个 Worker 共享同一台 NodeManager 主机。

| 组件 | Hazelcast 端口 | HTTP 端口 | 容器内存 | `config/` 下的 JVM 配置 | 示例堆大小 |
| --- | --- | --- | --- | --- | --- |
| 混合节点 | 5801 | 8080 | 4 GiB | `jvm_options` | `-Xmx2g` |
| 分离模式 Master | 5801 | 8080 | 2 GiB | `jvm_master_options` | `-Xmx1g` |
| 分离模式 Worker | 5802 | 8081 | 4 GiB | `jvm_worker_options` | `-Xmx2g` |

在对应组件的文件中设置堆大小，并在容器内为元空间、直接缓冲区和线程栈留出空间。

## 引擎配置与存储

将以下配置合并到 `config/seatunnel.yaml`。把 `fs.defaultFS` 替换为实际的 HDFS URI，并授予服务用户 checkpoint 目录的写权限。两种示例都只有一个可以存储 IMap 数据的成员，因此设置 `backup-count: 0`。混合模式示例将 `namespace` 设为 `/user/etl/seatunnel/checkpoints/zeta-hybrid`。

```yaml
seatunnel:
  engine:
    backup-count: 0
    checkpoint:
      storage:
        type: hdfs
        plugin-config:
          storage.type: hdfs
          fs.defaultFS: hdfs://namenode.example.net:8020
          namespace: /user/etl/seatunnel/checkpoints
    http:
      enable-http: true
      port: 8080
```

分离模式需要为 Worker 复制一份配置，并将副本中的 `seatunnel.engine.http.port` 改为 `8081`。YARN 服务定义中的 Worker 组件通过 `SEATUNNEL_CONFIG` 加载这份配置。

```bash
cp config/seatunnel.yaml config/seatunnel-worker.yaml
```

HDFS checkpoint 保存算子状态。若还需在整个集群重启后自动恢复作业，请为 IMap 配置共享 MapStore，以持久化恢复所需的作业元数据。混合模式在所有节点的 `config/hazelcast.yaml` 中配置，分离模式在各 Master 的 `config/hazelcast-master.yaml` 中配置。[状态存储与恢复](../../engines/zeta/state-storage-and-recovery.md)

## Kerberos

对于启用 Kerberos 的 YARN 集群，在服务定义的顶层 `kerberos_principal` 中配置服务 ApplicationMaster 使用的 principal 和 keytab。如果 HDFS checkpoint 通过 keytab 认证，请在 `seatunnel.engine.checkpoint.storage.plugin-config` 中设置 `kerberosPrincipal` 和 `kerberosKeytabFilePath`。将 keytab 单独部署在相关进程可读取且有访问控制的路径下，不要打入共享发行包。[YARN Service API](https://hadoop.apache.org/docs/r3.3.6/hadoop-yarn/hadoop-yarn-site/yarn-service/YarnServiceAPI.html#KerberosPrincipal) · [HDFS checkpoint 存储](../../engines/zeta/checkpoint-storage.md#hdfs)
