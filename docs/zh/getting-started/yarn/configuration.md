---
sidebar_position: 3
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

示例使用服务名 `zeta-session`、提交用户 `etl` 和 Registry DNS 域名 `example.net`。第一个 Master 的地址为 `master-0.zeta-session.etl.example.net`。请将三个 Hazelcast 配置文件中的这个地址替换为实际地址。如果直接使用主机名，请将 YARN 的 Master 容器固定调度到该主机。容器和提交客户端都需要能解析 Master 域名，并能访问各成员通告的地址。[YARN 服务发现](https://hadoop.apache.org/docs/r3.3.6/hadoop-yarn/hadoop-yarn-site/yarn-service/ServiceDiscovery.html)

在 `config/hazelcast-master.yaml` 和 `config/hazelcast-worker.yaml` 中，将 `hazelcast.cluster-name` 设为 `zeta-session`。将 `hazelcast.network.join` 替换为以下配置，并保留两个文件中各自的成员端口。

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

Master 和 Worker 以进程形式运行在 NodeManager 主机上，可能被调度到同一台主机。为两个角色分别设置成员端口和 HTTP 端口。

| 角色 | Hazelcast 成员端口 | SeaTunnel HTTP 端口 |
| --- | --- | --- |
| Master | 5801 | 8080 |
| Worker | 5802 | 8081 |

示例为 Master 容器申请 2 GiB 内存，为 Worker 容器申请 4 GiB。可先在 `config/jvm_master_options` 中设置 `-Xmx1g`，在 `config/jvm_worker_options` 中设置 `-Xmx2g`，其余空间留给元空间、直接缓冲区和线程栈。

## 引擎配置与存储

将以下配置合并到 `config/seatunnel.yaml`。把 `fs.defaultFS` 替换为实际的 HDFS URI，并授予服务用户 checkpoint 目录的写权限。示例只有一个 Master，因此设置 `backup-count: 0`。

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

为 Worker 复制一份配置，并将副本中的 `seatunnel.engine.http.port` 改为 `8081`。YARN 服务定义中的 Worker 组件通过 `SEATUNNEL_CONFIG` 加载这份配置。

```bash
cp config/seatunnel.yaml config/seatunnel-worker.yaml
```

HDFS checkpoint 保存算子状态。若还需在整个集群重启后自动恢复作业，请在 `config/hazelcast-master.yaml` 中为 IMap 配置共享 MapStore，持久化恢复所需的作业元数据。[状态存储与恢复](../../engines/zeta/state-storage-and-recovery.md)

## Kerberos

对于启用 Kerberos 的 YARN 集群，在服务定义的顶层 `kerberos_principal` 中配置服务 ApplicationMaster 使用的 principal 和 keytab。如果 HDFS checkpoint 通过 keytab 认证，请在 `seatunnel.engine.checkpoint.storage.plugin-config` 中设置 `kerberosPrincipal` 和 `kerberosKeytabFilePath`。将 keytab 单独部署在相关进程可读取且有访问控制的路径下，不要打入共享发行包。[YARN Service API](https://hadoop.apache.org/docs/r3.3.6/hadoop-yarn/hadoop-yarn-site/yarn-service/YarnServiceAPI.html#KerberosPrincipal) · [HDFS checkpoint 存储](../../engines/zeta/checkpoint-storage.md#hdfs)
