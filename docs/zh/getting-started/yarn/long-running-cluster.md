---
sidebar_position: 1
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

# 在 YARN 上部署 Zeta 常驻集群

将一套共享的 Zeta 集群部署为一个 Hadoop YARN Service。YARN Service 的 ApplicationMaster 为 Zeta Master 和 Worker 申请容器。后续作业都提交到同一个 Zeta Master。单个作业结束后，容器继续运行，可以接收下一个作业。

## 准备集群

- 示例使用 Hadoop 3.3.6，并已启用 YARN Service（`yarn.webapp.api-service.enable=true`）。确认可以访问 HDFS 并向目标队列提交应用。对于启用 Kerberos 的集群，请在服务定义的顶层 `kerberos_principal` 中配置分配给服务的 principal 和 keytab，并授予服务访问 HDFS 的权限。凭据应独立于发行包分发。[Hadoop YARN Service 快速入门](https://hadoop.apache.org/docs/r3.3.6/hadoop-yarn/hadoop-yarn-site/yarn-service/QuickStart.html)
- 示例使用服务名 `zeta-session`、提交用户 `etl` 和 Registry DNS 域名 `example.net`。启用 YARN Registry DNS 后，第一个 Master 的地址为 `master-0.zeta-session.etl.example.net`。请统一替换为实际的服务名、用户名和域名，确保容器和客户端都能解析 Master 域名，并能访问各成员通告的地址。如果调度策略将 Master 固定在指定主机，也可以使用该主机的稳定域名。[YARN 服务发现](https://hadoop.apache.org/docs/r3.3.6/hadoop-yarn/hadoop-yarn-site/yarn-service/ServiceDiscovery.html)
- 在提交机上准备干净的 SeaTunnel 3.1.0 二进制发行包，将 `SEATUNNEL_HOME` 指向解压目录，并放入作业所需的连接器和驱动。示例作业使用 FakeSource 和 Console。客户端和两种角色的容器都需要能通过 `PATH` 运行 Java 11 或 17。

## 配置 Master、Worker 和客户端

进入 `$SEATUNNEL_HOME`，修改发行包中的配置。示例启动一个 Master 和一个 Worker，均以前台进程运行。两个角色使用不同端口，也可以调度到同一台 NodeManager 主机上。

| 角色 | Hazelcast 成员端口 | SeaTunnel HTTP 端口 |
| --- | --- | --- |
| Master | 5801 | 8080 |
| Worker | 5802 | 8081 |

在 `config/hazelcast-master.yaml` 和 `config/hazelcast-worker.yaml` 中，将 `hazelcast.cluster-name` 设为 `zeta-session`，并用下面的内容替换 `hazelcast.network` 下的 `join` 配置。各角色使用发行包中各自的成员端口。

```yaml
join:
  tcp-ip:
    enabled: true
    member-list:
      - master-0.zeta-session.etl.example.net:5801
```

在提交机的 `config/hazelcast-client.yaml` 中设置以下配置，让 CLI 连接到 Master。

```yaml
hazelcast-client:
  cluster-name: zeta-session
  network:
    cluster-members:
      - master-0.zeta-session.etl.example.net:5801
```

将以下配置合并到 `config/seatunnel.yaml`。把 HDFS URI 替换为实际集群的 `fs.defaultFS`，并为服务用户授予 checkpoint 目录的写权限。示例只有一个 Master，因此备份副本数设为零。

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

```bash
cp config/seatunnel.yaml config/seatunnel-worker.yaml
```

在复制出的 `config/seatunnel-worker.yaml` 中，将 `seatunnel.engine.http.port` 改为 `8081`。在 `config/jvm_master_options` 和 `config/jvm_worker_options` 中设置堆大小，并为堆外内存留出空间。按下方容器的内存配额，Master 可先设为 `-Xmx1g`，Worker 可先设为 `-Xmx2g`。

## 上传发行包并启动服务

打包配置好的**发行目录中的内容**，使 `bin/` 和 `config/` 位于压缩包根目录。更新发行包或配置时，请使用新的归档文件名，并同步修改服务定义中的 `src_file`。

```bash
tar -czf /tmp/zeta-session-3.1.0.tgz -C "$SEATUNNEL_HOME" .
hdfs dfs -mkdir -p /user/etl/seatunnel/releases /user/etl/seatunnel/checkpoints
hdfs dfs -put /tmp/zeta-session-3.1.0.tgz /user/etl/seatunnel/releases/
```

将以下内容保存为 `zeta-session.json`。队列除了要容纳两个 Zeta 容器，还要为 YARN Service 的 ApplicationMaster 预留资源。YARN 将压缩包分发到各容器。每个角色分别解压到自己的可写目录，用于存放日志和运行时文件。Master 的成员端口就绪后才启动 Worker。两条启动命令都让 JVM 在前台运行，便于 YARN 跟踪进程状态。

```json
{
  "name": "zeta-session",
  "version": "1",
  "queue": "default",
  "configuration": {
    "files": [
      {
        "type": "STATIC",
        "src_file": "/user/etl/seatunnel/releases/zeta-session-3.1.0.tgz",
        "dest_file": "zeta.tgz"
      }
    ]
  },
  "components": [
    {
      "name": "master",
      "number_of_containers": 1,
      "resource": { "cpus": 1, "memory": "2048" },
      "readiness_check": { "type": "PORT", "properties": { "port": "5801" } },
      "launch_command": "mkdir -p seatunnel && tar -xzf zeta.tgz -C seatunnel && exec bash seatunnel/bin/seatunnel-cluster.sh -r master"
    },
    {
      "name": "worker",
      "number_of_containers": 1,
      "dependencies": ["master"],
      "resource": { "cpus": 2, "memory": "4096" },
      "configuration": { "env": { "SEATUNNEL_CONFIG": "seatunnel/config/seatunnel-worker.yaml" } },
      "launch_command": "mkdir -p seatunnel && tar -xzf zeta.tgz -C seatunnel && exec bash seatunnel/bin/seatunnel-cluster.sh -r worker"
    }
  ]
}
```

```bash
yarn app -launch zeta-session ./zeta-session.json
yarn app -status zeta-session
```

## 提交作业并验证集群

先确认 Zeta Master 返回的 `workers` 为 `1`，再从提交机运行示例批作业。

```bash
curl -fsS http://master-0.zeta-session.etl.example.net:8080/overview
```

```bash
cd "$SEATUNNEL_HOME"
bin/seatunnel.sh -m cluster -c config/v2.batch.config.template
```

CLI 会输出 Zeta 作业 ID 和最终状态。作业达到 `FINISHED` 后，再次执行 `yarn app -status zeta-session`。同一个 YARN application ID 应仍处于运行状态，此时可以继续向同一个 Master 提交作业。

## 运维

计划停机前，先等待运行中的作业结束或将其取消。停止服务会释放容器，需要时可以重新启动。

```bash
yarn app -stop zeta-session
yarn app -start zeta-session
```

如果已开启 YARN 日志聚合，可用 `yarn logs -applicationId <application-id>` 查看容器输出。若要将引擎日志一并收集，请在打包前启用 `config/log4j2.properties` 中的控制台 appender。

增加 Worker 时，使用相同固定端口的实例应分布在不同主机上，或使用独立且可路由的 IP。需要 Master 故障转移时，至少在不同主机上部署两个 Master，将它们加入发现地址列表，并设置相应的备份副本数。若要在整套集群停止后恢复运行中的作业，还需要持久化保存作业元数据的 IMap，并配置共享 checkpoint 存储。[分离集群部署](../../engines/zeta/separated-cluster-deployment.md) · [状态存储与恢复](../../engines/zeta/state-storage-and-recovery.md)
