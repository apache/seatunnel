---
sidebar_position: 3
---

# 混合集群模式

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

混合模式下，每个 Zeta 成员在同一个 JVM 中参与 Master 选举并执行任务。小规模常驻集群可以从一个成员起步，调度和任务执行共享容器的 CPU 和内存。如果需要为两个角色分别配置资源和扩缩容，请使用[分离集群模式](separated-cluster-mode.md)。

## 配置混合节点和客户端

先确认已满足 [YARN 部署的前置条件](yarn.mdx)。示例使用服务名 `zeta-hybrid`、组件名 `hybrid`、提交用户 `etl` 和 Registry DNS 域 `example.net`。请将成员和客户端配置中的示例主机名替换为实际地址。

在 `$SEATUNNEL_HOME` 发行目录中，将以下配置合并到 `config/hazelcast.yaml`，保留发行包的成员端口 `5801`。

```yaml
hazelcast:
  cluster-name: zeta-hybrid
  network:
    join:
      tcp-ip:
        enabled: true
        member-list:
          - hybrid-0.zeta-hybrid.etl.example.net:5801
```

在提交机上，将以下配置合并到 `config/hazelcast-client.yaml`，使 CLI 能发现混合节点。

```yaml
hazelcast-client:
  cluster-name: zeta-hybrid
  network:
    cluster-members:
      - hybrid-0.zeta-hybrid.etl.example.net:5801
```

在 `config/seatunnel.yaml` 中，为单成员集群设置 `seatunnel.engine.backup-count: 0`，保持 HTTP 服务启用并监听 `8080` 端口。按 [YARN 配置](configuration.md) 设置 HDFS checkpoint 存储，填写实际的 HDFS URI，并将 namespace 设为 `/user/etl/seatunnel/checkpoints/zeta-hybrid`。在 `config/jvm_options` 中设置低于容器 4 GiB 上限的堆大小，例如 `-Xmx2g`。

## 上传发行包并启动服务

将配置好的发行目录与作业需要的连接器一起打包，使 `bin/` 和 `config/` 位于压缩包根目录。

```bash
tar -czf /tmp/zeta-hybrid-3.1.0.tgz -C "$SEATUNNEL_HOME" .
hdfs dfs -mkdir -p /user/etl/seatunnel/releases /user/etl/seatunnel/checkpoints/zeta-hybrid
hdfs dfs -put /tmp/zeta-hybrid-3.1.0.tgz /user/etl/seatunnel/releases/
```

将以下内容保存为 `zeta-hybrid.json`。队列需要有足够资源运行 YARN Service 的 ApplicationMaster 和一个混合节点容器。启动命令使用默认的 `master_and_worker` 角色，并保持进程在前台运行。

```json
{
  "name": "zeta-hybrid",
  "version": "1",
  "queue": "default",
  "configuration": {
    "files": [
      {
        "type": "STATIC",
        "src_file": "/user/etl/seatunnel/releases/zeta-hybrid-3.1.0.tgz",
        "dest_file": "zeta.tgz"
      }
    ]
  },
  "components": [
    {
      "name": "hybrid",
      "number_of_containers": 1,
      "resource": { "cpus": 2, "memory": "4096" },
      "launch_command": "mkdir -p seatunnel && tar -xzf zeta.tgz -C seatunnel && exec bash seatunnel/bin/seatunnel-cluster.sh"
    }
  ]
}
```

```bash
yarn app -launch zeta-hybrid ./zeta-hybrid.json
yarn app -status zeta-hybrid
```

## 提交作业

确认 Zeta 节点已加入集群，返回的 `workers` 为 `1`。

```bash
curl -fsS http://hybrid-0.zeta-hybrid.etl.example.net:8080/overview
```

在提交机上运行发行包自带的批作业。作业结束后，同一套 YARN 服务可以继续接收下一个作业。

```bash
cd "$SEATUNNEL_HOME"
bin/seatunnel.sh -m cluster -c config/v2.batch.config.template
```

## 部署多个成员

为每个成员提供稳定地址，并在成员端和客户端的 Hazelcast 发现列表中加入这些地址。例如，该组件的第二个实例地址为 `hybrid-1.zeta-hybrid.etl.example.net:5801`。使用宿主机网络时，将各成员调度到不同的 NodeManager 主机，避免固定端口冲突。若有两个成员分别位于不同主机，可设置 `backup-count: 1`，为 IMap 保留一个备份副本。[混合集群部署](../../engines/zeta/hybrid-cluster-deployment.md)
