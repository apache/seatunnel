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

# 分离集群模式

下面以一个 Master 和一个 Worker 为例启动 `zeta-session` 常驻服务。使用 `yarn app` 管理共享服务，使用 `bin/seatunnel.sh` 向集群提交各个作业。

## 准备发行包

先确认已满足 [YARN 部署的前置条件](yarn.mdx)，并按 [YARN 配置](configuration.md) 设置 Master、Worker 和客户端，再打包发行目录。客户端读取本地的 `config/hazelcast-client.yaml`。部署到 YARN 的发行包也需要包含作业使用的连接器和驱动。

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

查看服务状态、容器日志和重启步骤，请参见 [YARN 运维](operations.md)。
