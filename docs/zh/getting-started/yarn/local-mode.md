---
sidebar_position: 2
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

# 本地模式

在一个 YARN 组件容器中运行一个 SeaTunnel 作业。CLI 启动内嵌的 Zeta 引擎，作业结束后进程退出。部署[混合集群](hybrid-cluster-mode.md)或[分离集群](separated-cluster-mode.md)前，可用此模式验证连接器或作业配置。

## 准备作业

在提交机的 `$SEATUNNEL_HOME` 下准备一份干净的 SeaTunnel 3.1.0 二进制发行包。示例使用自带的 `config/v2.batch.config.template`，从 FakeSource 读取有限条记录并写入 Console。运行自己的作业时，请将作业配置、连接器和驱动放入发行包，并修改服务定义中的 `-c` 路径。

在提交机上打包发行目录，确保 `bin/` 和 `config/` 位于压缩包根目录，然后上传到 HDFS。请将命令和服务定义中的 HDFS 路径均替换为实际路径。

```bash
tar -czf /tmp/zeta-local-3.1.0.tgz -C "$SEATUNNEL_HOME" .
hdfs dfs -mkdir -p /user/etl/seatunnel/releases
hdfs dfs -put /tmp/zeta-local-3.1.0.tgz /user/etl/seatunnel/releases/
```

## 在 YARN 上启动

将以下内容保存为 `zeta-local.json`。目标队列需要有足够资源同时运行 YARN Service 的 ApplicationMaster 和一个作业容器。容器在前台执行 `seatunnel.sh -m local`。`restart_policy` 设为 `NEVER` 后，唯一的组件退出时，YARN Service 随之结束并释放容器。

```json
{
  "name": "zeta-local",
  "version": "1",
  "queue": "default",
  "configuration": {
    "files": [
      {
        "type": "STATIC",
        "src_file": "/user/etl/seatunnel/releases/zeta-local-3.1.0.tgz",
        "dest_file": "zeta.tgz"
      }
    ]
  },
  "components": [
    {
      "name": "local",
      "number_of_containers": 1,
      "restart_policy": "NEVER",
      "resource": { "cpus": 1, "memory": "4096" },
      "launch_command": "mkdir -p seatunnel && tar -xzf zeta.tgz -C seatunnel && cd seatunnel && exec bash bin/seatunnel.sh -m local -c config/v2.batch.config.template"
    }
  ]
}
```

```bash
yarn app -launch zeta-local ./zeta-local.json
```

## 检查结果

将 `APP_ID` 设为启动命令输出的 YARN application ID。在 ResourceManager 返回的报告中确认 `State : FINISHED` 和 `Final-State : SUCCEEDED`。

```bash
yarn application -status "$APP_ID"
```

如果应用失败，请先查看报告中的诊断信息。开启日志聚合后，可执行以下命令查看容器输出。

```bash
yarn logs -applicationId "$APP_ID"
```

再次独立运行作业时，请在 JSON 和启动命令中使用同一个新服务名。
