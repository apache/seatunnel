---
sidebar_position: 4
title: 命令
---

# YARN 应用命令

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

使用 Application CLI 向 YARN 队列提交作业，凭 ResourceManager 分配的 application ID 查询或取消作业。提交、查询和取消时使用同一套 Hadoop 配置和有权限的身份。

## 命令格式

```bash
bin/seatunnel-application.sh submit -t yarn [-a yarn.conf] [-c job.conf] [-Dkey=value ...] [--wait]
bin/seatunnel-application.sh status -t yarn --id APPLICATION_ID [-a yarn.conf] [--wait]
bin/seatunnel-application.sh cancel -t yarn --id APPLICATION_ID [-a yarn.conf]
```

## submit

客户端注册发行包，按 application ID 暂存配置，然后提交 AM。AM 进入运行态或短批作业已成功结束时，命令打印两个 ID。YARN application ID 用于查询和取消，Zeta job ID 用于 checkpoint 恢复。

```text
Application ID: application_1700000000000_0001
Job ID: 917381145928572929
```

本地作业可以使用 `-c job.conf`。作业在 HDFS 上时，可设置 `yarn.job-config-file`。将 `HADOOP_CONF_DIR` 指向集群配置目录后，下面的命令通过参数指定作业和发行包，可以同时省略 `-a` 和 `-c`。

```bash
bin/seatunnel-application.sh submit -t yarn \
  -Dyarn.distribution=hdfs://nameservice1/releases/seatunnel.tgz \
  -Dyarn.job-config-file=hdfs://nameservice1/jobs/orders.conf
```

## status

默认查询一次 ResourceManager。加上 `--wait` 可持续等待作业结束。YARN 的 `ACCEPTED` 对应 `DEPLOYING`，`KILLED` 对应 `CANCELED`，`FINISHED` 使用 AM 上报的最终结果。`--wait-timeout` 以毫秒限制客户端轮询时间。退出码见[公共 CLI 参考](../cli.md#状态与等待)。

```bash
bin/seatunnel-application.sh status -t yarn --id "$APP_ID" -a yarn.conf --wait
```

使用 `--wait` 时，`SUCCEEDED` 和 `CANCELED` 的退出码为 `0`，`FAILED` 为 `1`，`UNKNOWN` 为 `2`。可用 `--wait-timeout` 限制客户端等待时间。等待超时后，YARN 应用继续运行。

## cancel

取消命令请求 YARN 终止应用，并清理该应用的 staging 目录。ResourceManager 根据集群策略保留应用报告。报告过期后，SeaTunnel 返回 `UNKNOWN`。后续提交可指定原 Zeta job ID，从已保留的 checkpoint 恢复。

```bash
bin/seatunnel-application.sh cancel -t yarn --id "$APP_ID" -a yarn.conf
```

## 在 YARN 中排查

将 `APP_ID` 设为提交命令打印的 application ID。下面依次查看队列、状态、诊断信息和跟踪地址，列出 application attempt，再读取容器聚合日志。可用 `yarn container -list <attemptId>` 查看某次尝试的容器及其 NodeManager 地址。如果已设置 `yarn.master.http-port`，作业运行期间还可以通过 AM 主机和指定端口直连 UI 和 REST API。

```bash
yarn application -status "$APP_ID"
yarn applicationattempt -list "$APP_ID"
yarn logs -applicationId "$APP_ID"
```
