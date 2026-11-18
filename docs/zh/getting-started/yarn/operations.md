---
sidebar_position: 5
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

# YARN 运维

## 检查服务状态和已注册的 Worker

在提交机上检查 YARN 服务和 Zeta HTTP 端点。[混合模式](hybrid-cluster-mode.md)使用混合节点的地址。

```bash
yarn app -status zeta-hybrid
curl -fsS http://hybrid-0.zeta-hybrid.etl.example.net:8080/overview
```

[分离模式](separated-cluster-mode.md)使用 Master 的地址。

```bash
yarn app -status zeta-session
curl -fsS http://master-0.zeta-session.etl.example.net:8080/overview
```

YARN 服务状态中包含 application ID、组件实例和容器所在主机。Zeta 返回的 `workers` 表示已加入集群的 Worker 数量。两种示例都启动了一个能执行任务的成员，等 `workers` 达到 `1` 后再提交作业。

在分离模式下，如果 YARN 已分配 Worker 容器，但注册数持续为零，先查看 Worker 日志，再检查容器是否能解析 Master 地址并连接 `5801` 端口，以及两端配置的 Hazelcast 集群名是否一致。

## 查看容器日志

启用 YARN 日志聚合后，将 `APP_ID` 设为服务状态中本次运行对应的 YARN application ID。

```bash
yarn logs -applicationId "$APP_ID"
```

若要在容器输出中查看 Zeta 引擎日志，在打包 SeaTunnel 前启用 `config/log4j2.properties` 中 stdout 和 stderr 的控制台 appender 引用。

## 集群组件扩缩容

### 混合节点扩缩容

在混合模式下，调整 `hybrid` 组件的实例数会同时改变任务执行容量、参与 Master 选举的成员和 IMap 分区分布。示例使用固定端口 `5801` 和 `8080`，需要把各成员放在不同主机上。移除节点前，确认剩余成员能容纳已配置的备份副本并满足作业的资源需求。

为两个混合节点配置好调度约束和成员发现后，增加组件实例数，等待 `/overview` 显示两个 Worker。

```bash
yarn app -flex zeta-hybrid -component hybrid 2
```

### 分离模式下的 Worker 扩缩容

示例中每个 Worker 都使用固定端口 `5802` 和 `8081`。如果容器共享宿主机网络，先配置 YARN 调度约束，让不同 Worker 分布在不同的 NodeManager 主机上，并确认队列有足够的内存和 vcore 分配给新增容器。

```bash
yarn app -flex zeta-session -component worker 2
```

此命令将 Worker 目标数量设为两个。确认 `/overview` 中的 `workers` 达到 `2` 后，再提交需要新增资源的作业。

要缩回一个 Worker，先等待批作业结束，或通过 savepoint 停止受影响的流作业。确认保留的 Worker 有足够的 slot 后再缩容。

```bash
yarn app -flex zeta-session -component worker 1
```

## 停止和重启服务

计划停机前，等待批作业结束，并为后续需要恢复的流作业创建 savepoint。停止服务会释放容器。以下命令使用分离模式的服务名，混合模式请将 `zeta-session` 替换为 `zeta-hybrid`。

```bash
yarn app -stop zeta-session
```

按已保存的服务定义重新启动，确认 Worker 注册后再提交或恢复作业。

```bash
yarn app -start zeta-session
yarn app -status zeta-session
```

两种示例都将 checkpoint 写入 HDFS。若希望整套集群故障后自动恢复运行中的作业，还需要在存储作业元数据的成员上[配置持久化 IMap MapStore](configuration.md)。计划停机时，请保留作业 ID 和作业配置，以便从各自的 savepoint 恢复。[状态存储与恢复](../../engines/zeta/state-storage-and-recovery.md)
