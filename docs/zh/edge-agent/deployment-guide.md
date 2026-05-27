---
sidebar_position: 4
title: 部署指南
---

# Edge Agent 部署指南

在边缘主机安装并运行 Edge Agent：每个安装根目录一个常驻进程，读取本地 input、经 SQLite WAL 缓冲出站数据，并向配置的 EdgeSocket 端点发送批次。

没有 Agent 集群管理器；通过 每台边缘主机（或每个数据域）一个 Agent 水平扩展，各自使用独立的 agent.yaml 与 WAL 路径。Engine 侧接入独立：部署含 [EdgeSocket](../connectors/source/EdgeSocket.md) Source 的作业，监听地址与 Agent output.endpoint 一致。

首次使用？ 请先完成 [快速开始](quick-start.md)（Console，再生产模式），再按本文检查清单部署。

## 部署前确认

- 各边缘主机已 [下载或构建](download.md) 安装包
- input.paths 与 output（transport 端点与 token）与 Engine 作业一致 — 见[输出配置指南](output-configuration.md)
- 边缘主机到 EdgeSocket 监听地址的网络可达
- 启动后日志含 BOOTSTRAP_READY；监控 WAL 与 RECEIVED/重试 — 见[运维](operations.md)

不使用 bin/seatunnel-edge-agent.sh 的前台调试方式见[运维](operations.md)。

## 1. 下载

[下载与构建 Edge Agent 安装包](download.md)

## 2. 配置 EDGE_AGENT_HOME

```shell
export EDGE_AGENT_HOME=/opt/apache-seatunnel-edge-agent-<version>
export PATH=$PATH:$EDGE_AGENT_HOME/bin
```

可写入 /etc/profile.d/edge-agent.sh 供全局使用。

## 3. 配置 agent.yaml

默认路径：$EDGE_AGENT_HOME/config/agent.yaml。通过 EDGE_AGENT_CONFIG 覆盖。

最小化生产示例：

```yaml
input:
  paths:
    - "/var/log/myapp/*.log"

output:
  type: transport
  endpoint: "seatunnel-engine-host:9876"
  auth-type: token
  token: "<与-edgesocket-source-一致的密钥>"
```

示例符合当前默认结构：未写 queue、retry（sqlite-path 默认为 data/wal.db）。需调 WAL 或重试策略时见[配置参数说明](configuration.md)。

## 4. 配置文件输入

input 是 agent.yaml 中最易因场景而变的部分：glob 路径、尾随/补数、多行堆栈、NDJSON 等。请使用独立指南中的示例：

[文件输入配置指南](input-configuration.md) — 路径 glob、multiline 与 match: after|before、日志轮转及 8 个场景 YAML。

速查：


| 主题                                                | 文档                                       |
| ------------------------------------------------- | ---------------------------------------- |
| 参数表（paths、encoding、glob-scan-interval-ms 等） | [配置说明 — input](configuration.md#input) |
| 场景 YAML（NDJSON、Java 日志、按日期滚动文件等）                  | [文件输入配置指南](input-configuration.md)       |


## 5. Engine 侧前置条件

在 Engine 上提交含 EdgeSocket Source 的作业，监听地址与 token 须与 Agent output 一致：

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  EdgeSocket {
    port = 9876
    token = "your-token"
  }
}

sink {
  Console {}
}
```

保存为 edgesocket-prod.conf，提交作业：

```shell
./bin/seatunnel.sh --config ./config/edgesocket-prod.conf
```

:::caution

启动 Agent 前，先确认边缘主机可访问 Engine 监听端口。RAW/PACKET 模式与 Sink 替换见[输出配置指南](output-configuration.md)，线协议见 [EdgeSocket Source](../connectors/source/EdgeSocket.md)。

:::

## 6. 启动 Agent

```shell
cd "$EDGE_AGENT_HOME"
sh bin/seatunnel-edge-agent.sh start
```

查看状态：

```shell
sh bin/seatunnel-edge-agent.sh status
```

成功启动日志示例：

```text
BOOTSTRAP_READY edge-agent started agentId=..., inputId=..., inputType=file, outputType=transport
```

日志默认输出到 $EDGE_AGENT_HOME/log/edge-agent.log。启动控制输出到 $EDGE_AGENT_HOME/edge-agent.out（除非通过环境变量覆盖）。

## 7. 停止与升级

```shell
sh bin/seatunnel-edge-agent.sh stop
```

升级步骤：停止 → 替换 starter/ 与 bin/（保留 config/agent.yaml 与 WAL）→ 启动。

若使用自动生成的 ID，请保留安装根目录下的 edge-agent.id 与 WAL 数据库（queue.sqlite-path，默认 data/wal.db，位于 data/ 目录下）。

## 8. 同一主机多个 Agent

为每个实例使用独立安装根（或独立的 EDGE_AGENT_CONFIG、EDGE_AGENT_PID_FILE、EDGE_AGENT_ID_FILE、queue.sqlite-path），不要共用同一 WAL 文件或 edge-agent.id。

## 9. JVM 参数

启动脚本直接调用 java，无独立 jvm_options 文件。可按需包装启动命令或编辑 bin/seatunnel-edge-agent.sh。典型边缘设置：

```text
-Xms256m -Xmx512m
```

前台调试可从安装根目录启动 starter JAR 并指定 --config；亦可执行 bin/seatunnel-edge-agent.sh help，详见[运维](operations.md)。

