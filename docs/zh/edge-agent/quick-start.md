---
sidebar_position: 2
title: 快速开始
---

# Edge Agent 快速开始

本文提供两条验证路径：


| 路径                                                 | 需要 Engine | 用途                            |
| -------------------------------------------------- | --------- | ----------------------------- |
| [Console 本地验证](#console-本地验证无需-engine) | 否 | 验证安装、采集、本地 WAL，无需 Engine |
| [生产模式（接入 Engine）](#生产模式接入-engine) | 是 | Agent 将数据发到 Engine 作业 |


生产环境加固请参阅 [部署指南](deployment-guide.md) 与 [运维](operations.md)。

## 步骤 1：在边缘主机安装 Edge Agent

按 [下载 Edge Agent 安装包](download.md) 操作后：

```shell
export EDGE_AGENT_HOME=/opt/apache-seatunnel-edge-agent-<version>
cd "$EDGE_AGENT_HOME"
```

安装根目录包含 bin/、config/、starter/。

## Console 本地验证

### 前提

- 边缘主机已安装 Java 11 或 17，并设置 JAVA_HOME。
- 不需要 SeaTunnel Engine，也不需要 EdgeSocket 网络连通。
- 安装根目录可写（默认持久化文件 data、日志、edge-agent.id）。

### 配置 Agent

编辑 $EDGE_AGENT_HOME/config/agent.yaml：

```yaml
input:
  paths:
    - "/tmp/edge-agent-quickstart.log"

output:
  type: console
```

省略 output.type 时默认也是 console。可不写 queue（sqlite-path 默认 data/wal.db）。详见 [输出配置 — console](output-configuration.md)。

创建示例日志并写入一行：

```shell
echo '{"event":"hello","ts":1}' >> /tmp/edge-agent-quickstart.log
```

### 启动并验证

```shell
sh bin/seatunnel-edge-agent.sh start
sh bin/seatunnel-edge-agent.sh status
```

在 log/edge-agent.log 中确认 BOOTSTRAP_READY，再追加一行：

```shell
echo '{"event":"world","ts":2}' >> /tmp/edge-agent-quickstart.log
```

在 log/edge-agent.log 中搜索 EDGE_CONSOLE_OUTPUT（console 通过应用日志输出，不是 edge-agent.out）。应能看到序列化后的 payload。

:::tip 没有 EDGE_CONSOLE_OUTPUT？

1. 确认 input.paths 中的文件存在且可读（ls -l /tmp/edge-agent-quickstart.log）。
2. 在出现 BOOTSTRAP_READY 之后再追加新行（首次打开默认从文件末尾尾随，不会重读旧内容）。
3. 查看 log/edge-agent.log 是否有输入或 WAL 相关错误。

:::

仍会生成 edge-agent.id 与 data/ 目录下的 WAL 持久化文件（wal.db、wal.db-wal、wal.db-shm），与是否连接 Engine 无关。说明见[配置说明 — WAL 持久化文件](configuration.md#sqlite-持久化文件)。

### 停止

```shell
sh bin/seatunnel-edge-agent.sh stop
```

接入 Engine 请参阅 [生产模式](#生产模式接入-engine)。

## 生产模式

### 前提

- 完成 [步骤 1](#步骤-1在边缘主机安装-edge-agent)。
- 网络：边缘机可访问 Engine 的 EdgeSocket 端口（示例 9876）。
- SeaTunnel Engine：可提交作业的集群或本地 Zeta。
- 安装根目录可写。

### 在 Engine 侧启动作业

提交含 EdgeSocket Source 的作业（HOCON）：

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  EdgeSocket {
    port = 9876
    token = "quick-start-secret"
  }
}

sink {
  Console {}
}
```

:::caution

记下监听 port 与 token。Agent 的 output.endpoint 须填边缘机能访问到的地址（多为 Engine 节点 IP 或负载均衡地址，而非 0.0.0.0）。

:::

保存为 edgesocket-quickstart.conf，提交作业：

```shell
./bin/seatunnel.sh --config ./config/edgesocket-quickstart.conf
```

### 配置 Agent

编辑 config/agent.yaml：

```yaml
input:
  paths:
    - "/tmp/edge-agent-quickstart.log"

output:
  type: transport
  endpoint: "<engine-host>:9876"
  auth-type: token
  token: "quick-start-secret"
  packet-mode: RAW
```

将 `<engine-host>` 换成 Engine 节点 IP 或主机名。若 Console 验证已跑过，可继续使用同一日志文件。

```shell
echo '{"event":"hello-engine","ts":1}' >> /tmp/edge-agent-quickstart.log
```

### 启动并验证

```shell
sh bin/seatunnel-edge-agent.sh start
```

在 log/edge-agent.log 中确认 BOOTSTRAP_READY。追加日志行后，Engine 侧 Console Sink 应在批次 RECEIVED 后打印数据。认证失败时可能出现 REJECTED — 见 [运维 — 常见问题](operations.md#常见问题)。

建议按以下最小验证链路检查：

1. Agent 日志出现 BOOTSTRAP_READY。
2. Agent 日志未出现 AUTH_FAILED / REJECTED。
3. Engine 作业日志可看到 EdgeSocket 批次接入与 Console Sink 输出。
4. 若认证失败：AUTH_FAILED 通常是 output.token 与 Engine token 不一致；REJECTED 通常是重复采集端或策略冲突。

### 停止

```shell
sh bin/seatunnel-edge-agent.sh stop
```

迁移或继续测试时请保留 edge-agent.id 与 data/ 目录（默认 wal.db 及 -wal/-shm）。见 [常见问题](faq.md)。

## 下一步

快速开始完成后，建议继续阅读 [部署指南](deployment-guide.md) 进行生产部署，或查看 [配置参数说明](configuration.md) 了解完整参数。全部文档导航见 [关于 Edge Agent](about.md#推荐阅读顺序)。

