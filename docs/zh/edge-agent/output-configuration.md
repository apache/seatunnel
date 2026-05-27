---
sidebar_position: 7
title: 输出配置
---

# 输出配置指南

output 段定义 Agent 在本地出站队列落盘后将批次发往何处。生产环境使用 type: transport（EdgeSocket TCP）；本地调试可使用 type: console。

全部键名、类型与默认值见 [配置说明 — output](configuration.md#output)。本文说明 与 Engine 的对齐、RAW 与 PACKET，以及 场景 YAML。

## transport 与 console

| output.type | 场景 |
|---------------|------|
| transport | 经 EdgeSocket 发往运行中作业（endpoint、token 等）。 |
| console | 在 log/edge-agent.log 中以 EDGE_CONSOLE_OUTPUT 记录序列化结果（非业务 stdout）。省略 type 时默认为 console。 |

## Engine endpoint 与 Agent output.endpoint

:::caution endpoint 易混淆

勿将 Engine 作业里 EdgeSocket Source 的 endpoint 与 Agent 的 output.endpoint 混为一谈，二者含义不同。

:::

| 配置 | 必填 | 含义 |
|------|------|------|
| EdgeSocket Source port | 是 | Engine 监听端口（通常绑定所有网卡）。 |
| EdgeSocket Source endpoint | 否 | 仅用于观测/日志，不改变绑定地址。 |
| Agent output.endpoint | 是 | 边缘机 TCP 拨号目标：可达的 host:port。 |

反例：将 Engine Source endpoint 设为 0.0.0.0:9876，并在 Agent output.endpoint 使用相同值。Agent 须使用边缘机可路由到的地址。

## 地址与认证

| 关注点 | Agent（agent.yaml） | Engine（EdgeSocket Source 作业） |
|--------|----------------------|----------------------------------|
| 拨号 / 监听 | `output.endpoint: "<host>:<port>"` — 须为边缘机可达地址 | port — Source 监听端口 |
| 共享密钥 | output.token | token（或作业中的等价配置） |

Agent 首先发送 `__AUTH__:<token>`。常见响应：

| 响应 | 含义 | Agent 行为 |
|------|------|------------|
| ACK | 认证成功 | 开始发送批次 |
| REJECTED | 重复采集端或策略冲突 | 快速失败，不自动重连 — 检查是否多个 Agent 连同一监听 |
| AUTH_FAILED | 密钥不一致 | 对齐 YAML 与作业配置后重启 |

线路细节见 [EdgeSocket Source](../connectors/source/EdgeSocket.md)。

## RAW 与 PACKET

| 模式 | 默认 | 行为 |
|------|------|------|
| RAW | 是 | 事件序列化后的载荷作为批次 body 发送，快速开始与多数日志管道适用。 |
| PACKET | 否 | 分帧包；支持 compression、encryption，须与 Engine 侧 EdgeSocket Source 一致。 |

:::caution 压缩与加密

compression、encryption 仅在 packet-mode: PACKET 时生效。RAW 模式下 YAML 中的相关项会被忽略。配置表中 compression 默认值 gzip 仅针对 PACKET。

:::

### 批次响应

认证通过后，批次为 `__BATCH__:<batchId>:<payload>`。Engine 常见回复：

| 响应 | Agent 处理 |
|------|------------|
| RECEIVED | 出站队列行确认 |
| RETRY | 重发同一批次 |
| `QUEUE_FULL:<ms>` | 等待后重试 |
| DECRYPT_FAILED | 致命错误 — 检查 PACKET/加密与 Engine 是否一致 |

:::note

Agent 不发送 `__COMMIT__`，耐久性以本地 SQLite 出站队列直至收到 RECEIVED 为准。

:::

## 场景配置示例

以下仅展示 output；请与 input 及 queue、retry 配置一并使用。

### 1. 生产 — RAW + token

```yaml
output:
  type: transport
  endpoint: "10.0.1.50:9876"
  auth-type: token
  token: "<与-engine-token-一致>"
  packet-mode: RAW
  connect-timeout-ms: 5000
  read-timeout-ms: 30000
```

### 2. 本地调试 — console

```yaml
output:
  type: console
```

:::tip

启动 Agent 后在 log/edge-agent.log 中搜索 EDGE_CONSOLE_OUTPUT，无需 Engine 作业。见[快速开始 — Console 本地验证](quick-start.md#console-本地验证)。

:::

### 3. PACKET + gzip

```yaml
output:
  type: transport
  endpoint: "10.0.1.50:9876"
  auth-type: token
  token: "<共享密钥>"
  packet-mode: PACKET
  compression: gzip
  encryption: none
```

压缩与分帧须与 EdgeSocket Source 作业配置一致。

### 4. PACKET + AES-GCM

```yaml
output:
  type: transport
  endpoint: "10.0.1.50:9876"
  auth-type: token
  token: "<共享密钥>"
  packet-mode: PACKET
  compression: none
  encryption: aes_gcm
  aes-secret-key-base64: "<与-engine-一致的-base64-密钥>"
```

:::caution

encryption: aes_gcm 时必须配置 aes-secret-key-base64，且须与 Engine 侧一致。

:::

### 5. 网络不稳定 — 加大超时与重连

```yaml
output:
  type: transport
  endpoint: "10.0.1.50:9876"
  auth-type: token
  token: "<共享密钥>"
  packet-mode: RAW
  connect-timeout-ms: 10000
  read-timeout-ms: 60000
  initial-backoff-ms: 500
  max-backoff-ms: 60000
  max-reconnect-cycles: 32
  max-batch-send-attempts: 128
```

:::note

传输层重连与 WAL 的 retry.*（调度/出站行重试）相互独立。

:::

## output.id 与迁移

output.id 用于出站侧日志与迁移标识（不写入线协议）。见 [身份文件](configuration.md#身份文件edge-agentid)。迁移时请一并拷贝 edge-agent.id、WAL 及 config/agent.yaml。

