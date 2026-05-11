import ChangeLog from '../changelog/connector-edge-socket.md';

# EdgeSocket

> 面向轻量边缘采集客户端的 Socket Source 连接器。

## 支持引擎

> SeaTunnel Zeta

## 主要特性

- [ ] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## 描述

`EdgeSocket` 用于边缘采集场景：轻量远端 collector 通过 socket 将数据发送到中心 SeaTunnel Zeta 集群。

该 Source 在 Zeta Worker 内以 ingress 服务端模式运行：绑定本地 host/port，等待边缘 collector 主动连接并推送数据。

`EdgeSocketSourceReader` 内部维护本地内存队列，长度由 `local_queue_capacity` 配置（默认 `1024`），`pollNext()` 只从这个本地队列拉取数据。

collector 每推送一条批次数据时，Source 按以下规则响应：

- 若本地队列已满，返回 `RETRY`；
- 若可接收，则包入队并返回 `RECEIVED`。

collector 随后发送 `__COMMIT__:<batchId>` 轮询 checkpoint 确认：

- `PENDING`：该批次已接收但还未 checkpoint 确认；
- `ACK:<watermarkBatchId>`：小于等于 watermark 的批次都已 checkpoint 确认。

队列操作逻辑内聚在本地队列组件（`queue` 包）中，socket 入口逻辑只负责协议解码和入队调用。
压缩处理在队列 `pollNext` 路径按压缩类型执行。

## 数据类型映射

未配置 `schema` 时，该 Source 输出一个名为 `value` 的字符串字段。
配置 `schema` 后，会按 JSON 反序列化并输出为用户定义的字段结构。

## 参数说明

| 参数名                  | 类型    | 必填 | 默认值 | 描述 |
|-------------------------|---------|------|--------|------|
| endpoint                | String  | 否   | -      | 可选对外可达入口地址，格式 `host:port`（例如 K8s LB DNS:port 或 VPC EIP:port）。不替代 `port`；配置后建议由 agent/collector 手动指定并直连该地址，不依赖自动发现。 |
| port                    | Integer | 是   | -      | Zeta Worker 上的 ingress 绑定端口。 |
| local_queue_capacity    | Integer | 否   | 1024   | Source Reader 本地内存队列长度，必须大于 0。 |
| max_retries             | Integer | 否   | 3      | ingress 端口绑定失败的全局重试预算。耗尽后 Reader 失败；`-1` 表示无限重试。 |
| reconnect_interval_ms   | Integer | 否   | 1000   | ingress 端口重开间隔（毫秒）。 |
| accept_timeout_ms       | Integer | 否   | 1000   | `accept/read` 超时时间（毫秒）。 |
| packet_mode             | String  | 否   | RAW    | 外部入站包模式：`RAW` 或 `PACKET`。 |
| aes_secret_key_base64   | String  | 否   | -      | `PACKET` 模式下，当 `encryption=AES_GCM` 时用于解密的 Base64 AES 密钥。 |
| auth_type               | String  | 否   | TOKEN  | 入站连接鉴权类型，当前支持值：`TOKEN`。 |
| auth_token              | String  | 是   | -      | `TOKEN` 鉴权使用的 token 值。collector 发送业务数据前必须先完成认证。 |
| schema                  | Config  | 否   | -      | 可选 schema 定义。配置后按 JSON 反序列化输出。 |
| common-options          | -       | 否   | -      | Source 通用参数，详见 [Source Common Options](../common-options/source-common-options.md)。 |

## 作业示例

### 最小配置示例（推荐先从这里开始）

```hocon
source {
  EdgeSocket {
    port = 9999
    auth_token = "my-edge-token"
  }
}
```

::::tip 提示
其余参数均可省略：`auth_type` 默认 `TOKEN`，`packet_mode` 默认 `RAW`，重试与超时参数使用内置默认值。
::::

### 完整配置示例（显式指定关键参数）

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  EdgeSocket {
    port = 9999
    local_queue_capacity = 1024
    packet_mode = "RAW"
    auth_token = "my-edge-token"
  }
}

sink {
  Console {
    parallelism = 1
  }
}
```

若不配置 `endpoint`，Source 仍监听 `0.0.0.0:<port>`，collector 侧可按发现地址连接（前提是网络可达）。

## 如何选择接入方式（先看这里）

### 接入模式（明确语义）

- 模式 A（手动指定，推荐复杂网络使用）：配置 `endpoint`，由 agent/collector 直接使用该地址连接，不走自动发现。
- 模式 B（自动发现，推荐内网直连使用）：不配置 `endpoint`，仅配置 `port`，通过 `jobId` 发现 worker 地址并连接 `workerHost:port`。

当作业中存在多个 `EdgeSocket` source 时，建议优先使用模式 A，由 agent 侧明确选择目标 `endpoint`。

按下面 3 步判断即可：

1. collector 是否能直接访问 Zeta worker 暴露的地址？
   - 能：可以不配 `endpoint`，走自动发现（`workerHost:port`）。
   - 不能：必须配置 `endpoint`（例如 LB / EIP / NAT 入口），并由 agent/collector 直连。
2. 你的网络是否是 K8s / 跨 VPC / 公网混合（如 EIP）？
   - 是：建议固定配置 `endpoint`，避免地址漂移。
3. 边缘网络是否缺少 collector 可达入口？
   - 是：若没有可达入口（`endpoint`），不能直连；若已通过 LB/NAT/EIP/中转网关提供可达入口，仍可接入。

::::tip 提示
`endpoint` 不是 Source 本机绑定地址，不用于 `bind`。Source 始终绑定本地监听端口（当前为 `0.0.0.0:<port>`）；`endpoint` 只用于告诉 collector 应该连接哪个入口地址。
::::

## 边缘网络接入说明

`EdgeSocket` 使用 collector 主动连入 Source ingress 的模型，因此核心前提是：

- collector 到 `EdgeSocket` 的 `host:port` 必须可达；
- Source 不会主动回连 collector。

### 典型网络场景

| 场景 | 是否可直接使用 | 建议 |
| --- | --- | --- |
| VM 同 VPC / 同内网可路由 | 是 | 可不配 `endpoint`，直接使用集群发现地址 |
| VM 跨 VPC（已打通路由/防火墙） | 是 | 建议配置 `endpoint` 指向稳定入口 |
| VM 跨 VPC（未打通私网）+ VPC EIP | 是 | 使用 `endpoint=<EIP>:<port>`，collector 走公网入口 |
| 公网 collector -> 私网 worker（无 EIP/LB/NAT） | 否（默认） | 先暴露可达入口（EIP/LB/NAT），再配置 `endpoint` |
| K8s（Service/LB/Ingress）复杂网络 | 是（需网络入口） | 推荐通过 LB/Ingress 暴露固定入口并配置 `endpoint` |
| Source 所在网络“只出不入”（NAT 后仅出站、禁止 collector 直入） | 视是否有可达入口而定 | 若有 LB/EIP/NAT/网关入口可接入；否则需先建设中转通道 |

### 关于“边缘网络只出不入”

若运行 `EdgeSocket` Source 的网络是“只出不入”，关键不是字面策略本身，而是是否能提供 collector 可达入口：

- 能提供可达入口（LB/EIP/NAT/网关）：可接入，collector 连接该入口（建议配置 `endpoint`）。
- 不能提供任何可达入口：不能直连，需要先建设中转通道。

常见可选做法：

- 在中心侧部署可达网关，边缘 collector 主动连接到该网关，再由网关转发到 `EdgeSocket`；
- 使用反向隧道或专线，把边缘出站连接转换为中心可接收入口；
- 在网络层先打通双向可达（VPN/VPC Peering/防火墙规则）后再直连。

### K8s 复杂网络配置示例（推荐）

```hocon
source {
  EdgeSocket {
    # Source 仍在 Worker 内监听
    port = 10091
    auth_type = "TOKEN"
    auth_token = "edge-token"
    packet_mode = "RAW"
    local_queue_capacity = 2048
    max_retries = 5
    reconnect_interval_ms = 1000
    accept_timeout_ms = 5000

    # collector 直连入口（例如 LB DNS:port）
    endpoint = "edge-lb.prod.example.com:10091"
  }
}
```

### VM 简单网络配置示例（同内网可直连）

```hocon
source {
  EdgeSocket {
    port = 10091
    auth_type = "TOKEN"
    auth_token = "edge-token"
    packet_mode = "RAW"
    local_queue_capacity = 1024
    max_retries = 3
    reconnect_interval_ms = 1000
    accept_timeout_ms = 1000
    # 不配置 endpoint，使用发现地址（要求网络可达）
  }
}
```

### VPC + EIP 配置示例（公网 collector 常见）

```hocon
source {
  EdgeSocket {
    port = 10091
    auth_token = "edge-token"

    # VPC 云主机绑定的公网 EIP（或 EIP+NAT 暴露地址）
    endpoint = "203.0.113.10:10091"
  }
}
```

## 外部接入包协议

当 `packet_mode = "PACKET"` 时，每行必须是一个 JSON 包：

```json
{
  "version": 1,
  "payload": "<base64 payload bytes>",
  "compression": "NONE|GZIP|ZLIB|DEFLATE",
  "encryption": "NONE|AES_GCM",
  "iv": "<base64 iv, AES_GCM 必填>"
}
```

处理顺序为：入口阶段 `解密 + 入队`，消费阶段 `按 compression 解压 -> utf-8 字符串`。

## Token 认证

`auth_type` 默认是 `TOKEN`，该模式下 `auth_token` 为必填。

collector 连接后第一行必须发送：

```text
__AUTH__:<token>
```

Source 返回：

- `ACK`：认证通过，可以继续发送业务数据
- `AUTH_FAILED`：认证失败，collector 需重连并修正 token

## Edge Collector 协议参考

可执行示例请直接参考 UT/E2E 测试：

- `seatunnel-connectors-v2/connector-edge-socket/src/test/java`
- `seatunnel-e2e/seatunnel-connector-v2-e2e/connector-edge-socket-e2e/src/test/java`

collector 侧应遵循基于 checkpoint 的批次 ACK 协议：

- 首行发送认证报文，收到 `ACK` 才进入业务数据发送阶段
- 发送一条批次数据：`__BATCH__:<batchId>:<payload>`
- 等待入队响应：`RECEIVED` 或 `RETRY`
- 使用 `__COMMIT__:<batchId>` 轮询批次确认
- 收到 `ACK:<watermarkBatchId>` 后发送下一批

## Changelog

<ChangeLog />
