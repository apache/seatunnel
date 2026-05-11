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

collector 每推送一行数据时，Source 按以下规则响应：

- 若本地队列已满，返回 `RETRY`；
- 若可接收，则包入队并返回 `ACK`。

队列操作逻辑内聚在本地队列组件（`queue` 包）中，socket 入口逻辑只负责协议解码和入队调用。
压缩处理在队列 `pollNext` 路径按压缩类型执行。

## 数据类型映射

未配置 `schema` 时，该 Source 输出一个名为 `value` 的字符串字段。
配置 `schema` 后，会按 JSON 反序列化并输出为用户定义的字段结构。

| SeaTunnel 数据类型 |
|--------------------|
| STRING             |

## 参数说明

| 参数名 | 类型 | 是否必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| host | String | 否 | - | 可选对外可达地址（用于发现结果暴露，例如 K8s LB/DNS）。未配置时发现层回退到 Worker 运行时地址；Source 实际监听地址固定为 `0.0.0.0` |
| port | Integer | 是 | - | Zeta Worker 上的 ingress 绑定端口 |
| local_queue_capacity | Integer | 否 | 1024 | Source Reader 本地内存队列长度，必须大于 0 |
| max_retries | Integer | 否 | 3 | ingress 端口绑定失败后的最大重试次数，`-1` 表示无限重试 |
| reconnect_interval_ms | Integer | 否 | 1000 | ingress 端口重开间隔（毫秒） |
| accept_timeout_ms | Integer | 否 | 1000 | `accept/read` 超时时间（毫秒） |
| packet_mode | String | 否 | RAW | 外部入站包模式：`RAW` 或 `PACKET` |
| aes_secret_key_base64 | String | 否 | - | `PACKET` 模式下，当 `encryption=AES_GCM` 时用于解密的 Base64 AES 密钥 |
| auth_type | String | 否 | TOKEN | 入站连接鉴权类型，当前支持值：`TOKEN` |
| auth_token | String | 是 | - | `TOKEN` 鉴权使用的 token 值。collector 发送业务数据前必须先完成认证 |
| schema | Config | 否 | - | 可选 schema 定义。配置后按 JSON 反序列化输出 |
| common-options | - | 否 | - | Source 通用参数，详见 [Source Common Options](../common-options/source-common-options.md) |

## 作业示例

```bash
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  EdgeSocket {
    port = 9999
    auth_type = "TOKEN"
    local_queue_capacity = 1024
    packet_mode = "RAW"
    auth_token = "my-edge-token"
    max_retries = 3
    reconnect_interval_ms = 1000
    accept_timeout_ms = 1000
  }
}

sink {
  Console {
    parallelism = 1
  }
}
```

若不配置 `host`，Source 仍监听 `0.0.0.0:<port>`，但发现结果会回退到 Worker 运行时地址。

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

## Edge Collector 示例

可以使用示例 collector 脚本进行联调：

```shell
python3 seatunnel-connectors-v2/connector-edge-socket/examples/edge-collector.py --host 127.0.0.1 --port 9999 --token my-edge-token --interval-ms 500
```

使用 PACKET + GZIP/ZLIB/DEFLATE 模式：

```shell
python3 seatunnel-connectors-v2/connector-edge-socket/examples/edge-collector.py --host 127.0.0.1 --port 9999 --token my-edge-token --packet-mode packet --compression deflate
```

使用 token 认证：

```shell
python3 seatunnel-connectors-v2/connector-edge-socket/examples/edge-collector.py --host 127.0.0.1 --port 9999 --token my-edge-token
```

示例脚本遵循按行 ACK 协议：

- 首行发送认证报文，收到 `ACK` 才进入业务数据发送阶段
- 发送一行数据
- 等待 Source 响应
- 收到 `ACK` 发送下一条
- 收到 `RETRY` 延迟后重发当前条

## Changelog

<ChangeLog />
