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

import ChangeLog from '../changelog/connector-websocket-server.md';

# WebSocketServer

`WebSocketServer` Source 在 SeaTunnel Worker 上监听入站 RFC 6455 WebSocket 连接。流式作业每接收一条完整数据消息，就生成一条记录。安装 `connector-websocket` 后，在 Source 中配置 `WebSocketServer`。

每个作业由一个 Worker 启动监听器。将 Source 并行度设置为 `1`，向发送端提供 Worker 的可达地址及端口，并在可能运行该 Source 的各个 Worker 上部署密钥库。恢复时作业可能换到另一台 Worker。如有这种情况，请为发送端提供指向当前监听器的稳定路由。

## 消息格式

- `raw` 输出 `message_type STRING`（`text` 或 `binary`）和 `payload BYTES`。文本消息须通过 UTF-8 校验，输出保留原始字节。
- `text` 解码 UTF-8，默认输出单列 `value STRING`。同时配置 `schema` 和 `field_delimiter` 可将每条消息拆分成多列。
- `json` 按必填的 `schema` 将一条 UTF-8 JSON 对象解析成一行。消息不符合配置的格式或列定义时，作业失败。

客户端可以发送分片消息，以及 ping、pong 和 close 帧。监听器在组装消息时也会处理交错的控制帧。客户端发送带掩码的帧，监听器发送不带掩码的控制响应。协议错误触发带对应状态码的断开。单一连接内保持消息顺序，来自不同客户端的消息可以交错。

## 配置项

| 配置项 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `bind_host` | String | `127.0.0.1` | Worker 监听地址。需要接受远程连接时，请使用客户端可达的网络接口。 |
| `port` | Integer | 必填 | 监听端口，1 至 65535。 |
| `path` | String | `/` | HTTP 升级的精确路径，最长 4096 字符。客户端请求路径不带查询字符串。 |
| `expected_host` | String | 可选 | 精确匹配 HTTP Host，配置包含端口时也匹配端口。每次升级均校验 Host 头是否合法。 |
| `allowed_origins` | String 列表 | `[]` | 允许的 HTTP(S) Origin。客户端携带 Origin 时必须匹配该列表，客户端省略 Origin 时可连接。 |
| `token` | String | 可选 | 配置后要求 `Authorization: Bearer <token>`。凭据经网络传输时请启用 TLS。 |
| `keystore_path` | String | 可选 | 包含 TLS 服务端证书和私钥的密钥库。配置后启用 `wss://`，客户端根据 URL 的主机名验证证书。 |
| `keystore_type` | String | `PKCS12` | 密钥库类型，支持 `PKCS12`、`JKS`。 |
| `password` | String | 配置 `keystore_path` 时必填 | 密钥库和私钥密码。 |
| `format` | String | `raw` | 可选值为 `raw`、`text`、`json`。 |
| `schema` | Config | 使用 `json` 时必填 | 结果列定义，使用 `text` 时可选。 |
| `field_delimiter` | String | `,` | 按列解析文本所用的分隔符。 |
| `max_connections` | Integer | `16` | 并发连接上限，1 至 64，包含正在握手的连接。 |
| `max_message_bytes` | Integer | `262144` | 完整消息字节数上限，1 至 4194304。该值与连接上限的乘积最多为 32 MiB。 |
| `max_queue_messages` | Integer | `1024` | 等待处理的消息数上限，1 至 10000。空消息也占一个名额。 |
| `max_queue_bytes` | Integer | `4194304` | 等待处理的消息字节数上限，从 `max_message_bytes` 到 32 MiB。 |
| `handshake_timeout_ms` | Integer | `5000` | TLS 与 HTTP 升级的总时限，100 至 120000 毫秒。 |
| `idle_timeout_ms` | Integer | `20000` | 已建立的连接等待下一帧的最长时间，100 至 600000 毫秒。 |
| `message_timeout_ms` | Integer | `10000` | 接收一帧或完整分片消息的总时限，100 至 120000 毫秒。 |

队列满时，监听器以状态码 `1013` 断开发送该消息的连接。发送端可重连，并依据自身的持久化缓冲和投递策略决定是否重发。连接升级、写入帧成功和 pong 只确认对应的协议操作。保存点停止、取消或失败时，连接和内存消息随监听器释放。检查点保存 SeaTunnel 的 Source 分配。需要重放和去重时，由发送端和下游基于业务消息标识实现。

## 示例

```hocon
env {
  job.mode = "STREAMING"
  parallelism = 1
}
source {
  WebSocketServer {
    bind_host = "0.0.0.0"
    port = 18898
    path = "/ingest"
    expected_host = "ingest.example.com:18898"
    allowed_origins = ["https://app.example.com"]
    token = "${websocket_token}"
    keystore_path = "/etc/seatunnel/ingest.p12"
    password = "${websocket_keystore_password}"
    format = "json"
    schema {
      fields {
        id = bigint
        value = string
      }
    }
  }
}
sink {
  Console {}
}
```

<ChangeLog />
