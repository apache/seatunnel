import ChangeLog from '../changelog/connector-websocket.md';

# WebSocket 客户端 Sink

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

WebSocket Sink 主动连接 `ws://` 或 `wss://` 服务端，每条插入记录发送一条消息。每个 writer 在第一条有效记录到达时建立连接并保持本 writer 的发送顺序。并行 writer 使用独立连接。输入为空时不会建立连接。

| 格式 | WebSocket 消息类型 | 输入 |
| --- | --- | --- |
| JSON | UTF-8 文本 | 将整行编码为一个 JSON 对象。 |
| TEXT | UTF-8 文本 | 发送所选 STRING 列的值，不附加分隔符。 |
| BINARY | 二进制 | 发送所选 BYTES 列的原始字节，不进行 Base64 编码。 |

TEXT 和 BINARY 所选值不能为空。更新和删除记录会使任务报错。使用 SeaTunnel 插件安装工具安装 `connector-websocket`。

## Sink 参数

| 名称 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| url | String | 是 | - | WebSocket 地址。路径不得包含密钥，地址不得包含用户信息、查询参数或片段。 |
| format | String | 否 | `JSON` | `JSON`、`TEXT` 或 `BINARY`。 |
| field | String | 视格式而定 | - | TEXT 或 BINARY 使用的输入列。 |
| token | String | 否 | - | 用于 `wss://` 的 Bearer token，通过部署环境的密钥机制提供。 |
| connect_timeout_ms | Int | 否 | `10000` | 建立连接的握手时限，范围 1–300000 毫秒。 |
| write_timeout_ms | Int | 否 | `10000` | Socket 写入和等待队列空间的时限，范围 1–300000 毫秒。 |
| flush_timeout_ms | Int | 否 | `10000` | 本地发送队列排空时限，范围 1–300000 毫秒。 |
| close_timeout_ms | Int | 否 | `10000` | 发送队列排空后正常关闭握手的时限，范围 1–300000 毫秒。 |
| max_frame_bytes | Int | 否 | `1048576` | 单条消息压缩前的字节数上限，范围 1 至 `max_queue_bytes`。 |
| max_queue_bytes | Int | 否 | `4194304` | 每个 writer 待发送数据的字节数上限，范围 1–16777216。 |
| max_inbound_message_bytes | Int | 否 | `1048576` | 每条入站消息（包含全部续帧）的字节上限，范围 1–16777216。超限时在无界聚合前失败。 |

客户端校验 TLS 证书及主机名，并拒绝重定向。该 Sink 不协商 WebSocket 扩展（包括 permessage-deflate）；收到扩展帧会明确失败。发送队列同时限制字节数和消息数。入站应用数据按帧丢弃，不进行整条消息聚合；传输帧与整条消息累计字节数均受 `max_inbound_message_bytes` 限制。单条消息超限时不会加入队列。连接、写入或关闭故障会使 writer 失败。应连接可信的接收服务。对端返回的数据消息不会被视为对输出记录的确认。

客户端沿用部署 JVM 的默认代理选择器：普通 `ws` 使用 HTTP 代理转发，`wss` 先建立 HTTP CONNECT 隧道再进行 TLS 握手；SOCKS5 由代理解析目标主机。既有 JVM 代理和绕过规则继续生效，连接器不修改全局选择器。SOCKS5 密码质询使用 JVM 默认认证器；认证器未提供凭据时，使用 `user.name` 和空密码。本 Sink 不配置 HTTP 代理认证。

## 投递与恢复

Flush 和 checkpoint 准备阶段等待本地发送队列排空。正常停机还等待 WebSocket 关闭握手。这些操作无法确认服务端应用接收或持久化记录。该 Sink 不提供应用层确认、事务状态或 exactly-once 保证。

连接失败后，writer 不会自动重连或重发。发生错误之前，服务端可能已经接收部分消息。任务重试或从 checkpoint 恢复后，源端重放可能导致同一条消息再次发送。如需避免重复处理，请在接收端实现幂等。

## 示例

通过已认证的 TLS 连接，每条插入记录发送一条 UTF-8 JSON 消息。

```hocon
sink {
  WebSocket {
    url = "wss://collector.example.com/events"
    format = "JSON"
    token = "replace-with-bearer-token"
  }
}
```

<ChangeLog />
