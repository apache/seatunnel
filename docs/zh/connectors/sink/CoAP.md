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

import ChangeLog from '../changelog/connector-coap.md';

# CoAP

`CoAP` Sink 为每一行输入发起一次 UDP 交换，等待最终的 2.xx 响应。URL 必须是单一的单播 `coap://` 目标端点。请在适合无认证及无加密流量的网络中使用。

## 参数

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `url` | String | — | 必填，单播 UDP 端点的 `coap://` URI。 |
| `method` | Enum | `POST` | 请求方法，支持 `GET`、`POST`、`PUT` 或 `DELETE`。 |
| `request_type` | Enum | `NON` | `CON` 发送可确认消息，`NON` 发送不可确认消息。 |
| `request_timeout_ms` | Integer | `5000` | 每行请求的总时间上限，范围为 1 至 120000 毫秒。 |
| `format` | Enum | `JSON` | 消息格式，支持 `JSON`、`TEXT`、`BINARY`、`CSV`、`AVRO`、`PROTOBUF` 和 `RECORD_JSON`。 |
| `max_payload_bytes` | Integer | `65536` | 重组后响应体的字节数上限，范围为 16 至 1048576。 |
| `blockwise_timeout_ms` | Integer | `30000` | 分块传输的空闲超时，范围为 500 至 120000 毫秒。 |
| `csv_delimiter` | String | `,` | CSV 分隔符，须为一个可打印且非引号的字符。 |
| `protobuf_schema` | String | — | Protobuf schema 内容，`format = PROTOBUF` 时必填。 |
| `protobuf_message_name` | String | — | 消息的完全限定名，`format = PROTOBUF` 时必填。 |
| `payload_field` | String | `payload` | `TEXT` 和 `BINARY` 分别使用指定的 `STRING` 或 `BYTES` 输入列。 |
| `avro_container` | Boolean | `true` | 使用含 schema 的 Avro 容器，每个容器写入一条记录。设置为 `false` 时写入原始二进制记录。 |

## 请求与消息体

GET 和 DELETE 请求不发送消息体。POST 和 PUT 使用所选格式编码消息体。

| 消息格式 | Content-Format | 消息内容 |
| --- | --- | --- |
| `JSON` | 50 | 有效的 UTF-8 JSON。Source 未提供 `schema` 时以 `payload` 保留 JSON 文本，提供 `schema` 时读取对象并输出类型化字段。Sink 将输入行编码为对象。 |
| `TEXT` | 0 | 一条严格 UTF-8 文本，允许空字符串。 |
| `BINARY` | 42 | 一组原始字节，允许空数组。 |
| `CSV` | 0 | 一行带分隔符的记录，无表头。 |
| `AVRO` | 42 | 按输入行类型生成 schema，写入对象容器或一条原始 Avro 记录。 |
| `PROTOBUF` | 42 | 按指定 schema 与消息名称读写一条二进制消息。 |
| `RECORD_JSON` | 50 | 将 SeaTunnel 行写入含消息头和类型化字段的 JSON 记录。 |

CON 请求的协议重传会复用同一交换标识。NON 请求不要求协议确认。两种模式均以收到最终成功响应作为写入成功条件。收到 RST、非 2.xx 响应或超时，都会使写入失败。超时可能发生在远端已执行之后，因此连接器遇到结果不明时不另起一次业务请求。

每个 Writer 同时处理一个请求，一行返回后本地无该行的待发送记录。检查点和作业结束均在此前的写入处理完成后进行。作业从上游恢复时可能再次发送同一行。远端应用响应确认其处理结果，持久化和事务由远端独立决定。

## 示例

```hocon
sink {
  CoAP {
    url = "coap://device.example:5683/sensor/data"
    method = "PUT"
    request_type = "CON"
    format = "JSON"
    request_timeout_ms = 5000
  }
}
```

<ChangeLog />
