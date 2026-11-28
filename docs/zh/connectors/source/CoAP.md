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

`CoAP` Source 在指定 UDP 端口监听，接收配置资源上的 GET、POST 和 PUT。Source 并行度必须为 1。持续采集请使用 `STREAMING`，有界读取请将 `stop_after_messages` 设为正数。

连接器使用明文 UDP，请部署在适合无认证及无加密流量的网络中。该连接器的协议范围为单播 CoAP UDP。

## 参数

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `bind_host` | String | `0.0.0.0` | 监听地址，使用 IPv4 或 IPv6 字面量。 |
| `port` | Integer | `5683` | UDP 端口，范围为 1 至 65535。 |
| `resource_path` | String | `/sdc` | 绝对资源路径，包含 1 至 16 个路径段，每段最多 64 个 URI 非保留字符。 |
| `format` | Enum | `JSON` | 消息格式，支持 `JSON`、`TEXT`、`BINARY`、`CSV`、`AVRO`、`PROTOBUF`、`XML`、`LOG`、`DATAGRAM` 和 `RECORD_JSON`。 |
| `max_payload_bytes` | Integer | `65536` | 重组后响应体的字节数上限，范围为 16 至 1048576。 |
| `queue_capacity` | Integer | `128` | 等待读取的完整消息数上限，范围为 1 至 8192。 |
| `max_peers` | Integer | `64` | 可追踪的远端地址和端口组合数上限，范围为 1 至 1024。 |
| `max_concurrent_requests` | Integer | `4` | 协议工作线程数，范围为 1 至 32。 |
| `blockwise_timeout_ms` | Integer | `30000` | 分块传输的空闲超时，范围为 500 至 120000 毫秒。 |
| `stop_after_messages` | Long | `0` | 0 表示持续采集，正数表示读取指定数量的消息后结束。 |
| `schema` | Map | — | CSV、Avro 和 Protobuf 必须提供，JSON 可选。 |
| `csv_delimiter` | String | `,` | CSV 分隔符，须为一个可打印且非引号的字符。 |
| `avro_writer_schema` | String | — | 用于解码原始 Avro 的可选写入端 schema，默认从 `schema` 派生。 |
| `protobuf_schema` | String | — | Protobuf schema 内容，`format = PROTOBUF` 时必填。 |
| `protobuf_message_name` | String | — | 消息的完全限定名，`format = PROTOBUF` 时必填。 |
| `xml_record_path` | String | `/*` | 选择记录元素的 XPath。 |
| `xml_namespaces` | Map | `{}` | 按 XPath 前缀配置命名空间 URI。 |
| `xml_preserve_root` | Boolean | `false` | 在输出对象最外层保留被选元素的名称。 |
| `log_mode` | Enum | `COMMON_LOG_FORMAT` | 选择下方列出的日志解析器。 |
| `log_pattern` | String | — | 自定义正则、Grok 表达式、Apache 布局或 Log4j 布局。 |
| `log_groups` | Map | `{}` | REGEX 模式下输出字段名与捕获组编号的映射。 |
| `grok_definitions` | Map | `{}` | 补充命名 Grok 定义，正则分组使用非捕获语法。 |
| `datagram_mode` | Enum | `RAW_DATA` | 可选 `RAW_DATA`、`SYSLOG`、`NETFLOW` 或 `COLLECTD`。 |
| `datagram_separator` | String | — | RAW_DATA 的 UTF-8 文本分隔符，长度 1 至 32 字符。省略时输出完整字节数组。 |
| `netflow_max_templates` | Integer | `128` | NetFlow v9 模板缓存上限，范围为 1 至 1024。 |
| `netflow_template_timeout_ms` | Integer | `300000` | NetFlow v9 模板有效期，范围为 1000 至 3600000 毫秒。 |
| `collectd_username` | String | — | 用于鉴别 collectd 消息体的用户名。 |
| `collectd_password` | String | — | 用于验证 collectd 签名和解密的密码，须与用户名同时配置。 |
| `collectd_types` | Map | `{}` | 按 collectd 类型配置逗号分隔的指标名。 |
| `collectd_convert_time` | Boolean | `true` | 将 collectd 时间戳及间隔转换为毫秒。 |
| `collectd_omit_interval` | Boolean | `false` | 从 collectd 输出中省略间隔字段。 |
| `record_header_field` | String | — | 指定输出 schema 中用于保留 RECORD_JSON 消息头的字段。 |

## 消息处理

| 消息格式 | Content-Format | 消息内容 |
| --- | --- | --- |
| `JSON` | 50 | 有效的 UTF-8 JSON。Source 未提供 `schema` 时以 `payload` 保留 JSON 文本，提供 `schema` 时读取对象并输出类型化字段。Sink 将输入行编码为对象。 |
| `TEXT` | 0 | 一条严格 UTF-8 文本，允许空字符串。 |
| `BINARY` | 42 | 一组原始字节，允许空数组。 |
| `CSV` | 0 | 一行带分隔符的记录，无表头。 |
| `AVRO` | 42 | 一条原始 Avro 二进制记录。Sink 使用由输入行类型生成的 Avro schema。 |
| `PROTOBUF` | 42 | 按指定 schema 与消息名称读写一条二进制消息。 |
| `XML` | 41 | 输出 XPath 选中的 XML 元素。属性使用 `@名称`，重复子元素转换为数组，混合文本写入 `$text`。 |
| `LOG` | 0 | 通过 `log_mode` 解码为结构化记录。 |
| `DATAGRAM` | 42 | 内嵌 syslog、NetFlow、collectd 或原始数据报。 |
| `RECORD_JSON` | 50 | 一条或多条包含 `header` 及递归类型化 `value` 的 JSON 记录。 |

请求未携带 Content-Format 时按作业配置解析。显式指定其他格式时返回 4.15。完整且有效的消息成功进入内存队列后返回 2.03，无法接收时返回 5.03，格式错误时返回 4.00。Californium 负责 CON/NON 关联、消息去重和分块传输。分块消息不完整或超限时可返回相应的 CoAP 错误。发送端重传时，连接器可以在对端地址及消息 ID 仍处于有界去重窗口的情况下重用已缓存的响应。

监听器分别限制对端数量、入站协议任务、出站报文和已入队的完整消息。启动时会校验完整消息、部分重组和协议报文的合计容量，预算上限为 64 MiB。运行时内存还包含解码后的数据行、编解码器分配以及 SDK 状态。对端数量或协议队列达到上限时，数据报可能在进入应用队列前丢失。大消息须使用分块传输。Avro 容器文件或应用自有封装等不透明格式可通过 `BINARY` 传输，再由下游解码。

## Source 格式和解析上限

XML、LOG 和已解码的 DATAGRAM 在未配置 schema 时将结构化 JSON 写入 `payload` 列。RECORD_JSON 保留消息头并解码字段的类型标记和值。配置 schema 后，这些格式会映射为类型化字段。可通过 `record_header_field` 将 RECORD_JSON 消息头一并映射到类型化字段。每条消息最多生成 1024 行，整条消息通过校验后才会接收其中的记录。仅含模板的 NetFlow v9 消息可以成功处理且不产生输出行。

- XML 使用本地 XPath，禁用 DTD、外部实体及外部资源访问。
- LOG 支持 `COMMON_LOG_FORMAT`、`COMBINED_LOG_FORMAT`、`APACHE_ERROR_LOG_FORMAT`、`APACHE_CUSTOM_LOG_FORMAT`、`REGEX`、`GROK`、`LOG4J`、`CEF` 和 `LEEF`。每行解析一条日志。单行最多 16384 个字符，自定义匹配每行最多访问一百万次字符。Grok 可用自定义定义扩展内置的单词、数字、地址、时间戳和 Apache 访问日志模式。
- DATAGRAM 支持 RFC 3164 与 RFC 5424 syslog、NetFlow v5 和 v9 模板、选项及数据 FlowSet，以及 collectd 网络报文。NetFlow 按导出方 IP 和观测域缓存模板，模板在配置的有效期后失效。v9 数据 FlowSet 需要对应导出方的模板。配置两项 collectd 凭据后，解析器要求消息体含有效签名或可认证的加密内容。模板缓存仅在当前监听会话中有效。

## 检查点与交付

检查点记录已输出的完整消息数量和监听配置。恢复时重新绑定监听端口，并从已保存的数量继续。2.03 响应表示消息已进入易失性内存，Sink 持久化在后续独立完成。重启会清除未完成的分块、仍在队列中的消息及协议去重状态，重新发送的消息可能再次被接收。UDP 没有可供重放的远端位点。

## 示例

```hocon
env {
  job.mode = "STREAMING"
  parallelism = 1
}

source {
  CoAP {
    bind_host = "127.0.0.1"
    port = 5683
    resource_path = "/sensor/data"
    format = "JSON"
  }
}

sink {
  Console {}
}
```

<ChangeLog />
