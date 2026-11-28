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

import ChangeLog from '../changelog/connector-udp.md';

# UDP

UDP Source 在 Worker 上监听配置的端口，每个报文独立处理。端到端测试使用 SeaTunnel Zeta。

每个端口由一个 Reader 独占。Source 并行度可设置为 1 至端口数量。连接器先排序端口，再以轮询方式分配。分布式部署时，发送端应访问对应 Reader 所在的 Worker。其他进程或作业无法同时绑定相同的地址和端口。

## Source 参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `ports` | List&lt;Integer&gt; | 是 | | 1 至 64 个互不重复的 UDP 端口，每个端口范围为 1–65,535。 |
| `bind_host` | String | 否 | `0.0.0.0` | 本机 IPv4 或 IPv6 数字地址。无需且不支持主机名和 IPv6 scope ID。 |
| `format` | String | 否 | `TEXT` | `TEXT`、`BINARY`、`SYSLOG`、`COLLECTD` 或 `NETFLOW_V5`，不区分大小写。 |
| `separator` | String | 否 | 空 | `TEXT` 和 `BINARY` 可使用的固定分隔符，最长 64 个 UTF-8 字节。 |
| `max_datagram_bytes` | Integer | 否 | `65507` | 可接收的报文负载字节数，范围 1–65,527。实际网络栈可能有更小的上限。 |
| `receive_buffer_bytes` | Integer | 否 | `262144` | 每个套接字向操作系统请求的接收缓冲区字节数，范围 1,024–16,777,216。操作系统可调整该值。 |
| `max_datagrams_per_poll` | Integer | 否 | `32` | 每个 Reader 单次轮询处理的报文上限，范围 1–128。 |
| `max_records_per_datagram` | Integer | 否 | `256` | 每个报文最多可解码的行数，范围 1–1,024。 |
| `idle_timeout_ms` | Integer | 否 | `0` | 端口在未收到报文后停止监听的时间，范围 0–86,400,000 毫秒。0 表示持续监听。BATCH 模式要求正值。 |
| `on_malformed` | String | 否 | `DROP` | `DROP` 丢弃错误、超长或解码行数超出上限的报文。 `FAIL` 使作业失败。 |

## 输出格式

每种格式都追加 `sender_host`（数字 IP 字符串）、`sender_port`（整数）、`listen_port`（整数）和 `record_index`（报文内从零开始的记录位置）。发送端地址和端口来自套接字，未经认证。

| 格式 | 特定字段与行为 |
| --- | --- |
| `TEXT` | `payload` 为严格校验的 UTF-8 字符串，保留换行和空报文。配置 `separator` 后，仅在单个报文内拆分并跳过空片段。 |
| `BINARY` | `payload` 为原始 BYTES，可为零字节。配置分隔符后使用同样的字节级拆分规则。 |
| `SYSLOG` | 每个报文包含一条 RFC 5424 版本 1 或 RFC 3164 消息。输出 `facility`、`severity`、`version`、`event_time`、`hostname`、`app_name`、`process_id`、`message_id`、`structured_data` 和 `message`。两种消息都采用严格的 UTF-8 校验。保留传统格式不带年份的本地时间，以及 RFC 5424 的结构化数据原文。NILVALUE 字段输出 null。 [UDP 传输规范](https://www.rfc-editor.org/rfc/rfc5426.html) |
| `COLLECTD` | 明文 Network 插件指标。输出 `hostname`、`time_ms`、`interval_ms`、`plugin`、`plugin_instance`、`metric_type`、`type_instance`、`value_index`、`value_type`、`integer_value` 和 `gauge_value`。无符号 COUNTER、ABSOLUTE 以及有符号 DERIVE 使用 DECIMAL(20,0)，GAUGE 使用 DOUBLE。暂不支持签名、加密、通知报文或 types 数据库别名。 [collectd 报文规范](https://github.com/collectd/collectd/wiki/Binary-protocol) |
| `NETFLOW_V5` | 每条流输出一行。报文头字段为 `uptime_ms`、`unix_seconds`、`unix_nanoseconds`、`flow_sequence`、`engine_type`、`engine_id`、`sampling_mode`、`sampling_interval`。地址和计数字段为 `source_ip`、`destination_ip`、`next_hop`、`input_interface`、`output_interface`、`packets`、`octets`、`first_ms`、`last_ms`。其余字段为 `source_port`、`destination_port`、`tcp_flags`、`protocol`、`tos`、`source_as`、`destination_as`、`source_mask`、`destination_mask`。暂不支持 NetFlow v9 或 IPFIX。 [Cisco 报文规范](https://www.cisco.com/c/en/us/td/docs/net_mgmt/netflow_collection_engine/5-0-3/user/guide/format.html) |

## 交付与关闭

UDP 尽力交付且没有应答。网络可能重排、复制或丢弃报文。下游产生背压时，接收报文由操作系统缓冲区排队。缓冲区耗尽后，由操作系统丢弃报文。源指标 `udpMalformedDatagrams` 和 `udpOversizedDatagrams` 统计连接器拒绝的报文。操作系统在连接器收到前丢弃的报文不计入这些指标。被拒绝的报文不会输出部分记录。

检查点仅保存端口的归属和终止状态。恢复时，活跃端口重新绑定并开始新的空闲计时，已结束的端口保持关闭。恢复时应保持端口列表一致。连接器没有可重放的报文或偏移量。同一次接收最多处理一次，但发送端或网络产生的重复报文仍会被处理。

## 示例

```hocon
env {
  job.mode = "STREAMING"
  parallelism = 1
}

source {
  UDP {
    bind_host = "0.0.0.0"
    ports = [5514]
    format = "SYSLOG"
  }
}

sink {
  Console {}
}
```

<ChangeLog />
