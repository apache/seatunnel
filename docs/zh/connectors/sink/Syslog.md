import ChangeLog from '../changelog/connector-syslog.md';

# Syslog

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

> 使用 TLS 发送 RFC 5424 消息，采用 RFC 5425 字节计数分帧。

## 描述

将 INSERT 行发送到 TLS Syslog 接收端。本连接器仅为 Sink，不监听或接收 Syslog。
不支持 RFC 3164、明文 TCP、UDP、RELP、事务或自动重连重试。

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)

## 支持的数据源

接收端必须支持 RFC 5424、TLS 和 RFC 5425 分帧，例如 syslog-ng 的
`syslog(transport("tls"))` Source。仅支持换行分隔的旧式 TCP 监听器不兼容。
连接器使用 JDK TLS，不需要额外客户端库。
消息字节上限不得超过接收端限制，并非所有接收端都接受 8192 字节。

## Sink 选项

| 名称 | 类型 | 必需 | 默认值 | 描述 |
|------|------|------|--------|------|
| host | String | 是 | - | 接收端 DNS 名称或 IP，不含协议或端口，必须与证书匹配。 |
| port | Int | 否 | 6514 | 接收端端口，范围 1-65535。 |
| connect_timeout_ms | Int | 否 | 10000 | TCP 连接超时毫秒数，必须为正数。不包括 JVM/平台 DNS 解析时间。 |
| write_timeout_ms | Int | 否 | 10000 | 每次 TLS 握手、写入/刷新和关闭的超时毫秒数，必须为正数。 |
| max_message_bytes | Int | 否 | 8192 | 编码后的消息上限，范围 1-1048576 字节。包括头、结构化数据和 UTF-8 BOM，不含长度前缀。超限报错，不截断。 |
| tls.ca_cert_path | String | 否 | - | Worker 本地 PEM CA 证书集合；未配置时使用 JVM 默认信任证书。 |
| tls.key_store.path | String | 否 | - | 双向 TLS 使用的 Worker 本地客户端密钥库。 |
| password | String | 配置密钥库时 | - | 密钥库及私钥密码。 |
| tls.key_store.type | String | 否 | PKCS12 | 客户端密钥库类型：`PKCS12` 或 `JKS`。 |
| common-options | | 否 | - | [通用 Sink 选项](../common-options/sink-common-options.md)，包括 `plugin_input`。 |

## 输入 Schema

列名固定，可用 Transform 重命名。其他列被忽略。可选列一旦存在，必须使用指定类型。
Schema 校验不会访问网络或读取 TLS 文件。

| 列 | 类型 | 缺失或 null 时 | 约束 |
|----|------|---------------|------|
| message | STRING | 列必需；null 不发送 MSG | UTF-8 文本；空字符串与 null 不同。非 null MSG 带 UTF-8 BOM。 |
| facility | INT | 1（用户级） | 0-23，PRI = facility * 8 + severity。 |
| severity | INT | 6（信息） | 0-7。 |
| timestamp | STRING | `-` | RFC 5424 时间戳，使用大写 T/Z 或数字时区偏移，最多六位小数秒。不接受非法日期、闰秒或无时区时间。不自动生成时间戳。 |
| hostname | STRING | `-` | 1-255 个不含空格的可打印 US-ASCII 字符。 |
| app_name | STRING | `-` | 1-48 个不含空格的可打印 US-ASCII 字符。 |
| proc_id | STRING | `-` | 1-128 个不含空格的可打印 US-ASCII 字符。 |
| msg_id | STRING | `-` | 1-32 个不含空格的可打印 US-ASCII 字符。 |
| structured_data | MAP&lt;STRING, MAP&lt;STRING, STRING&gt;&gt; | `-`，空 Map 也相同 | SD-ID 到参数 Map 的映射，不接受原始结构化数据字符串。 |

SD-ID 和参数名为 1-32 个可打印 ASCII 字符，不得含空格、`=`、`]`、`"`。
参数值必须是非 null 字符串，允许空参数 Map。参数值中的引号、反斜杠和右方括号会被转义。
请使用 IANA 注册的 SD-ID 或带自身企业编号的标识；`example@32473` 仅供文档示例使用。
非法 Unicode 被拒绝。MSG 和参数值中的换行及控制字符被保留，字节分帧使它们仍属于同一消息，
但接收端的存储或显示策略可能修改这些字符。头字段中的空格及控制字符被拒绝，不进行转义。
RFC 3164 Source 的 `Oct 11 22:14:15` 等时间戳必须先显式转换。

## 交付语义和资源限制

- 每个 Writer 使用一个 TLS 连接，同步发送消息，没有异步消息队列。同一 Writer 最多有一次写入进行中。
  字节上限限制每个帧及中间编码缓冲区。
- 写入成功或检查点刷新成功仅表示本地 TLS/Socket 操作完成。
  Syslog 不提供对接收端解析、持久化或下游交付的应用级确认。
  本 Sink 不保证至少一次或精确一次；故障和恢复可能造成丢失或重复。
- 部分写入、超时或检测到断连会永久终止该 Writer，不会静默重试。
  引擎恢复可能重新执行输入，从而在接收端产生重复消息。
- 两个 prepare-commit 方法、snapshot 和 close 都刷新或检查连接并传播传输错误。
  不提供事务提交或可恢复的 Writer 状态。
- 超时监控直接关闭底层 TCP Socket 以中断阻塞的 TLS 写入，不能仅用读超时来限制写入。
  并发 close 会中断进行中的操作；中断写入线程可能需要等待至当前操作的超时期限。
  DNS 解析受 JVM/平台解析器限制，需单独配置。
- 操作超时应小于作业检查点超时。并行 Writer 使用不同连接，不保证全局顺序。
  本地写入成功无法发现所有接收端故障，例如接收端接收后丢弃消息。

## TLS 和安全

必须使用 TLS，仅启用 Worker JDK 支持的 TLS 1.2 和 TLS 1.3。
使用配置的 CA 集合或 JVM 默认信任证书验证接收端证书链，并通过 JSSE HTTPS 端点标识校验 host。
不提供信任全部证书或跳过主机名校验的选项，也不修改 JVM 全局 TLS 设置。

信任证书和客户端密钥库必须部署到所有 Worker。限制私钥文件的访问权限，通过配置替换或部署环境的
密钥管理提供 `password`，不要提交真实密码。双向 TLS 必须同时配置密钥库路径和密码，
接收端也必须信任该客户端证书。TLS 认证的是传输对端，不是输入行中的 hostname 或应用标识。

## 示例

接收端应配置为使用 TLS 接收 RFC 5424 字节计数帧。替换以下端点和 CA 路径后运行。
示例文件：`seatunnel-connectors-v2/connector-syslog/examples/fake_to_syslog.conf`。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}
source {
  FakeSource {
    row.num = 1
    schema.fields {
      facility = int
      severity = int
      timestamp = string
      hostname = string
      app_name = string
      proc_id = string
      msg_id = string
      structured_data = "map<string, map<string, string>>"
      message = string
    }
    rows = [{
      kind = INSERT
      fields = [1, 6, "2026-01-02T03:04:05.123Z", "origin", "seatunnel", "-", "ID47",
        {"example@32473": {"component": "pipeline"}}, "job completed"]
    }]
  }
}
sink {
  Syslog {
    host = "syslog.example.org"
    port = 6514
    tls.ca_cert_path = "/etc/seatunnel/syslog-ca.pem"
    connect_timeout_ms = 10000
    write_timeout_ms = 10000
    max_message_bytes = 8192
  }
}
```

可选客户端认证：

```hocon
tls.key_store.path = "/etc/seatunnel/syslog-client.p12"
tls.key_store.type = "PKCS12"
password = ${SYSLOG_KEY_STORE_PASSWORD}
```

<ChangeLog />
