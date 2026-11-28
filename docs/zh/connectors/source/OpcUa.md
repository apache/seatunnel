import ChangeLog from '../changelog/connector-opcua.md';

# OpcUa

{/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */}

通过 OPC TCP 协议读取实时 OPC UA 值和地址空间元数据。每张表使用一个 split，支持 Java 11。连接器使用匿名用户身份。安全通道另需客户端应用证书。

## 读取模式

- `POLLING` 分批读取当前值。每次读取后等待 `interval_ms`。轮询可能跳过间隔内的变化。
- `SUBSCRIBE` 创建监控项并接收服务端通知。服务端可以调整请求中的发布或采样间隔。
- `BROWSE_NODES` 遍历本地 Object 和 Variable 节点。每个可达节点只输出一次，父节点取首次发现时的父节点。结果不包含起始节点和远程引用。

值读取可以使用 `MANUAL` 和 `nodes`，通过 `FILE` 读取 JSON 文件，或使用 `BROWSE` 选择 `browse_root` 下的 Variable。文件应使用作业提交端和引擎进程都可访问的绝对路径，格式为别名到 NodeId 的对象，例如 `{"temperature":"nsu=urn:factory:line1;s=temperature"}`。按浏览结果选取的节点会周期刷新，找不到变量会报错。浏览跟随服务端续页，触及限制会报错。

NodeId 支持 `i=`（无符号整数）、`s=`（字符串）、`g=`（UUID）和 `b=`（Base64 字节），可用 `ns=<索引>;` 或 `nsu=<URI>;` 指定命名空间。服务端专有命名空间推荐使用 `nsu=`。若配置使用索引，连接器记录解析后的 URI，用于重连和检查点恢复。

## 安全与恢复

连接器选择服务端公布且主机、端口、路径、安全策略及消息安全模式均与配置匹配的端点。安全端点也支持在相同主机和端口的端点路径后附加 `/discovery` 进行发现。安全端点需要可信服务端证书的 SHA-256 指纹、预期的服务端应用 URI 和 Worker 本地 PKCS12 文件。连接器校验证书有效期、主机名和应用 URI。默认 `SignAndEncrypt` 保护机密性。None 端点需要同时设置 `security_policy = "None"`、`allow_insecure = true`。使用旧版策略时需要 `allow_legacy_security = true`。

持续读取使用流式作业。将 `max_records` 设置为正数后，值模式也可用于批作业。检查点保留命名空间绑定、有限读取进度和结束状态。服务端历史和待消费的订阅通知不进入检查点。重连可能重复或漏读。连接器发现订阅中断、通知丢失、转移失败、订阅状态或看门狗异常、监控集合变化时，输出 `GAP`。客户端通知缓冲区溢出会使作业失败。读取器停止时，取消正在进行的网络请求。

## 输出字段

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| kind | STRING | VALUE、NODE 或 GAP |
| node_id | STRING | 使用命名空间 URI 的规范化 NodeId |
| node_alias | STRING | 配置的别名或发现的浏览名称 |
| parent_id、browse_name、node_class | STRING | NODE 的浏览元数据 |
| depth | INT | 相对起始节点的深度，从 1 开始 |
| value_type、value_json | STRING | OPC UA 内置类型和 JSON 值 |
| quality | STRING | GOOD、UNCERTAIN 或 BAD |
| status_code | BIGINT | 以非负长整型保存的无符号 OPC UA 状态码 |
| source_time、server_time | STRING | ISO 8601 UTC 时间，分辨率为 100 纳秒 |
| source_picoseconds、server_picoseconds | INT | 可选的小数时间字段，单位为 10 皮秒 |
| diagnostic | STRING | GAP 的原因 |

支持 Boolean、SByte、Byte、Int16、UInt16、Int32、UInt32、Int64、UInt64、Float、Double、String、DateTime、Guid、ByteString 及这些类型的数组。整数保留完整精度，ByteString 使用 Base64，非有限浮点数用 JSON 字符串承载。Bad 值输出 JSON 字面量 `null` 并保留状态信息。

## 配置项

| 配置项 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `endpoint` | string | — | 服务端公布的 OPC TCP 地址，含端口及路径。 |
| `read_mode` | string | `POLLING` | 读取模式，取值为 POLLING、SUBSCRIBE 或 BROWSE_NODES。 |
| `node_fetch_mode` | string | `MANUAL` | 值读取的节点选择模式，取值为 MANUAL、FILE 或 BROWSE。 |
| `nodes` | map | — | 别名到 OPC UA 文本 NodeId 的映射，MANUAL 必填。 |
| `node_file` | string | — | 别名与文本 NodeId 的 JSON 映射文件，FILE 必填。 |
| `browse_root` | string | `i=84` | 浏览起始节点的文本 NodeId。 |
| `browse_page_size` | int | `100` | 每页请求返回的最大引用数。 |
| `max_nodes` | int | `1024` | 每次浏览或显式选择允许的最大唯一节点数。 |
| `max_depth` | int | `16` | 从起始节点向下浏览的最大深度。 |
| `interval_ms` | int | `1000` | 轮询间隔，或订阅发布和采样间隔。 |
| `refresh_interval_ms` | int | `3600000` | 按浏览结果选取的值节点刷新间隔。 |
| `read_batch_size` | int | `100` | 一次服务请求允许读取的最大值数或创建监控项数。 |
| `queue_capacity` | int | `1024` | 每个读取器可排队的最大订阅记录数。 |
| `max_records` | long | `0` | 值模式的最大输出行数，0 表示持续读取。 |
| `request_timeout_ms` | int | `5000` | 一次采集操作或完整浏览的时间上限。 |
| `session_timeout_ms` | int | `120000` | 请求的会话超时以及订阅连接恢复预算。 |
| `security_policy` | string | `Basic256Sha256` | 安全策略，取值为 None、Basic128Rsa15、Basic256 或 Basic256Sha256。 |
| `message_security_mode` | string | 随安全策略变化 | None 策略默认 None，其他策略默认 SignAndEncrypt。 |
| `allow_insecure` | boolean | `false` | 允许显式选择 None 策略。 |
| `allow_legacy_security` | boolean | `false` | 允许旧版 Basic128Rsa15 或 Basic256 策略。 |
| `application_uri` | string | `urn:apache:seatunnel:opcua:client` | 客户端应用 URI，须与客户端证书 SAN URI 一致。 |
| `server_application_uri` | string | — | 预期的服务端应用 URI，安全策略启用时必填。 |
| `server_certificate_sha256` | string | — | 受信服务端应用证书的 SHA-256 指纹，安全策略启用时必填。 |
| `client_key_store` | string | — | Worker 本地的 PKCS12 客户端密钥库，安全策略启用时必填。 |
| `client_key_alias` | string | `client` | 密钥库中的客户端私钥别名。 |
| `password` | string | — | PKCS12 密钥库和私钥的密码，安全策略启用时必填。 |
| `max_chunk_size` | int | `65536` | 单个安全通道分块的最大字节数。 |
| `max_chunk_count` | int | `32` | 安全通道每条消息允许的最大分块数。 |
| `max_message_size` | int | `2097152` | 接收的 OPC UA 消息最大字节数，也限制单个 JSON 值。 |
| `max_array_length` | int | `65536` | 一个 OPC UA 数组解码后允许的最大元素总数。 |
| `max_string_bytes` | int | `65536` | 标准化前单个字符串或 ByteString 的最大字节数。 |

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  OpcUa {
    endpoint = "opc.tcp://opcua.example.org:4840/plant"
    read_mode = "POLLING"
    nodes = { temperature = "nsu=urn:factory:line1;s=temperature" }
    max_records = 1
    application_uri = "urn:example:seatunnel:opcua:client"
    server_application_uri = "urn:example:opcua:plant"
    server_certificate_sha256 = "<trusted-certificate-sha256>"
    client_key_store = "/etc/seatunnel/opcua-client.p12"
    password = ${OPCUA_KEYSTORE_PASSWORD}
  }
}
sink {
  Console {}
}
```

<ChangeLog />
