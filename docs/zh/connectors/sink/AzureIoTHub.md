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

import ChangeLog from '../changelog/connector-http-azure-iot-hub.md';

# AzureIoTHub

Azure IoT Hub Sink 将每条 INSERT 记录作为一条设备到云端的遥测消息，通过 HTTPS 调用 [Send Device Event REST API](https://learn.microsoft.com/en-us/rest/api/iothub/device/device/send-device-event?view=rest-iothub-device-2021-11-30)。连接器只负责生产设备遥测。端到端测试使用 SeaTunnel Zeta。

请先在 IoT Hub 中注册设备，启用对称密钥认证，并提供设备的一个 Base64 密钥。每次尝试发送时，连接器都会生成仅对该设备有效的 SAS 令牌。连接器使用 Worker JVM 的证书信任库并校验服务器主机名。

## Sink 参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `hub_url` | String | 是 | | 受信任的 HTTPS 地址，例如 `https://my-hub.azure-devices.net`。允许指定端口和末尾斜杠。地址不能包含用户信息、端点路径、查询参数或片段。 |
| `device_id` | String | 是 | | 已注册设备的 ID，区分大小写，最长 128 个字符。 |
| `secret_key` | String | 是 | | 已注册设备的对称密钥，Base64 解码后须为 16–128 字节。通过密钥存储或环境变量注入。 |
| `format` | String | 否 | `JSON` | `JSON` 将整条记录编码为一个 UTF-8 JSON 对象。 `TEXT` 写入 UTF-8 字符串，`BINARY` 写入原始字节。格式名不区分大小写。 |
| `field` | String | `TEXT` 和 `BINARY` 必填 | | `TEXT` 使用 STRING 列，`BINARY` 使用 BYTES 列。列值不能为 null，允许空字符串或空字节数组。 |
| `application_properties` | Map&lt;String, String&gt; | 否 | `{}` | 可用于路由的公开消息属性。最多 32 个属性，名称长 1–64 个字符，只允许 ASCII 字母、数字、下划线、点和连字符。名称统一转为小写，忽略大小写后必须唯一。每个属性值最多 1,024 个字符，遵循 [HTTPS 消息属性字符集](https://learn.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-messages-construct)：ASCII 字母、数字和 <code>!#$%&amp;'*+-.^_&#96;&#124;~</code>。空格、斜杠和冒号会在联网前被拒绝。所有名称和值合计最多 4,096 字节。 |
| `sas_ttl_seconds` | Integer | 否 | `3600` | 每个 SAS 令牌的有效期，单位为秒，范围 30–86,400。 |
| `max_payload_bytes` | Integer | 否 | `262144` | 消息体的字节数上限，范围 1–262,144。实际可用大小还需在 IoT Hub 的 256 KiB 限额内为系统属性和应用属性预留空间。 |
| `connect_timeout_ms` | Integer | 否 | `10000` | 建立 TCP 连接的超时，单位为毫秒，范围 1–300,000。 |
| `read_timeout_ms` | Integer | 否 | `10000` | TLS 握手及响应读取的空闲超时，单位为毫秒，范围 1–300,000。 |
| `row_timeout_ms` | Integer | 否 | `30000` | 一条记录的 I/O 总时限，涵盖连接、TLS、写入、读取及重试等待。单位为毫秒，范围 1–300,000。 |
| `max_attempts` | Integer | 否 | `3` | 每条记录最多尝试的 HTTP 请求次数，包含首次请求，范围 1–10。 |

JSON 转换还会在分配对象树之前，按实际消息体上限限制转换工作量：每个逻辑值或容器出现一次计一个单位，另计字段名和字符串的 UTF-8 字节、Base64 消息体字节及数字表示工作量。共享值和被覆盖的 Map 条目会逐次计入，因此极大或反复归一化的输入即使最终 JSON 较小，也可能被拒绝。Schema 检查、转换和容器输出都采用迭代方式，不另设任意固定深度限制。转义后的实际输出字节另行精确校验。TEXT 和 BINARY 保留原有字节及类型校验。

## 投递行为

收到 HTTP 204 后才确认该记录投递成功。只有 HTTP 429 限流响应会触发重试。有效的 `Retry-After` 指定最短等待时间。响应头缺失或无法解析时使用有上限的退避时间。只有剩余的记录时限允许时才会重试。遇到重定向、其他 HTTP 响应或传输失败时，当前记录写入失败。应答丢失时，IoT Hub 可能已经接受该消息。

连接器没有需要写入检查点的待发送批次或事务。任务重启后重放记录可能产生重复消息。如需去重，可以在下游使用应用提供的唯一标识。

## 示例

以下作业使用已注册设备 `sensor-1` 发送 JSON 遥测。通过部署环境的密钥注入方式提供 `AZURE_IOT_DEVICE_KEY`。

```hocon
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 1
    schema {
      fields {
        reading = double
        unit = string
      }
    }
    rows = [{ kind = INSERT, fields = [21.5, "C"] }]
  }
}

sink {
  AzureIoTHub {
    hub_url = "https://my-hub.azure-devices.net"
    device_id = "sensor-1"
    secret_key = ${AZURE_IOT_DEVICE_KEY}
    application_properties = {
      measurement = "temperature"
    }
  }
}
```

<ChangeLog />
