import ChangeLog from '../changelog/connector-http-aviationstack.md';

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

# Aviationstack

安装 `connector-http-aviationstack` 后，使用 `Aviationstack` 作为 Source 标识。本连接器以并行度 1 在有界作业中读取所选航空目录，通过 HTTPS GET 将账户密钥放在供应商规定的查询参数中。每个成功读取的页面都会消耗请求额度，请按需选择目录。 [Aviationstack 认证与错误](https://apilayer.portal.swaggerhub.com/aviationstack/docs/getting-started)

## 目录

| 资源 | API 路径 | 内容 |
| --- | --- | --- |
| `airports` | `/v1/airports` | 机场及位置信息。 |
| `airlines` | `/v1/airlines` | 航空公司名称、代码及机队元数据。 |
| `airplanes` | `/v1/airplanes` | 飞机注册与交付信息。 |
| `aircraft_types` | `/v1/aircraft_types` | 机型和型号标识。 |
| `cities` | `/v1/cities` | 带航空业标识的城市。 |
| `countries` | `/v1/countries` | 国家和国家代码。 |
| `taxes` | `/v1/taxes` | 航空税费代码和名称。 |

## 输出

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `resource` | STRING | 目录名称。 |
| `record_id` | STRING | 供应商字段 `id` 的文本形式。跨目录时，可用目录名称和 ID 共同标识记录。 |
| `payload` | STRING | UTF-8 JSON 形式的完整供应商对象，保留空值、嵌套字段和精确数值。 |

## Source 参数

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `access_key` | `String` | — | 必填。Aviationstack 控制台中的 API 密钥。 |
| `resources` | `List<String>` | — | 必填。目录名称不可重复，按配置顺序读取。 |
| `start_date` | `String` | — | 飞机交付时间的 RFC 3339 起点，包含边界。 |
| `end_date` | `String` | — | 飞机交付时间的 RFC 3339 终点，包含边界。只配置起点时，默认取创建扫描分片时的时间。 |
| `page_size` | `Integer` | `100` | 单次请求的记录数。范围为 1 至 100。 |
| `max_pages_per_resource` | `Integer` | `10000` | 单个目录的分页上限。范围为 1 至 20,000。 |
| `max_attempts` | `Integer` | `4` | 可重试页面的总尝试次数。范围为 1 至 10。 |
| `request_timeout_ms` | `Integer` | `30000` | 每页请求的总时限，含读取响应体和重试等待。范围为 100 至 300,000 毫秒。 |
| `max_response_bytes` | `Integer` | `8388608` | 解压后响应体的字节上限。范围为 1,024 至 67,108,864。 |
| `endpoint` | `String` | `https://api.aviationstack.com` | 可选的 HTTPS 源站。使用根路径，并与 `trust_certificate` 一起配置。 |
| `trust_certificate` | `String` | — | 自定义源站使用的工作节点本地 PEM 信任证书。连接时仍校验主机名。 |

## 交付日期、分页与恢复

- 可选日期窗口仅适用于 `airplanes`，按 UTC 比较并包含起止边界。纯日期按 UTC 零点处理。支持带时区偏移的时间戳，包括使用空格分隔日期和时间，或偏移量中省略冒号的旧格式。配置日期窗口后会筛掉缺少交付日期的记录，无效的非空日期会使作业报错。未配置窗口时，原样保留日期字段。
- API 请求只携带访问密钥与分页参数。设置日期窗口后仍读取飞机目录的各页，包括筛选后没有结果的页面。交付日期表示历史事件，无法用于发现后续记录修改。
- 校验分页元数据后，按供应商返回的原始条数推进偏移。重复页、页内数量矛盾或总数改变时停止作业。供应商也可能在总数不变时调整结果顺序，因此偏移分页无法保证快照隔离。
- 检查点保存目录顺序、偏移、固定日期边界和已发出的位置。页内恢复会重新请求原页，在验证结果一致后输出后续记录。页内容或读取范围发生变化时，恢复会失败。请按下游要求处理可能重复的记录。
- 账户额度耗尽或套餐拒绝时，作业停止读取其余目录。明确的短时限流或临时故障可在请求期限内重试。成功请求超出套餐额度时，供应商可能收取超额费用。 [目录接口文档](https://apilayer.portal.swaggerhub.com/aviationstack/docs/api-documentation) [额度和计费说明](https://aviationstack.com/faq)

## 示例

以下配置读取机场目录和自 2024 年 1 月 1 日起至创建扫描分片时交付的飞机记录。

```hocon
env {
  job.mode = BATCH
  parallelism = 1
}
source {
  Aviationstack {
    resources = ["airports", "airplanes"]
    access_key = ${AVIATIONSTACK_ACCESS_KEY}
    start_date = "2024-01-01T00:00:00Z"
    plugin_output = "aviation_catalog"
  }
}
sink {
  Console {
    plugin_input = "aviation_catalog"
  }
}
```

<ChangeLog />
