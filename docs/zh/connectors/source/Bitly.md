import ChangeLog from '../changelog/connector-http-bitly.md';

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

# Bitly

安装 `connector-http-bitly` 后，使用 `Bitly` 作为 Source 标识。连接器通过 HTTPS 和 Bearer 令牌读取 Bitly v4 API，输出名为 `records` 的有界表，源端并行度为 1。[Bitly API 文档](https://dev.bitly.com/api-reference/)

## 资源

| 资源 | 输出 | 时间筛选 |
| --- | --- | --- |
| `bsds` | 自定义短域名响应对象 | 当前列表 |
| `campaigns`、`channels` | 每个活动或渠道一行 | `modified` |
| `organizations`、`groups` | 每个组织或组一行 | `modified` |
| `group_preferences` | 每个组的偏好设置对象 | 当前设置 |
| `organization_shorten_counts`、`group_shorten_counts` | 每个组织或组的缩短链接统计响应 | 以窗口结束时刻为基准的历史日统计 |
| `bitlinks` | 按组输出每条短链接，包含已归档链接 | `created_at` |
| `bitlink_clicks` | 按组输出每条短链接的点击统计响应 | 以窗口结束时刻为基准的历史日统计 |
| `qr_codes` | 与短链接关联的二维码 | `modified` |

每一行包含 `resource`、`record_id`、`organization_id`、`group_id`、`bitlink_id` 和 `payload`。各列均为字符串。没有对应的归属关系或 Bitly 未返回归属标识时，相关字段为 null。`payload` 保留供应商的完整 JSON 对象。可组合使用资源、适用的归属标识和记录 ID 来标识一行。

## 配置项

| 名称 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `token` | STRING | 是 | | Bitly API 令牌。 |
| `resources` | LIST of STRING | 是 | | 选择上表中的资源，各资源仅可出现一次。 |
| `start_date` | STRING | 是 | | 窗口的起始时刻，包含该时刻，使用带时区偏移的 RFC 3339。 |
| `end_date` | STRING | 是 | | 窗口的结束时刻，排除该时刻，使用带时区偏移的 RFC 3339。 |
| `qr_mode` | STRING | 否 | `stored` | 通过当前 v4 接口读取已保存的二维码。`legacy_bitlink` 使用按短链接发送一次 GET 的旧接口。 |
| `page_size` | INT | 否 | 50 | 短链接或已保存二维码的请求页大小。范围为 1 至 100。 |
| `max_pages_per_parent` | INT | 否 | 1000 | 单个上级资源下，每类分页资源允许的最大页数。范围为 1 至 5000。 |
| `max_requests` | INT | 否 | 50000 | 单次扫描允许的逻辑 GET 请求上限。重试或恢复未完成的请求时不会额外消耗一个逻辑位置。范围为 1 至 1000000。 |
| `max_attempts` | INT | 否 | 4 | 单次可重试 GET 的总尝试次数。范围为 1 至 10。 |
| `request_timeout_ms` | INT | 否 | 30000 | 单次数据获取的总超时，单位为毫秒，包含重试和成对获取的二维码详情与图像。范围为 100 至 300000。 |
| `max_response_bytes` | INT | 否 | 8388608 | 成功响应解码后的最大字节数。范围为 1024 至 67108864。 |
| `endpoint` | STRING | 否 | `https://api-ssl.bitly.com` | 自定义 HTTPS 源地址，不包含路径、查询参数、片段或凭据。必须与 `trust_certificate` 一起配置。 |
| `trust_certificate` | STRING | 否 | | 执行节点上的自定义源地址 PEM 信任证书。连接器继续验证主机名。 |

## 读取范围与恢复

连接器按照令牌允许访问的组织发现资源。组织和组的输出时间条件不会影响子资源发现。仅读取短链接时，请求使用秒级创建时间筛选，输出仍按配置的精确窗口判断。需要读取短链接的子资源时，先发现所有短链接，再仅对 `bitlinks` 输出应用创建时间窗口。统计请求显式使用 `unit=day` 和 `units=-1`，以向下取整到秒的 `end_date` 为基准。响应保留 Bitly 提供的历史统计分桶，包括记录窗口之前的分桶。

已保存二维码从组内发现，仅包含与短链接关联的二维码。连接器验证详情中的组和短链接标识，然后以 JSON 获取内嵌 SVG 图像。输出用 `metadata` 和 `image` 保存两个完整响应。旧版模式保留原响应格式，需要账户有权使用旧接口。连接器仅读取供应商响应，将 URL 和二维码图像作为数据保存，并且不会访问这些内容中的地址。

续页使用已经校验的 `search_after`，始终属于当前上级资源。续页 URL 的源地址、路径和其中提供的固定参数必须与当前请求相符。携带续页的空页会继续读取。重复页面或游标、归属矛盾、权限失败或读取完成前触及配置上限都会让作业失败。待处理的子资源请求最多保留 10000 个。检查点保存归属关系和游标。继续输出已读取一部分的响应前，连接器会校验响应指纹。Bitly API 无法隔离扫描期间账户内的数据变更，本连接器按窗口执行有界读取，统计值仍是聚合快照。

月度 API 额度耗尽时，连接器会直接报告错误。Bitly 平台限速响应明确给出该错误类型和正数 `Retry-After` 时，连接器才会重试。连接器禁止重定向。[Bitly 限速规则](https://dev.bitly.com/docs/getting-started/rate-limits/)

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Bitly {
    token = ${BITLY_TOKEN}
    resources = ["bitlinks", "bitlink_clicks", "qr_codes"]
    start_date = "2024-03-01T00:00:00Z"
    end_date = "2024-04-01T00:00:00Z"
  }
}
```

## 变更日志

<ChangeLog />
