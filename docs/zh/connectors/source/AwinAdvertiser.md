import ChangeLog from '../changelog/connector-http-awin-advertiser.md';

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

# Awin Advertiser

安装 `connector-http-awin-advertiser` 后，在并行度为 1 的有界作业中使用 `AwinAdvertiser`。请提供有权读取广告主账户的个人或经典 API 令牌。连接器通过 HTTPS 调用经典广告主 API，并在请求头使用 Bearer 认证。 [Awin API 认证](https://help.awin.com/apis/apidocs/api-authentication)

## 资源

`/reports`、`/transactions` 和 `/publishers` 路径使用 `/advertisers/{advertiserId}` 前缀。

| 资源 | GET 路径 | 内容 |
| --- | --- | --- |
| `campaign_performance` | `/reports/campaign` | 按日期、活动和发布商汇总。每次请求最多包含 400 个日期。 |
| `transactions` | `/transactions/` | 交易及可用的购物篮商品。每次请求最多包含 31 个 UTC 日期。 |
| `publisher_performance` | `/reports/publisher` | 按 UTC 逐日读取发布商报告。 |
| `creative_performance` | `/reports/creative` | 按 UTC 日期和所选 Awin 区域分别读取素材报告。 |
| `publishers` | `/publishers` | 与指定广告主有有效合作关系的发布商。 |
| `accounts` | `/accounts?type=advertiser` | 令牌用户有权查看的广告主账户。 |

## 输出

六个字段均为 STRING，输出为 INSERT 记录。

| 字段 | 含义 |
| --- | --- |
| `resource` | 所选资源。 |
| `record_key` | 交易、发布商和账户使用供应商 ID。报告使用由标识维度组成的 JSON 数组。 |
| `advertiser_id` | 配置的项目 ID，或账户结果中的账户 ID。 |
| `region` | 供应商返回的发布商报告区域，或素材报告的查询区域。 |
| `report_date` | UTC 报告日期或交易日期。无日期的资源为 null。 |
| `payload` | 完整的供应商 JSON，包含嵌套字段和精确数值。发布商及素材报告补充日期，素材报告还补充区域。交易的佣金状态为 deleted 时，补充的布尔值 `is_deleted` 为真。 |

活动报告的标识维度依次为日期、发布商、活动。发布商报告依次为日期、发布商、区域、币种。素材报告依次为日期、区域、素材、发布商、币种。

## Source 参数

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `token` | `String` | — | 必填。Awin 个人访问令牌或经典 API 令牌。 |
| `resources` | `List<String>` | — | 必填。资源名称不可重复，按配置顺序读取。 |
| `advertiser_id` | `Long` | — | 正整数形式的广告主项目 ID。选择广告主范围的资源时必填。 |
| `start_date` | `String` | — | 包含边界的 UTC 起始日期，格式 YYYY-MM-DD。读取报告和交易时必填。 |
| `end_date` | `String` | 当前 UTC 日期 | 包含边界的 UTC 结束日期，格式 YYYY-MM-DD，最晚为当前日期。读取当天交易时，以启动时截取的 UTC 秒为止。 |
| `regions` | `List<String>` | `[]` | 读取素材报告时必填。选择不重复的 Awin 代码，支持 AT、AU、BE、BR、BU、CA、CH、DE、DK、ES、FI、FR、GB、IE、IT、NL、NO、PL、SE 和 US。 |
| `resume_from` | `Map<String, String>` | `{}` | 各已选资源此前读取到的 UTC 日期。素材报告使用类似 `creative_performance:GB` 的键。 |
| `lookback_days` | `Integer` | `0` | 每个已提供的恢复日期回读的天数，以 start_date 为下限。范围为 0 至 4000。 |
| `campaign_window_days` | `Integer` | `400` | 每次活动报告请求最多包含的日期数。范围为 1 至 400。 |
| `transaction_window_days` | `Integer` | `31` | 每次交易请求最多包含的 UTC 日期数。范围为 1 至 31。 |
| `requests_per_minute` | `Integer` | `20` | 每个读取器在滚动一分钟内的 HTTP 尝试上限。范围为 1 至 20。 |
| `max_requests` | `Integer` | `10000` | 每次扫描最多包含的报告或元数据请求数，重试单独计数。范围为 1 至 100,000。 |
| `max_attempts` | `Integer` | `4` | 可重试请求的总尝试次数。范围为 1 至 10。 |
| `request_timeout_ms` | `Integer` | `90000` | 单个请求的总时限，包括本地限速、重试与响应解码。范围为 100 至 300,000 毫秒。 |
| `max_response_bytes` | `Integer` | `8388608` | 单次响应解压后的字节上限。范围为 1,024 至 67,108,864。 |
| `endpoint` | `String` | `https://api.awin.com` | 可选的自定义 HTTPS 源站，需同时配置证书。请使用可信源站以保护令牌。 |
| `trust_certificate` | `String` | — | 自定义源站使用的工作节点本地 PEM 证书。连接继续验证主机名。 |

## 日期窗口、用量与恢复

- 创建分片时固定 UTC 结束点。所有日期范围包含边界。完整交易日的请求结束于 UTC 23:59:59，下一请求从次日零点开始。发布商和素材报告每次只读取一天，保证汇总数据归属明确的日期。
- 已持久化各资源的同步日期时，可以在新作业中提供 `resume_from`。每个已选日期资源和素材区域只在创建读取计划时应用一次回读。没有恢复日期的区域从 `start_date` 开始。引擎从检查点恢复时沿用已保存的窗口和行偏移。
- 页内恢复会重新请求报告并核对响应摘要。供应商修改响应或者配置改变读取范围时，恢复会失败。供应商可能在不同请求之间更新历史记录。下游可按资源、广告主和记录键实现更新或去重，以吸收后续重叠窗口中的变更。交易的删除标志表示 API 为所查日期范围返回的状态。
- 经典 API 的额度按 Awin 用户计量，同一用户的其他 API 客户端也会消耗额度。读取器对自身尝试计数，并遵守供应商的重试等待时长。部分响应或意外的续页信息会使作业停止。经典接口未提供分页和快照隔离，连接器无法发现供应商未发出信号的省略结果。 [经典 API 限制与分页](https://help.awin.com/advertisers/docs/en/comparison-between-shareasale-and-awin-apis)

## 示例

以下作业读取交易和两个区域的素材报告。GB 回读恢复日期之前的两天，US 从配置的起始日期开始。

```hocon
env {
  job.mode = BATCH
  parallelism = 1
}
source {
  AwinAdvertiser {
    token = ${AWIN_TOKEN}
    advertiser_id = 123456
    resources = ["transactions", "creative_performance"]
    start_date = "2024-01-01"
    end_date = "2024-01-07"
    regions = ["GB", "US"]
    resume_from = { "creative_performance:GB" = "2024-01-05" }
    lookback_days = 2
    plugin_output = "awin_rows"
  }
}
sink {
  Console {
    plugin_input = "awin_rows"
  }
}
```

<ChangeLog />
