import ChangeLog from '../changelog/connector-http-alpaca-broker.md';

# AlpacaBroker

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

AlpacaBroker 通过只读 HTTPS GET 读取美国 Broker API 的七个资源。每个 Source 的读取范围有限，并行度为 1。

| 资源 | 记录 ID | 输出 |
| --- | --- | --- |
| assets | 资产 ID | 资产对象。`asset_class` 可选 `us_equity`、`crypto`、`us_index` 或 `crypto_perp`。 |
| accounts_activities | 活动 ID | 升序分页的活动。 |
| accounts | 账户 UUID | 创建时间在包含首尾的窗口内的账户。 |
| account_documents | 文档 ID | 所有已发现账户的文档元数据。 |
| calendar | 交易日 | 按交易日或结算日读取。 |
| clock | 市场时间戳 | 一行市场时钟数据。 |
| country-info | 国家或地区代码 | 三字母字典键对应的记录。 |

每行包含四个字符串字段。`resource` 和 `record_id` 标识记录，文档行填充 `account_id`。`payload` 保留 JSON 对象、空值和小数精度，API 返回的金额字符串仍为字符串。

## 鉴权与传输

将 Broker API Key ID 和 Secret 分别填入 `username`、`password`。沙箱使用 `https://broker-api.sandbox.alpaca.markets`，`LIVE` 使用 `https://broker-api.alpaca.markets`。自定义沙箱需要 HTTPS 及 Worker 本地的一份 PEM 证书。连接器校验主机名并拒绝重定向。

重试共享请求总预算，并遵循有效的 `Retry-After` 或 HTTP 429 的 `X-RateLimit-Reset`。如果等待超过预算，请求失败。响应大小按解压后字节数限制。错误信息避免包含凭据或供应商返回内容。

## 时间窗与恢复

活动使用 UTC 整秒精度，按创建时间筛选且不含首尾。活动日期可能与创建时间不同。使用上一页最后一条记录 ID 续页，空页结束。

账户窗口包含边界，保留亚秒精度。API 最多返回 1,000 条。满页时将最后一个创建时间对应的整组记录留给下一次包含该时间的查询。若整页都属于一个时间戳，API 无法确定该时间组是否已完整返回，连接器会报错。

账户、活动和文档父账户发现的上界固定在创建 split 时。日历与文档范围包含首尾日期，若未设置 `end_date`，使用创建 split 当天的 UTC 日期。文档日期只筛选子文档。连接器读取文档元数据。

检查点保存固定上界、下次游标、待遍历的账户和结束状态。恢复要求资源、地址、Key ID、过滤参数及活动页大小一致。供应商并发更新可能改变扫描结果。最终投递语义取决于 Sink。

## 配置项

| 配置项 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `resource` | string | — | 待读取的资源。 |
| `environment` | string | `SANDBOX` | 美国 Broker API 环境，SANDBOX 或 LIVE。 |
| `username` | string | — | HTTP Basic Key ID。 |
| `password` | string | — | HTTP Basic Secret。 |
| `base_url` | string | — | 可选的沙箱 HTTPS 地址。 |
| `trust_certificate` | string | — | Worker 本地的自定义沙箱 PEM 信任证书。 |
| `start_time` | string | — | 账户或活动的 RFC 3339 起始时间。 |
| `end_time` | string | — | 账户或活动的 RFC 3339 结束时间。 |
| `start_date` | string | — | 文档或日历的起始日，含该日。 |
| `end_date` | string | — | 文档或日历的结束日，含该日。 |
| `calendar_date_type` | string | `trading` | 按交易日 trading 或结算日 settlement 筛选。 |
| `asset_class` | string | — | 可选的 Broker API 资产类别。 |
| `page_size` | int | `100` | 活动每页最大条数，1 到 100。 |
| `max_requests` | int | `10000` | 每个 split 的逻辑请求上限。 |
| `request_timeout_ms` | int | `30000` | 一次请求的总预算，含重试与解码。 |
| `max_attempts` | int | `3` | 可重试读取的最大尝试次数。 |
| `max_response_bytes` | int | `8388608` | 响应解压后的最大字节数。 |

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  AlpacaBroker {
    resource = "accounts_activities"
    environment = "SANDBOX"
    username = ${ALPACA_BROKER_KEY}
    password = ${ALPACA_BROKER_SECRET}
    start_time = "2024-02-29T00:00:00Z"
    end_time = "2024-03-01T00:00:00Z"
  }
}
sink {
  Console {}
}
```

<ChangeLog />
