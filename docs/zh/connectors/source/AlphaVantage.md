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

import ChangeLog from '../changelog/connector-http-alpha-vantage.md';

# AlphaVantage

AlphaVantage 从 [Alpha Vantage API](https://www.alphavantage.co/documentation/) 读取一个股票代码的历史价格和最新报价。使用批处理模式，源端并行度设为 1。每个所选资源通过单次有界查询获取，无分页或时间游标。

## 特性

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)

## 资源

| 资源 | API 函数 | 资源专用选项 |
| --- | --- | --- |
| `time_series_intraday` | `TIME_SERIES_INTRADAY` | `interval`、`adjusted`、`outputsize` |
| `time_series_daily` | `TIME_SERIES_DAILY` | `outputsize` |
| `time_series_daily_adjusted` | `TIME_SERIES_DAILY_ADJUSTED` | `outputsize` |
| `time_series_weekly` | `TIME_SERIES_WEEKLY` | — |
| `time_series_weekly_adjusted` | `TIME_SERIES_WEEKLY_ADJUSTED` | — |
| `time_series_monthly` | `TIME_SERIES_MONTHLY` | — |
| `time_series_monthly_adjusted` | `TIME_SERIES_MONTHLY_ADJUSTED` | — |
| `quote` | `GLOBAL_QUOTE` | — |

默认选择全部八种资源。请按 API 密钥可访问的接口和配额显式选择资源。日内、日线和调整日线的 `compact` 查询最多返回 100 条观测值。`full` 模式下，日线请求供应商可提供的历史数据，日内请求最近 30 天的窗口。实际可用的接口、历史范围和时效取决于 Alpha Vantage 授予的权限。周线、月线和报价按接口返回内容读取。

## 源端选项

| 名称 | 类型 | 是否必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `token` | String | 是 | — | Alpha Vantage API 密钥。 |
| `symbol` | String | 是 | — | 一个股票代码，按需加上交易所后缀。 |
| `resources` | 字符串列表 | 否 | 上表所列八种资源及顺序 | 按读取顺序选择资源，不能重复。 |
| `api_url` | String | 否 | `https://www.alphavantage.co` | 主机名为 `www.alphavantage.co` 的 HTTPS 源地址，可指定端口。 |
| `interval` | String | 否 | `1min` | 日内时间间隔，取 `1min`、`5min`、`15min`、`30min` 或 `60min`。 |
| `adjusted` | Boolean | 否 | `false` | 请求经过股票拆分和股息调整的日内数据。 |
| `outputsize` | String | 否 | `compact` | 日内、日线和调整日线的窗口，取 `compact` 或 `full`。 |
| `max_records` | Integer | 否 | `50000` | 单个资源响应的观测值数量上限，范围为 1 到 100000。 |
| `max_response_bytes` | Integer | 否 | `8388608` | 压缩和解压后响应体各自的字节上限，范围为 1024 到 16777216。 |
| `connect_timeout_ms` | Integer | 否 | `10000` | TCP 连接超时，范围为 1 到 300000。 |
| `read_timeout_ms` | Integer | 否 | `10000` | TLS 和响应读取的空闲超时，范围为 1 到 300000。 |
| `request_timeout_ms` | Integer | 否 | `30000` | 每个资源的请求总时限，包含限速及重试等待，范围为 1 到 300000。 |
| `request_interval_ms` | Integer | 否 | `2000` | 每个读取器两次请求的最小间隔，范围为 1 到 60000。 |
| `max_attempts` | Integer | 否 | `3` | 单个资源的请求次数上限，包含第一次，范围为 1 到 10。 |
| `max_requests` | Integer | 否 | `24` | 单个读取器执行期间所有资源的请求次数上限，包含首次和重试，范围为 1 到 80。 |

## 输出和恢复

固定输出包含六个字符串字段。

| 字段 | 值 |
| --- | --- |
| `resource` | 所选资源名称。 |
| `symbol` | 股票代码。 |
| `date` | `yyyy-MM-dd` 格式的交易日。报价行使用供应商返回的最近交易日。 |
| `timestamp` | 日内数据保留 `yyyy-MM-dd HH:mm:ss` 格式的原始市场本地时间，其他资源为 null。 |
| `time_zone` | 价格序列响应有时区时保留供应商的时区字符串，报价为 null。 |
| `data` | 单条价格观测值或报价的 JSON 文本，保留供应商字段名、数值字符串、精确 JSON 数值及 null。 |

连接器完整校验每个资源的响应，再按时间升序输出。合法的空价格序列或空报价不输出任何行。无效响应、认证失败、权限不足、供应商报告每日额度耗尽、重试或请求预算耗尽都会使作业失败。TLS 使用 JVM 信任库，并拒绝重定向。源端错误和检查点状态不会包含 API 密钥。

首次请求和每次重试都计入读取器的本地请求预算。恢复时新的读取器会重新计算本地预算。供应商仍按同一密钥内的所有恢复和作业累计使用量。请按可用配额选择资源并设置 `request_interval_ms` 和 `max_requests`。[Alpha Vantage 支持说明](https://www.alphavantage.co/support/)

恢复时跳过检查点中已完成的资源。如果某个资源只输出了部分行，源端重新查询该资源，并在验证内容后从已保存的行位置恢复。资源快照变化时恢复失败。检查点时尚未开始输出的资源会重新查询。

## 示例

把 API 密钥作为 SeaTunnel 作业变量传入。

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  AlphaVantage {
    token = "${ALPHA_VANTAGE_API_KEY}"
    symbol = "IBM"
    resources = ["time_series_daily", "quote"]
    outputsize = "compact"
  }
}

sink {
  Console {}
}
```

## 变更日志

<ChangeLog />
