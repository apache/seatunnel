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

import ChangeLog from '../changelog/connector-http-apple-search-ads.md';

# Apple Ads

安装 `connector-http-apple-search-ads`，在批处理任务中使用 `AppleAds`。使用能够读取相应 App Store 广告账户的 Apple Ads API 用户客户端 ID，以及在外部签发的 ES256 客户端密钥 JWT。[Apple Ads Platform 访问权限](https://developer.apple.com/documentation/apple-ads-platform-api/access-overview) · [OAuth 令牌交换](https://developer.apple.com/documentation/apple-ads-platform-api/implementing-oauth-for-the-apple-ads-platform-api)

## API 模式

| `api_mode` | 账户参数 | API 源站 |
| --- | --- | --- |
| `PLATFORM_V1`（默认） | `ad_account_id` | `https://api.ads.apple.com` |
| `LEGACY_V5` | `org_id` | `https://api.searchads.apple.com` |

Apple 宣布 Campaign Management v5 将于 2027 年 1 月 26 日停用。迁移时使用 Platform 广告账户 ID。该 ID 与旧版组织 ID 含义不同，检查点只能用于原模式和账户。连接器调用 Apple 已公开的读取接口。[Campaign Management API 停用说明](https://developer.apple.com/documentation/apple_ads/reports)

## 资源

| 资源 | 数据 |
| --- | --- |
| `campaigns` | App Store 广告系列 |
| `adgroups` | 各广告系列的广告组 |
| `keywords` | 各广告系列和广告组的定位关键词 |
| `ads` | 各广告系列和广告组的广告 |
| `campaigns_report_daily` | 广告系列每日指标 |
| `adgroups_report_daily` | 广告组每日指标 |
| `keywords_report_daily` | 定位关键词每日指标 |
| `ads_report_daily` | 广告每日指标 |

报表按国家或地区分组。Platform 单日报表使用 Apple 返回的当日汇总，若该汇总没有地理信息，国家或地区字段为空。多日报表按返回的日期和地区输出每日记录。输出不包含账户级报表总计。广告系列与广告组的发现不受报表日期限制。权限错误或供应商错误会使源任务失败。

## 参数

| 参数 | 类型 | 默认值 | 用途 |
| --- | --- | --- | --- |
| `api_mode` | 字符串 | `PLATFORM_V1` | API 版本和账户命名空间 |
| `ad_account_id` | 字符串 | 无 | Platform 模式必填的广告账户正整数 ID |
| `org_id` | 字符串 | 无 | 旧版模式必填的组织正整数 ID |
| `client_id` | 字符串 | 必填 | Apple Ads API 用户客户端 ID |
| `client_secret` | 字符串 | 必填 | 在外部签发的 ES256 客户端密钥 JWT |
| `resources` | 字符串列表 | 全部八项 | 按指定顺序读取的资源名 |
| `start_date` | 字符串 | 无 | 选择日报资源时必填，包含 `YYYY-MM-DD` 起始日期 |
| `end_date` | 字符串 | 所选日历的昨天 | 包含 `YYYY-MM-DD` 结束日期 |
| `report_cursor_date` | 字符串 | 无 | 新建批次时由调用方提供的已处理报表日期 |
| `lookback_days` | 整数 | 30 | 从报表游标向前重读的天数，范围 0 至 30 |
| `timezone` | 字符串 | `UTC` | Apple 报表使用 `UTC` 或 `ORTZ` |
| `organization_timezone` | 字符串 | 无 | `ORTZ` 必填，填写与 Apple 组织日历一致的 IANA 时区 |
| `report_window_days` | 整数 | 7 | 每次报表请求包含的最大天数，范围 1 至 90 |
| `page_size` | 整数 | 1000 | 每页最多读取的供应商实体数，范围 1 至 1000 |
| `max_pages_per_partition` | 整数 | 10000 | 每个父级关系或报表窗口的最大页数 |
| `request_timeout_ms` | 整数 | 30000 | 单次 HTTPS 尝试的总超时 |
| `operation_timeout_ms` | 整数 | 120000 | 整个操作的超时，包含令牌获取、请求间隔及重试 |
| `min_request_interval_ms` | 整数 | 100 | HTTPS 尝试之间的最短时间 |
| `max_retries` | 整数 | 3 | 符合重试条件的响应或读取异常可追加的尝试次数 |
| `max_response_bytes` | 整数 | 16777216 | 分别限制传输字节和解压后的 JSON 字节 |
| `api_origin` | 字符串 | 当前模式的源站 | 可选的 Apple Ads 请求 HTTPS 源站 |
| `oauth_origin` | 字符串 | `https://appleid.apple.com` | Apple 令牌交换 HTTPS 源站 |

日报查询范围应在所选日历的最近 90 天内。连接器取 `start_date` 和 `report_cursor_date` 回退 `lookback_days` 后的日期中较晚者。未提供游标的新批次从 `start_date` 开始。恢复时使用原先固定的日期和日历。请使用已经结束的自然日。[Platform 日期范围](https://developer.apple.com/documentation/apple-ads-platform-api/timerange) · [旧版日报范围](https://developer.apple.com/documentation/apple_ads/reportingrequest)

## 输出与恢复

源输出字符串列 `resource`、`api_mode`、`account_id`、`entity_id`、`campaign_id`、`ad_group_id`、`report_date`、`country_or_region` 和 `record`。不适用的字段为空。实体的 `record` 保留 Apple JSON 字段，报表的 `record` 包含 Apple 报表 `metadata` 和当前行的 `metrics`。金额保持供应商原始表示和精度。

一个活跃读取器读取父级列表和各报表页。检查点保存模式、账户、固定日期窗口、当前分页及已接收的日报行位置。恢复前会核对保存的分页偏移量和内容指纹，若分页内容发生改变，任务会以恢复错误终止。Apple 可能在读取过程中更新广告或归因数据。端到端写入结果取决于 Sink 的检查点能力。

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  AppleAds {
    ad_account_id = "123456789"
    client_id = "<apple-api-user-client-id>"
    client_secret = ${APPLE_ADS_CLIENT_SECRET}
    resources = ["campaigns", "adgroups", "keywords", "ads"]
  }
}

sink {
  Console {}
}
```

通过任务配置的密钥注入机制提供 `client_secret`。引擎需要信任两个源站的 HTTPS 证书。

<ChangeLog />
