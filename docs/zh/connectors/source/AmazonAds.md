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

import ChangeLog from '../changelog/connector-http-amazon-ads.md';

# Amazon Ads

安装 `connector-http-amazon-ads`，在批处理任务中配置 `AmazonAds`。通过获授 Amazon Ads API 权限的集成获取 Login with Amazon 客户端 ID、客户端密钥和刷新令牌。连接器调用按 Profile 授权的各产品 API 和 Reporting v3。 [Amazon Ads API 集合](https://github.com/amzn/ads-advanced-tools-docs/tree/main/postman) · [授权流程](https://advertising.amazon.com/API/docs/en-us/guides/account-management/authorization/authorization-grants)

## 直接资源

| 资源 | API 操作 |
| --- | --- |
| `profiles` | GET `/v2/profiles` |
| `portfolios` | POST `/portfolios/list` |
| `sponsored_display_campaigns` | GET `/sd/campaigns` |
| `sponsored_display_ad_groups` | GET `/sd/adGroups` |
| `sponsored_display_product_ads` | GET `/sd/productAds` |
| `sponsored_display_targetings` | GET `/sd/targets` |
| `sponsored_display_creatives` | GET `/sd/creatives` |
| `sponsored_display_budget_rules` | GET `/sp/budgetRules` |
| `sponsored_brands_keywords` | GET `/sb/keywords` |
| `sponsored_brands_campaigns` | POST `/sb/v4/campaigns/list` |
| `sponsored_brands_ad_groups` | POST `/sb/v4/adGroups/list` |
| `sponsored_brands_ads` | POST `/sb/v4/ads/list` |
| `sponsored_product_campaigns` | POST `/sp/campaigns/list` |
| `sponsored_product_ad_groups` | POST `/sp/adGroups/list` |
| `sponsored_product_keywords` | POST `/sp/keywords/list` |
| `sponsored_product_negative_keywords` | POST `/sp/negativeKeywords/list` |
| `sponsored_product_campaign_negative_keywords` | POST `/sp/campaignNegativeKeywords/list` |
| `sponsored_product_ads` | POST `/sp/productAds/list` |
| `sponsored_product_targetings` | POST `/sp/targets/list` |
| `sponsored_product_ad_group_bid_recommendations` | POST `/sp/targets/bid/recommendations` |
| `sponsored_product_ad_group_suggested_keywords` | POST `/sp/targets/keywords/recommendations` |
| `attribution_report_products` | POST `/attribution/report` |
| `attribution_report_performance_adgroup` | POST `/attribution/report` |
| `attribution_report_performance_campaign` | POST `/attribution/report` |
| `attribution_report_performance_creative` | POST `/attribution/report` |

Profile 列表可输出可访问的 seller 和 vendor Profile。`profiles` 和 `marketplace_ids` 取并集，决定读取哪些 Profile 的子资源。只选择建议资源也会查找 Sponsored Products 广告组。`sponsored_display_budget_rules` 使用 Sponsored Products 预算规则接口。请选择 Profile 有权访问的资源。权限不足或产品不可用时接口可能报错，任务会失败。

## 异步报表

| 汇总资源 | 日报资源 | 报表类型 |
| --- | --- | --- |
| `sponsored_brands_v3_report_stream` | `sponsored_brands_v3_report_stream_daily` | `sbPurchasedProduct` |
| `sponsored_brands_campaigns_report_stream` | `sponsored_brands_campaigns_report_stream_daily` | `sbCampaigns` |
| `sponsored_brands_adgroups_report_stream` | `sponsored_brands_adgroups_report_stream_daily` | `sbAdGroup` |
| `sponsored_brands_ads_report_stream` | `sponsored_brands_ads_report_stream_daily` | `sbAds` |
| `sponsored_display_campaigns_report_stream` | `sponsored_display_campaigns_report_stream_daily` | `sdCampaigns` |
| `sponsored_display_adgroups_report_stream` | `sponsored_display_adgroups_report_stream_daily` | `sdAdGroup` |
| `sponsored_display_productads_report_stream` | `sponsored_display_productads_report_stream_daily` | `sdAdvertisedProduct` |
| `sponsored_display_targets_report_stream` | `sponsored_display_targets_report_stream_daily` | `sdTargeting` |
| `sponsored_display_asins_report_stream` | `sponsored_display_asins_report_stream_daily` | `sdPurchasedProduct` |
| `sponsored_products_campaigns_report_stream` | `sponsored_products_campaigns_report_stream_daily` | `spCampaigns` |
| `sponsored_products_adgroups_report_stream` | `sponsored_products_adgroups_report_stream_daily` | `spCampaigns` |
| `sponsored_products_keywords_report_stream` | `sponsored_products_keywords_report_stream_daily` | `spTargeting` |
| `sponsored_products_targets_report_stream` | `sponsored_products_targets_report_stream_daily` | `spTargeting` |
| `sponsored_products_search_terms_report_stream` | `sponsored_products_search_terms_report_stream_daily` | `spSearchTerm` |
| `sponsored_products_productads_report_stream` | `sponsored_products_productads_report_stream_daily` | `spAdvertisedProduct` |
| `sponsored_products_asins_keywords_report_stream` | `sponsored_products_asins_keywords_report_stream_daily` | `spPurchasedProduct` |
| `sponsored_products_asins_targets_report_stream` | `sponsored_products_asins_targets_report_stream_daily` | `spPurchasedProduct` |

## 参数

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `client_id` | 字符串 | 必填 | Login with Amazon 客户端 ID。 |
| `client_secret` | 字符串 | 必填 | Login with Amazon 客户端密钥。 |
| `refresh_token` | 字符串 | 必填 | 获授 Amazon Ads 访问权的刷新令牌。 |
| `region` | 字符串 | `NA` | 广告 API 区域，可选 NA、EU、FE。 |
| `base_url` | 字符串 | 自动确定 | 所选区域广告 API 的可选 HTTPS 源地址覆盖。 |
| `oauth_base_url` | 字符串 | 自动确定 | 令牌服务的可选 HTTPS 源地址覆盖。 |
| `download_hosts` | 字符串列表 | `[]` | 允许提供预签名下载的额外 HTTPS 主机名。 |
| `resources` | 字符串列表 | `[]` | 要读取的资源名称，不可重复。空数组时读取全部。 |
| `profiles` | 字符串列表 | `[]` | 子资源的 Profile ID，与 marketplace_ids 取并集。 |
| `marketplace_ids` | 字符串列表 | `[]` | 子资源的 Marketplace ID，与 profiles 取并集。 |
| `state_filter` | 字符串列表 | `[]` | Sponsored Products 和 Sponsored Brands 广告活动状态。 |
| `start_date` | 字符串 | 自动确定 | 可选报表开始日，包含当天，格式 YYYY-MM-DD。 |
| `end_date` | 字符串 | 自动确定 | 可选报表结束日，包含当天，格式 YYYY-MM-DD。 |
| `look_back_window` | 整数 | `3` | 默认滚动日期区间额外回读的日数，0–30。 |
| `report_window_days` | 整数 | `30` | 每份异步报表最大天数，1–30。 |
| `page_size` | 整数 | `100` | 可配置分页接口的每页大小，1–100。 |
| `max_pages` | 整数 | `1000` | 每个 Profile 和资源最多页数，1–10000。 |
| `max_profiles` | 整数 | `1000` | Profile 列表最大条数，1–10000。 |
| `max_attempts` | 整数 | `4` | 每次可重试 API 操作的最大尝试次数，1–8。 |
| `request_interval_ms` | 整数 | `100` | 请求间隔下限，10–60000 毫秒。 |
| `io_timeout_ms` | 整数 | `15000` | 连接及读取空闲超时，100–120000 毫秒。 |
| `request_deadline_ms` | 整数 | `60000` | API 请求含重试的总预算，500–900000 毫秒。 |
| `poll_interval_ms` | 整数 | `10000` | 报表状态查询间隔，100–60000 毫秒。 |
| `report_timeout_ms` | 整数 | `10800000` | 报表处理总超时，1000–14400000 毫秒。 |
| `duplicate_wait_ms` | 整数 | `60000` | 无法复用重复报表时的重试间隔，100–120000 毫秒。 |
| `download_deadline_ms` | 整数 | `300000` | 每次下载及解码的总耗时，500–900000 毫秒。 |
| `max_response_bytes` | 整数 | `8388608` | 解码后 API 响应及单条报表记录的上限，1024–33554432 字节。 |
| `max_download_bytes` | 整数 | `67108864` | 压缩报表的上限，1024–268435456 字节。 |
| `max_report_bytes` | 整数 | `268435456` | 解压后报表及临时文件的上限，1024–1073741824 字节。 |
| `max_report_rows` | 整数 | `1000000` | 每份报表最大条数，1–1000000。 |

## 日期、下载与恢复

NA、EU、FE 区域分别使用 `advertising-api.amazon.com`、`advertising-api-eu.amazon.com`、`advertising-api-fe.amazon.com`。Login with Amazon 默认使用 `api.amazon.com/auth/o2/token`。报表通过经过验证的 HTTPS 从 Amazon S3 或显式配置的主机下载。下载请求只带预签名路径，无广告或 OAuth 凭据。遇到重定向时报错。 [Login with Amazon 令牌交换](https://developer.amazon.com/docs/login-with-amazon/authorization-code-grant.html) · [Reporting v3](https://d1y2lf8k3vrkfu.cloudfront.net/openapi/en-us/dest/OfflineReport_prod_3p.json)

异步报表使用包含起止日的 UTC 日期。未填写日期时，截止日为任务启动时的 UTC 当天，开始日从昨天再回读配置天数。早于 60 天的起点截到窗口内。归因报表使用 Profile 时区，历史限制同为 60 天，默认只读取该时区的冻结当天。账户和报表规则可能进一步限制范围。请求区间完全超出可用日期时任务失败。

输出字段 `resource`、`id`、`profile_id`、可空的 `parent_id`、`window_start`、`window_end` 和 `record` 都使用字符串。ID 使用供应商标识或确定性的记录标识。异步报表 ID 绑定资源、Profile、请求窗口和行位置。`record` 保存供应商 JSON。异步报表记录补充 `profileId` 与表示分窗截止日的 `reportDate`。日报保留每行的 `date`。建议记录补充广告组和广告活动 ID。

HTTP 425 可指向已请求的报表。连接器核对权限范围、日期、完整配置和状态后才复用。创建响应丢失时供应商可能已创建报表，后续请求可通过 425 查找。连接器分别限制压缩和解压字节，验证报表后将记录暂存到私有临时文件，再输出记录。处理完成或取消时释放临时文件。

检查点保存冻结的区间、Profile 和资源位置、报表标识、请求校验值以及部分输出页或文件的校验值。使用相同提取参数恢复时，会重新认证并获取新的下载地址。新任务从配置的日期开始，API 集合可能在请求之间变化。

## 示例

```hocon
env {
  job.mode = "BATCH"
}

source {
  AmazonAds {
    client_id = "${amazon_ads_client_id}"
    client_secret = "${amazon_ads_client_secret}"
    refresh_token = "${amazon_ads_refresh_token}"
    region = "NA"
    resources = ["profiles", "sponsored_product_campaigns", "sponsored_products_campaigns_report_stream_daily"]
  }
}

sink {
  Console {}
}
```

<ChangeLog />
