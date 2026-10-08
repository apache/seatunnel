import ChangeLog from '../changelog/connector-tiktok-ads.md';

# TikTokAds

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

## 描述

通过 `GET /open_api/v1.3/report/integrated/get/` 读取单个广告主的 TikTok API for Business
同步综合报表。仅支持 `BASIC` 报表、`AUCTION` 服务、`AUCTION_AD` 数据层级和普通分页。
不支持原始用户或事件导出、GMV Max、异步报表、多广告主或自定义筛选。

## 主要特性

- [x] 批处理
- [ ] 流处理
- [ ] 精确一次

源并行度必须为 1。恢复时从第 1 页重新读取，不恢复页偏移量。
远端报表**不是不可变快照**。总数和重复维度检查只能检测部分分页变化，不能保证快照一致性
或数据完整性；指标或记录可能在请求之间发生未被检测到的变化。失败前可能已有记录写入
非事务型下游，重放可能产生重复或不同的记录。请根据业务采用暂存和对账方案。

## 参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| token | string | 是 | - | 具有广告主报表权限的 Access-Token，仅通过请求头发送。原生键名 token 可由 SeaTunnel 日志脱敏。 |
| advertiser_id | string | 是 | - | 单个数字广告主 ID，不是 Business Center ID。 |
| data_level | string | 是 | - | 必须为 AUCTION_AD。 |
| start_date | string | 是 | - | 广告主账号时区中的 yyyy-MM-dd，包含起始日期。 |
| end_date | string | 是 | - | 包含结束日期。包含 stat_time_day 时最多 30 个自然日，否则最多 365 日。 |
| dimensions | list | 是 | - | 仅支持 ["ad_id"] 或 ["ad_id", "stat_time_day"]。 |
| metrics | list | 是 | - | spend、impressions、clicks 的非空且不重复的子集。 |
| schema | config | 是 | - | 字段必须与请求的维度和指标完全对应，输出顺序可自行指定。 |
| page_size | int | 否 | 1000 | 每页请求行数，1-1000。 |
| max_report_rows | int | 否 | 100000 | 总行数上限，1-1000000；超过时失败而非截断，不能绕过 API 广告 ID 上限。 |
| max_response_bytes | int | 否 | 4194304 | 单次响应字节上限，1024-16777216。 |
| request_timeout_ms | int | 否 | 30000 | 包含响应体读取的 HTTP 截止时间，100-120000 毫秒。 |
| report_timeout_ms | int | 否 | 600000 | 报表截止时间，100-3600000 毫秒；在记录、请求之间和重试等待中检查。 |
| max_retries | int | 否 | 3 | 每页额外尝试次数，0-5；仅对 HTTP 429、500、502、503、504 重试。 |
| max_retry_wait_ms | int | 否 | 30000 | 最大重试等待，1000-120000 毫秒；Retry-After 超出时失败，不提前重试。 |
| mock_url | string | 否 | - | 仅测试使用的 HTTP origin，要求 token 为字面值 mock-token；禁止真实令牌。不是官方模拟器。 |
| common-options | | 否 | - | 支持 parallelism=1 和 plugin_output。 |

### 类型和报表语义

- `ad_id` 使用 STRING。对 Manual/Smart+ Ads 表示广告，对 Upgraded Smart+ Ads 表示
  **创意**，本版本不支持 `ad_id_v2`。
- `stat_time_day` 使用 STRING，保留 `yyyy-MM-dd 00:00:00` 的广告主本地日期标签，
  不转换成假设的 UTC 时间戳。日期必须位于请求范围内。
- `spend` 使用 DECIMAL(p,s)，精度不超过 38，按广告主账号货币单位表示，
  不进行分/元换算或汇率转换。超出精度或需要舍入时失败。
- `impressions` 和 `clicks` 使用 BIGINT，非法值、小数或溢出时失败。
- 请求字段必须为非空字符串。缺失、null、格式错误或不可精确表示的值均失败，不补零。
  此版本不输出可选字段，空结果输出零行。

API 默认 `ad_status=STATUS_NOT_DELETE` 筛选生效，**不包括已删除广告**，不是全状态历史导出。
不支持自定义筛选或自动按广告 ID 分区。请求显式指定 `query_lifetime=false` 和
`query_mode=REGULAR`，采用 API 默认顺序，不保证快照顺序稳定。
Branded Mission 数据需要广告主层级报表，本连接器的 `ad_id` 维度不返回此类数据。

### 错误和限制

HTTP 200 但 API code 非零时直接失败。不合法响应、缺失分页字段、页码或总数不一致、
页面不完整、重复维度键、总数变化均失败。权限、解析、响应大小和传输错误不重试。
HTTP 重试使用有上限的指数等待，支持 Retry-After 秒数和 HTTP 日期；非法值失败且不回显。

官方当前同步报表文档描述 20,000 个广告 ID 截断上限，较早 v1.3 更新说明为 10,000。
`X-Tt-Ads-Throttle` 用于超限警告。本连接器**保守地拒绝该响应头的任何非空值**，
包括无法识别的警告，且不记录其内容。缺少该头不能证明数据完整，分页不能绕过截断。
需要筛选或异步导出的广告主不属于此版本的完整导出范围。同步结果可能与 Ads Manager 下载不同。

禁止自动重试、重定向、Cookie 和响应压缩。生产 origin 固定为
`https://business-api.tiktok.com`。关闭读取器会中止请求并唤醒重试等待。
JVM/操作系统 DNS 解析和阻塞的下游 collect 可能超过截止时间，因此在这些阶段不保证硬性取消时限。

## 前提与验证

需要通过 TikTok API for Business 获得具有广告主报表权限的令牌。
通过受保护的配置或环境变量提供；不要把令牌放入查询参数、schema 字段名、自定义键或调试日志。
mock_url 仅接受非秘密哨兵值 mock-token，防止真实令牌误传。

本地模拟测试和真实引擎上的模拟作业只能验证协议处理及引擎集成，
不能证明真实广告主权限、生产配额、数据新鲜度或完整性。真实环境验证需要另行提供授权访问。

## 示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}
source {
  TikTokAds {
    token = ${TIKTOK_ADS_TOKEN}
    advertiser_id = "123456789"
    data_level = "AUCTION_AD"
    start_date = "2026-09-01"
    end_date = "2026-09-02"
    dimensions = ["ad_id", "stat_time_day"]
    metrics = ["spend", "impressions", "clicks"]
    schema {
      fields {
        ad_id = string
        stat_time_day = string
        spend = "decimal(38, 6)"
        impressions = bigint
        clicks = bigint
      }
    }
  }
}
sink {
  Console {}
}
```

## 参考资料

- [同步报表 API](https://business-api.tiktok.com/portal/docs/run-a-synchronous-report/v1.3)
- [官方 SDK](https://github.com/tiktok/tiktok-business-api-sdk/blob/main/python_sdk/docs/ReportingApi.md)
- [基本报表维度](https://business-api.tiktok.com/portal/docs?id=1751443956638721)

## 变更日志

<ChangeLog />
