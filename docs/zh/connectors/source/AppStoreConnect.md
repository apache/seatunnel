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

import ChangeLog from '../changelog/connector-http-app-store-connect.md';

# App Store Connect

安装 `connector-http-app-store-connect`，在批处理任务中使用 `AppStoreConnect`。团队 API 密钥需要有权访问所选应用和报表，销售与财务报表的角色要求不同。读取持续或一次性分析报表前，先在 App Store Connect 创建对应请求。连接器发现并下载已经生成的报表分段。[团队 API 令牌要求](https://developer.apple.com/documentation/appstoreconnectapi/generating-tokens-for-api-requests) · [分析报表生命周期](https://developer.apple.com/documentation/appstoreconnectapi/downloading-analytics-reports)

## 可选资源

| 资源 | 数据 |
| --- | --- |
| `list_id_apps`、`customer_reviews_per_app` | 应用及各应用的客户评论 |
| `sales_report`、`subscriber_report`、`subscription_report`、`subscription_event_report` | 销售与趋势日报表 |
| `finance_report` | 区域 `ZZ` 的合并月度 `FINANCIAL` 报表 |
| `analytics_report_requests_ongoing`、`analytics_report_requests_historical` | 应用的 `ONGOING` 与 `ONE_TIME_SNAPSHOT` 分析报表请求 |
| `analytics_installations_reports`、`analytics_installations_instances`、`analytics_installations_segments`、`analytics_installations_segment_details`、`app_store_installations_and_deletions` | 安装与卸载标准报表的元数据和下载后的 TSV 行 |
| `analytics_app_download_reports`、`analytics_app_download_instances`、`analytics_app_download_segments`、`analytics_app_download_segment_details`、`app_download` | App 下载标准报表的元数据和下载后的 TSV 行 |

最后两组的十个资源均可在名称末尾加 `_historical`，读取对应的一次性历史快照。选择子资源时，连接器自动遍历父级列表。分段列表和分段详情的输出会移除临时下载地址。销售汇总日报使用版本 `1_0`，订阅者、订阅和订阅事件报表使用 `1_3`。[销售报表版本](https://developer.apple.com/documentation/appstoreconnectapi/get-v1-salesreports) · [财务报表](https://developer.apple.com/help/app-store-connect/getting-paid/download-financial-reports)

## 参数

| 参数 | 类型 | 默认值 | 用途 |
| --- | --- | --- | --- |
| `issuer_id` | 字符串 | 必填 | 团队密钥发行者 ID |
| `key_id` | 字符串 | 必填 | 团队密钥 ID |
| `secret_key` | 字符串 | 必填 | PEM 或 base64 DER 格式的 PKCS#8 P-256 私钥 |
| `vendor_number` | 字符串 | 无 | 五个销售与趋势或财务报表资源需要供应商编号 |
| `resources` | 字符串列表 | 全部 | 按配置顺序读取的资源名 |
| `sales_reports_start_date` | 字符串 | 批次 UTC 日期之前 365 天 | 销售、订阅者和订阅报表的起始日期，格式为 `YYYY-MM-DD`，读取至昨天，包含起止日期 |
| `subscription_event_reports_start_date` | 字符串 | 批次 UTC 日期之前 365 天 | 订阅事件的起始日期，格式为 `YYYY-MM-DD`，读取至批次日期之前两天，包含起止日期 |
| `subscription_event_skip_dates` | 字符串列表 | 空 | 跳过订阅事件报表请求的 `YYYY-MM-DD` 日期 |
| `finance_reports_start_date` | 字符串 | 批次 UTC 月份之前 11 个月 | 财务报表起始月份，格式为 `YYYY-MM`，读取至批次日期之前 35 天所在的月份，包含起止月份 |
| `analytics_reports_start_date` | 字符串 | 批次 UTC 日期之前 35 天 | 持续分析报表的处理起始日期，读取至昨天，包含起止日期。历史请求读取所有可用的每日实例。 |
| `reviews_start_date` | 字符串 | 无 | 可选的 ISO 8601 带偏移量时间戳，读取起始时间至批次开始时的评论，包含边界 |
| `api_origin` | 字符串 | `https://api.appstoreconnect.apple.com` | 接收团队 JWT 的 HTTPS API 源站 |
| `download_hosts` | 字符串列表 | 空 | 附加允许的签名下载 HTTPS 主机名，必须精确匹配。默认允许标准 Amazon S3 HTTPS 主机。 |
| `page_size` | 整数 | 200 | 每页请求的最大记录数，范围 1 至 200 |
| `max_pages_per_parent` | 整数 | 1000 | 每个父级关系的最大分页数 |
| `min_request_interval_ms` | 整数 | 100 | HTTPS 请求的最短间隔 |
| `request_timeout_ms` | 整数 | 30000 | 单次 HTTPS 尝试超时，含响应体处理 |
| `operation_timeout_ms` | 整数 | 120000 | 操作超时，覆盖重试、重签和下载地址刷新 |
| `max_retries` | 整数 | 3 | 临时 HTTP 错误或 GET 连接故障的额外重试次数 |
| `max_json_bytes` | 整数 | 4194304 | JSON 元数据传输和解压后响应体的字节上限 |
| `max_compressed_report_bytes` | 整数 | 33554432 | 报表传输字节上限 |
| `max_decoded_report_bytes` | 整数 | 134217728 | 报表解压字节上限 |
| `max_report_row_chars` | 整数 | 262144 | 单行报表记录的 UTF-16 码元上限 |

销售报表最多回溯 365 天，财务报表最多回溯 11 个月。报表可用性和保留期限由 Apple 决定。每次新建批次都会重新读取所配置的日期范围。

## 输出与恢复

输出包含字符串列 `resource`、`id`、`app_id`、`request_id`、`report_id`、`instance_id`、`segment_id`、`report_date` 和 `record`。层级 ID 和日期可以为空。分析报表的 `report_date` 是实例处理日期，事件日期位于 TSV 的 `Date` 列。 `record` 是 JSON，包含 Apple 元数据或按 TSV 表头映射的完整记录。TSV 字段以原始字符串输出，保留空字段、带前导零的标识和精确小数。ID 对应 API 对象或报表日期、分段内的行位置。

每个批次有一个活跃的源读取器。检查点记录当前资源、固定日期范围、当前分页位置和报表行偏移量。恢复时先校验分页及报表内容指纹，再从保存的位置继续读取。若 Apple 更改或移除了保存位置的数据，连接器以恢复错误终止。下载地址通过分段 ID 重新获取。写入结果取决于所选 Sink 的检查点支持。

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  AppStoreConnect {
    issuer_id = "<team-issuer-id>"
    key_id = "<team-key-id>"
    secret_key = ${APP_STORE_CONNECT_SECRET_KEY}
    resources = ["list_id_apps", "customer_reviews_per_app"]
    reviews_start_date = "2026-01-01T00:00:00Z"
  }
}

sink {
  Console {}
}
```

通过任务配置的密钥注入机制设置 `secret_key`。引擎需要信任 API 及下载主机的 HTTPS 证书。

<ChangeLog />
