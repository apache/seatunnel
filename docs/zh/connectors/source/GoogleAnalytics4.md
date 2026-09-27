import ChangeLog from '../changelog/connector-google-analytics4.md';

# GoogleAnalytics4

<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements. See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License. You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

## 描述

通过 Google Analytics Data API v1beta 的 `runReport` REST 方法读取一个 GA4 属性的聚合报告。
本连接器不是原始事件导出、CDC 或不可变快照。

## 支持的连接器版本

Google Analytics 4，Data API v1beta；不支持 Universal Analytics。
连接器 JAR 已包含 Google 认证和 HTTP 依赖，无需单独安装驱动。

## 支持的引擎

SeaTunnel Zeta、Flink、Spark。

## 主要功能

- 仅支持批处理，source 并行度必须为 1。
- 一个属性、一个明确的绝对日期区间（首尾日期均包含，按属性时区解释）。
- 明确的维度、指标、Google 指标类型和匹配的 SeaTunnel schema。
- 有界分页、响应大小、请求时限及瞬时错误重试。
- 使用服务账号文件进行 OAuth 令牌刷新；隔离的 HTTP 测试服务不需要凭据。

## 一致性与恢复

按配置顺序对全部维度使用 Google 的 `ALPHANUMERIC` 升序排序。
下一页 offset 按实际返回行数推进，允许非空短页；不会跳过空页或静默截断。
返回总行数变化、重复或乱序维度组合、到达报告末尾前的空页，以及响应列名、类型、行宽不匹配均导致失败。

这些检查不能证明数据完整或具有快照一致性。GA4 可能在分页请求之间更新历史报告，
即使总行数和排序未变化，也可能有行跨越 offset 或指标值变化。
绝对日期只固定请求参数，不冻结服务端数据。重要场景应使用已稳定的历史区间并进行结果核对。

一次报告读取期间，共用的 single-split reader 持有 checkpoint 锁；没有页级 checkpoint 或可恢复 offset。
任务恢复会使用相同日期从 offset 0 重读整个报告。报告变化时连接器不会自行重新开始报告。
引擎或作业的重启次数限制由运维配置。已输出的行可能重放，后续页面失败也可能在非事务 sink 中留下部分数据。
应使用暂存/替换流程或符合恢复要求的 sink，不能把失败作业的输出当作完整报告。

检测到以下元数据时，连接器明确失败：`samplingMetadatas` 采样、`subjectToThresholding` 阈值处理、
`dataLossFromOtherRow` 聚合数据丢失、活动指标访问限制，以及非空 `emptyReason`。
即使阈值处理可能未实际删除任何行，也会拒绝该报告；V1 不提供静默接受这些限制的选项。
允许缺少可选元数据，但没有警告不代表完整性保证。
单独出现字面维度值 `(other)` 不作为聚合丢失的判断依据。

## 认证

在服务账号所属项目启用 Google Analytics Data API，并授予服务账号邮箱对 GA4 属性的只读权限
（例如 Viewer）。在每个 worker 的配置路径提供可信的服务账号 JSON 文件，限制文件读取权限。
V1 仅支持服务账号密钥文件，不支持 ADC、直接 access token、用户 refresh token 或工作负载身份联合。
不要把密钥内容放在作业配置中。

通过 Google Auth Library 使用 `analytics.readonly` scope，在令牌即将过期时刷新，
每页收到 HTTP 401 后最多强制刷新一次。
令牌端点固定为 `https://oauth2.googleapis.com/token`，请求及报告时限同样适用于令牌交换。
无效文件、非标准 token_uri 或刷新错误均失败，连接器错误信息不包含密钥、令牌、HTTP 正文或底层认证异常。

`emulator_url` 是显式启用的未认证 HTTP 测试服务地址，不是 Google 官方 GA4 模拟器。
它与密钥文件互斥，仅接受非 Google 的 HTTP origin，不允许用户信息、路径、query 或 fragment。
不要用于生产 GA4 访问。生产请求固定使用 Google HTTPS 端点，禁用重定向、cookie 和隐式 HTTP 重试。

## Source 选项

| 名称 | 类型 | 必填 | 默认值 | 说明 |
| :--- | :--- | :--- | :--- | :--- |
| property_id | string | 是 | - | 一个数字 GA4 属性 ID，不是 measurement ID。 |
| start_date | string | 是 | - | 包含的开始日期，绝对有效日历日期 yyyy-MM-dd，不支持相对日期。 |
| end_date | string | 是 | - | 包含的结束日期，不早于 start_date。 |
| dimensions | array | 否 | [] | 按顺序指定不重复的维度 API 名称，最多 9 个。 |
| metrics | array | 是 | - | 按顺序指定不重复的指标 API 名称，1 至 10 个，不能与维度重名。 |
| metric_types | array | 是 | - | 与指标顺序一致的精确 Google MetricType 枚举值。 |
| schema | config | 是 | - | 字段必须严格按维度、指标顺序排列，名称和类型必须匹配。 |
| service_account_key_file | string | 条件必填 | - | worker 上的可信服务账号 JSON 文件，最大 1 MiB；两个认证选项必须且只能选一个。 |
| emulator_url | string | 条件必填 | - | 未认证的隔离 HTTP 测试服务 origin，与密钥文件互斥。 |
| page_size | int | 否 | 1000 | 每页请求行数，1 至 10000；有意小于 Google API 最大值。 |
| max_report_rows | int | 否 | 1000000 | 报告总行数超过此值则失败，不截断；1 至 10000000。 |
| max_response_bytes | int | 否 | 4194304 | 每页最大响应字节数，1024 至 16777216；超过时失败且不重试。 |
| request_timeout_ms | int | 否 | 30000 | 包括响应正文传输的 HTTP 请求时限，100 至 120000 毫秒；不保证能中断 JVM/平台的同步 DNS 解析。 |
| report_timeout_ms | int | 否 | 600000 | 报告整体时限，100 至 3600000 毫秒；适用于 HTTP、认证和重试等待，在行输出之间检查，不能中断同步 DNS 解析或下游阻塞的 collect 调用。 |
| max_retries | int | 否 | 3 | 每页瞬时传输错误或 HTTP 429/500/502/503/504 的额外尝试次数，0 至 5。 |
| max_retry_wait_ms | int | 否 | 30000 | 每次重试等待上限，1000 至 120000 毫秒。 |

重试采用有界指数退避和抖动，并遵守 `Retry-After` 秒数或 HTTP 日期。
服务端要求的等待超过配置上限时，作业失败而不是提前重试。
无效请求、权限错误、schema 错误、过大响应和认证刷新错误不重试。
未完成报告的页面返回 token 或 server-error 配额耗尽时，在下一次请求之前失败；
不会在后台持续等待每日或每小时配额重置。

内存按页限制，不缓存整个报告。JSON 解析和类型化行的内存使用量会超过响应字节大小本身。
应根据 worker 堆内存同时调整 page_size 和 max_response_bytes。
关闭/取消会终止活动 HTTP 请求并唤醒重试等待。请求时限也约束停滞的响应。
报告时限不控制引擎重启，也不能消除下游背压。
Apache HttpClient 在建立 socket 连接之前同步解析主机名。JVM/平台的 DNS 查询可能在连接和
socket 超时之外阻塞，终止请求也不保证中断该查询。因此，DNS 解析阻塞期间，请求/报告时限
和取消不是严格的墙钟时间上限；OAuth token 请求也有相同限制。

## 数据类型

维度使用 STRING，包括空字符串和日期形式的维度值，不自动解析日期。
`TYPE_INTEGER` 指标要求 BIGINT，并检查 64 位溢出。
`TYPE_FLOAT`、`TYPE_SECONDS`、`TYPE_MILLISECONDS`、`TYPE_MINUTES`、`TYPE_HOURS`、
`TYPE_STANDARD`、`TYPE_CURRENCY`、`TYPE_FEET`、`TYPE_MILES`、`TYPE_METERS` 和
`TYPE_KILOMETERS` 要求 DOUBLE。无效或非有限数值导致失败。
DOUBLE 使用浮点精度，货币指标也不是精确十进制金额。
每页检查精确 Google 类型枚举，因此不会把单位变化误认为兼容的 DOUBLE 类型。

请先核对属性可用的维度、指标及兼容性。API 拒绝不兼容请求时，连接器返回 HTTP 状态并隐藏响应正文。
V1 不支持过滤器、表达式、cohort、comparison、多个日期区间、实时报告或额外指标聚合。
构造 factory/schema 时不执行网络请求。

## 示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}
source {
  GoogleAnalytics4 {
    property_id = "123456789"
    service_account_key_file = "/run/secrets/ga4-service-account.json"
    start_date = "2024-01-01"
    end_date = "2024-01-31"
    dimensions = ["country"]
    metrics = ["activeUsers", "purchaseRevenue"]
    metric_types = ["TYPE_INTEGER", "TYPE_CURRENCY"]
    schema {
      fields {
        country = string
        activeUsers = bigint
        purchaseRevenue = double
      }
    }
  }
}
sink {
  Console {}
}
```

仅在隔离测试中，将 service_account_key_file 替换为 `emulator_url = "http://fixture:1080"`，不要同时配置两者。

## 验证

提供确定性的 HTTP 测试以及模拟服务器的引擎 E2E 测试。
认证测试使用临时生成的 RSA 密钥和模拟 OAuth 交换，不验证真实 GA4 属性。
生产使用前仍需用经过授权的真实属性凭据验证访问权限、指标兼容性、分页、配额和报告限制。
连接器不包含真实凭据。

## 参考

- [runReport REST API](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/runReport)
- [响应元数据](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/ResponseMetaData)
- [指标类型](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/MetricType)
- [配额](https://developers.google.com/analytics/devguides/reporting/data/v1/quotas)
- [快速开始与属性访问](https://developers.google.com/analytics/devguides/reporting/data/v1/quickstart-client-libraries)

## 更新日志

<ChangeLog />
