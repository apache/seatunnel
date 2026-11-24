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

import ChangeLog from '../changelog/connector-http-harvest.md';

# Harvest

## 支持的引擎

SeaTunnel Zeta、Flink、Spark

## 描述

通过 `GET /v2/time_entries` 读取一个 Harvest 账户的工时记录，仅支持 BATCH 模式和 Source 并行度 1。可见记录取决于令牌的权限，连接器不会提升账户访问权限。

## 参数

| 名称 | 类型 | 必填 | 默认值 | 描述 |
| --- | --- | --- | --- | --- |
| token | String | yes | - | 个人访问令牌，不含 `Bearer ` 前缀。使用引擎已有的 `token` 日志脱敏规则。 |
| account_id | String | yes | - | 令牌有权访问的 Harvest 数字账户 ID。 |
| schema | Config | yes | - | 每条工时记录的字段结构。嵌套对象可使用 ROW 类型，仅输出配置的字段。 |
| base_url | String | no | `https://api.harvestapp.com` | 不含用户信息、查询参数、片段或路径的 HTTP(S) 地址。仅为可信代理或测试覆盖此值；HTTP 会明文传输凭据，重定向会被拒绝。 |
| page_size | Int | no | 100 | 每页记录数，范围为 1–2000。 |
| from | String | no | - | 包含边界的 `spent_date` 起始日期，格式为 `yyyy-MM-dd`。 |
| to | String | no | - | 包含边界的结束日期，格式为 `yyyy-MM-dd`，不能早于 `from`。 |
| updated_since | String | no | - | 带时区偏移的 ISO 8601 时间，例如 `2026-11-01T00:00:00Z`；仅作为更新过滤条件，不是持久化增量游标。 |
| retry | Int | no | 3 | 每页对 HTTP 429、500、502、503、504 的额外重试次数，0 表示禁用。 |
| retry_backoff_ms | Int | no | 1000 | 正整数，指数退避的初始等待毫秒数。 |
| max_retry_delay_ms | Int | no | 60000 | 每次重试的最长等待毫秒数，不能小于 `retry_backoff_ms`。服务端要求更长等待时直接失败。 |
| connect_timeout_ms | Int | no | 10000 | 连接与连接池等待超时毫秒数，必须大于 0。 |
| socket_timeout_ms | Int | no | 60000 | Socket 读取超时毫秒数，必须大于 0。 |
| common options | - | no | - | 参见[Source 通用参数](../common-options/source-common-options.md)，Source 并行度必须为 1。 |

## 读取和恢复

连接器按顺序请求分页，仅在响应明确包含 `next_page: null` 时结束。输出数据前校验 `time_entries` 数组、当前页和连续的下一页。响应结构错误、分页元数据错误、类型转换失败以及 HTTP 错误都会使任务失败，不会被当作空结果，也不会跟随响应中的 URL。

仅重试上表列出的状态码；认证错误、重定向、损坏的 JSON、类型转换错误和网络异常不会由连接器重试。有效的 `Retry-After`（秒数或 HTTP 日期）与指数退避取较长等待时间；缺失或无效时使用本地退避。等待超过 `max_retry_delay_ms` 时失败，不提前重试。线程中断会停止后续重试。

Checkpoint 只保存带版本的完成标记。未完成分片恢复后从第 1 页重新读取，因此已输出的记录可能重复；需要时使用工时记录 `id` 配合幂等或 upsert Sink。已完成分片不会重新读取。Reader 分片状态不保存页码和令牌；凭据仍属于作业配置，需要保护作业配置的访问。

Harvest 分页接口不提供一致性快照：并发新增、更新、删除会改变分页结果，时间过滤也不会冻结数据。本连接器不提供 CDC、删除事件或 exactly-once 抽取。需要稳定导出时应读取不再变化的时间段，并按需与服务端核对。网络请求和重试等待不持有 checkpoint 锁。

## 示例

```hocon
env { parallelism = 1, job.mode = "BATCH" }
source {
  Harvest {
    token = "<personal-access-token>"
    account_id = "12345"
    from = "2026-11-01"
    to = "2026-11-23"
    schema.fields {
      id = bigint
      spent_date = date
      hours = "decimal(18,4)"
      notes = string
      user = "row<id bigint, name string>"
    }
  }
}
sink { Console {} }

```

## 参考资料

- [Harvest 工时记录](https://help.getharvest.com/api-v2/timesheets-api/timesheets/time-entries/)
- [Harvest 认证](https://help.getharvest.com/api-v2/authentication-api/authentication/authentication/)
- [Harvest 限流](https://help.getharvest.com/api-v2/introduction/overview/general/)

<ChangeLog />
