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

import ChangeLog from "../changelog/connector-http-buildkite.md";

# Buildkite

## 描述

Buildkite Source 以有界批作业读取十二种 REST API 资源，通过 API 访问令牌向 `api.buildkite.com` 发送 HTTPS GET 请求。为所选资源授予读取权限。API 会省略没有有效套餐的组织，组织范围内的请求若返回 HTTP 404，作业会报错。[Buildkite REST API](https://buildkite.com/docs/apis/rest-api)、[API 访问令牌](https://buildkite.com/docs/apis/managing-api-tokens)。

| 资源 | 直接读取权限 | 记录主键 | 创建时间过滤 |
| --- | --- | --- | --- |
| `organizations` | `read_organizations` | `id` | 本地 |
| `analytics_organizations_suites` | `read_suites` | `id` | |
| `organizations_pipelines` | `read_pipelines` | `id` | 本地 |
| `access-token` | | `uuid` | |
| `builds` | `read_builds` | `id` | API 和本地 |
| `organizations_clusters` | `read_clusters` | `id` | 本地 |
| `organizations_builds` | `read_builds` | `id` | API 和本地 |
| `organizations_pipelines_builds` | `read_builds` | `id` | API 和本地 |
| `organizations_clusters_queues` | `read_clusters` | `id` | 本地 |
| `organizations_clusters_tokens` | `read_clusters` | `id` | 本地 |
| `organizations_emojis` | | `name` | |
| `user` | `read_user` | `id` | 本地 |

单个读取子任务依次遍历各组织及其选定子资源。流水线构建在所属流水线下读取，队列及 Agent 令牌在所属集群下读取。读取任一组织子资源时，还需要 `read_organizations` 用于发现组织。流水线构建需要 `read_pipelines`，读取 Agent 令牌元数据可能还需要集群管理权限。访问令牌和 Agent 令牌接口提供元数据。若访问令牌、Agent 令牌或测试套件的元数据意外返回密钥字段，Source 会丢弃这些字段。[组织接口](https://buildkite.com/docs/apis/rest-api/organizations)、[访问令牌接口](https://buildkite.com/docs/apis/rest-api/access-token)、[Agent 令牌](https://buildkite.com/docs/apis/rest-api/clusters/agent-tokens)。

## 选项

| 名称 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| token | string | 是 | | Buildkite API 访问令牌。 |
| resources | list | 否 | [] | 上表中的资源名，空列表选择全部十二种。 |
| start_date | string | 读取带创建时间的资源时必填 | | 包含此时刻，使用带时区的 RFC 3339 格式。 |
| end_date | string | 否 | 批作业启动时刻 | 排除该时刻，首次请求前固定。 |
| page_size | int | 否 | 100 | 标准分页每页 1 至 100 项。 |
| max_pages_per_collection | int | 否 | 10000 | 同一父资源下单类集合最多接受的页数。 |
| max_pages_per_scan | int | 否 | 100000 | 一次扫描最多接受的响应数。 |
| max_unpaginated_items | int | 否 | 10000 | 单次完整列表响应的最大记录数。 |
| tls_trust_certificate | string | 否 | JVM 默认信任配置 | 仅此 Source 使用的可选 PEM 或 Base64 DER X.509 信任锚，仍校验主机名。 |
| connect_timeout_ms | int | 否 | 10000 | TCP 连接超时，单位为毫秒。 |
| request_timeout_ms | int | 否 | 30000 | 单次 HTTPS 请求的截止时间，单位为毫秒。 |
| operation_timeout_ms | int | 否 | 120000 | 一次逻辑读取包含重试与等待的总时限，必须不小于 `request_timeout_ms`。 |
| request_interval_ms | int | 否 | 100 | 相邻请求尝试的最短间隔，单位为毫秒。 |
| max_retries | int | 否 | 3 | 暂时性读取失败时可额外尝试的次数。 |
| max_retry_delay_ms | int | 否 | 60000 | 请求重试的最大等待时间，包含服务端限流恢复或 Retry-After 提示。 |
| max_response_bytes | int | 否 | 8388608 | 解压前、解压后各自的响应字节上限。 |

标准分页根据 HTTP `Link` 头中的 next 关系继续。空页或未填满的页仍可能有续页。连接器核对当前集合，并使用原筛选条件构造下一次请求。组织表情列表一次返回。测试套件接口可返回单个完整列表，如服务提供 next 则继续分页。`access-token` 和 `user` 返回单个对象。[分页](https://buildkite.com/docs/apis/rest-api#pagination)、[测试套件](https://buildkite.com/docs/apis/rest-api/test-engine/suites)、[表情接口](https://buildkite.com/docs/apis/rest-api/emojis)。

暂时性错误会在配置的时限内重试。限流时，Source 采用适用的组织、用户和深度构建分页恢复时间。若重试等待超过重试时限或操作时限，读取失败。格式错误、重定向、认证或权限错误都会导致作业失败。[Buildkite 限流](https://buildkite.com/docs/apis/rest-api/rate-limits)。

## 时间窗口与检查点

带创建时间的记录按 UTC 的半开区间 `[start_date, end_date)` 输出。构建请求发送 `created_from` 和 `created_to`，其余带日期列表在每次响应后过滤。早于时间窗创建的父资源仍可发现符合窗口的子记录。构建筛选依据创建时间，若需读取较早构建后续更新的状态，新批次应覆盖该构建的创建时间。若 Buildkite 拒绝较深的构建分页，可缩短日期区间。[构建接口](https://buildkite.com/docs/apis/rest-api/builds)。

检查点记录原截止时间和当前组织、父页、子页的位置。恢复时先验证页面摘要及父路径，再读取剩余位置。已完成的记录和集合会跳过。更换令牌或改选资源需新启批作业。Buildkite 集合在分页期间可能变化，重放时页面已变则恢复失败，新批次可能读到之前的记录。

## 输出

七个输出列均为字符串。

| 列名 | 含义 |
| --- | --- |
| `resource` | 所选资源名。 |
| `id` | 服务返回的记录主键。 |
| `organization_slug` | 组织记录和子资源的组织 slug。 |
| `pipeline_slug` | 流水线记录及其构建列表的流水线 slug。 |
| `cluster_id` | 集群记录、队列与 Agent 令牌列表的集群 ID。 |
| `created_at` | 带时间资源的 UTC 创建时间，其他资源为 null。 |
| `record` | 保留服务原始字段、嵌套结构、null 与数值精度的 JSON 字符串。 |

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Buildkite {
    token = ${BUILDKITE_TOKEN}
    resources = ["organizations_pipelines_builds", "organizations_emojis"]
    start_date = "2025-01-01T00:00:00Z"
    end_date = "2025-02-01T00:00:00Z"
  }
}
```

## 变更日志

<ChangeLog />
