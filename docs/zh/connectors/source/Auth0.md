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

import ChangeLog from '../changelog/connector-http-auth0.md';

# Auth0

> Auth0 Management API 源连接器 · BATCH

## 描述

从 Auth0 租户读取应用客户端、用户、组织、组织成员及成员的直接角色分配。连接器按顺序扫描单个租户，所选资源共用组织和成员列表。单个 split 由编号为 0 的子任务处理，建议将源连接器的并行度设为 1。

## 身份认证和权限

`tenant_url` 填写固定的租户 HTTPS 地址。已有 Management API 访问令牌时设置 `token`。使用已获授权的机器应用时设置 `client_id`、`client_secret` 和 `audience`。例如，租户的 Management API audience 可以是 `https://my-tenant.us.auth0.com/api/v2/`。源连接器在 OAuth 访问令牌过期前更新令牌，收到 Management API 的 401 时尝试更新一次。租户请求始终使用配置的 HTTPS 地址，并校验证书和主机名。使用可信的自定义域名或私有租户主机名时，须显式启用 `allow_custom_domain`。

| 资源 | 所需权限 |
| --- | --- |
| `clients` | `read:clients` |
| `users` | `read:users` |
| `organizations` | `read:organizations` |
| `organization_members` | `read:organizations`、`read:organization_members` |
| `organization_member_roles` | `read:organizations`、`read:organization_members`、`read:organization_member_roles` |

## 参数

| 名称 | 类型 | 是否必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `tenant_url` | string | 是 | | 租户 HTTPS 地址，可带尾部斜杠，不包含其他路径、用户信息、查询或片段。 |
| `allow_custom_domain` | boolean | 否 | `false` | 允许显式配置非标准 HTTPS 租户主机名。标准 Auth0 域名以 `.auth0.com` 结尾，使用 HTTPS 443 端口。 |
| `token` | string | 条件必填 | | Management API 访问令牌。选择此选项或下列 OAuth 应用凭据参数。 |
| `client_id` | string | 条件必填 | | 机器应用标识。 |
| `client_secret` | string | 条件必填 | | 机器应用密钥。 |
| `audience` | string | 条件必填 | | 以 `/api/v2/` 结尾的 HTTPS Management API 标识符。 |
| `tls_trust_certificate` | string | 否 | | 仅对此连接器生效的 X.509 信任证书，支持 PEM 或 base64 DER。 |
| `resources` | list of string | 否 | 全部五类 | 上表中的资源名称。空列表选择全部资源。 |
| `start_date` | string | 否 | `2013-01-01T00:00:00.000Z` | 用户 `updated_at` 的起始时间，包含该时刻。使用带时区偏移的 ISO 8601 时间戳，精度为毫秒。 |
| `end_date` | string | 否 | 批任务启动时间 | 同格式的结束时间，包含该时刻。初始边界保存在 split 中。 |
| `page_size` | int | 否 | `50` | 单页大小，范围 1 至 100。 |
| `max_pages_per_resource` | int | 否 | `10000` | 批次中每类资源的逻辑请求上限，包括用户计数探测和所有父对象下的请求。 |
| `connect_timeout_ms` | int | 否 | `10000` | TCP 连接超时。 |
| `request_timeout_ms` | int | 否 | `30000` | 每次 HTTPS 尝试的时限。 |
| `operation_timeout_ms` | int | 否 | `120000` | 每次逻辑读取的时限，包含认证、限速等待及重试。 |
| `min_request_interval_ms` | int | 否 | `100` | 请求尝试之间的最小间隔。 |
| `max_retries` | int | 否 | `3` | 可重试读取错误的额外尝试次数。 |
| `max_response_bytes` | int | 否 | `4194304` | 单个响应体传输和解码后的字节数上限。 |

### 输出字段

`resource`、`id`、`organization_id`、`user_id` 和 `record` 均为字符串。客户端、用户和组织行的两个上下文字段为空。成员行保留组织与用户标识，并使用成员的 `user_id` 作为 `id`。成员角色行的 `id` 为角色标识，同时保留组织与用户标识。`record` 将 Auth0 返回的字段保存为 JSON 对象。根据权限，这些字段可能包含个人数据或应用凭据，请使用获准存储这些字段的目标端。

### 分页和恢复

组织、成员使用 Auth0 的 checkpoint 分页，客户端和直接成员角色使用偏移量分页。用户搜索按服务端确定的 `updated_at` 顺序、双端包含的毫秒区间进行，并先查询数量。达到 1,000 条阈值时，先拆分时间区间再分页。如果同一毫秒达到阈值，任务会明确报错。Auth0 用户搜索采用最终一致的索引，并受索引可见性和用户档案限制约束。此源读取有界区间内当前可见的索引，不提供持续更新或源端快照隔离。[Auth0 用户搜索限制](https://auth0.com/docs/manage-users/user-search/user-search-best-practices)

检查点记录扫描范围、时间边界、当前父对象标识、分页游标和已经发出的行数。恢复时重新读取未完成的页面并校验其摘要。静态令牌可以在同一租户下轮换。OAuth 恢复时保持应用标识和 audience 一致，允许更换应用密钥。请保留所选资源需要的权限。源端数据变化或服务端游标过期可能需要重新启动一次扫描。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Auth0 {
    tenant_url = "https://my-tenant.us.auth0.com"
    token = ${AUTH0_TOKEN}
    resources = ["clients", "users", "organizations", "organization_members", "organization_member_roles"]
    start_date = "2025-06-01T00:00:00.000Z"
  }
}

sink {
  LocalFile {
    path = "/secure/auth0-export"
    file_format_type = "json"
  }
}
```

<ChangeLog />
