import ChangeLog from '../changelog/connector-http-7shifts.md';

# SevenShifts

{/*
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
*/}

`SevenShifts` Source 从 7shifts v2 API 读取资源。每次有界扫描读取一种资源。请在引擎中安装 `connector-http-7shifts`，并使用有权读取目标资源及相关公司、用户的客户访问令牌。实际可访问的资源取决于 7shifts 套餐和权限。 [7shifts 身份认证](https://developers.7shifts.com/reference/authentication)

## 配置项

| 名称 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `token` | String | 是 | | 客户访问令牌，用于 Bearer 认证。 |
| `resource` | String | 是 | | 从下表选择资源名称。 |
| `base_url` | String | 否 | `https://api.7shifts.com` | 受信任的 API 源地址，不含路径、查询、片段和凭据。允许末尾带斜杠。 |
| `allow_insecure_http` | Boolean | 否 | false | 允许显式信任的隔离端点使用明文 HTTP。 |
| `api_version` | String | 否 | `2026-01-01` | 每个请求携带的日期格式 API 版本。 |
| `start_date` | String | 否 | | 支持日期筛选的资源的修改时间下限，包含边界。格式为带时区偏移的 ISO-8601 时间。 |
| `page_size` | Integer | 否 | 100 | 支持分页的公司级端点每页请求的记录数，范围 1 到 100。 |
| `request_timeout_ms` | Integer | 否 | 30000 | 单页总时限，含重试及等待，范围 1 到 300000 毫秒。 |
| `max_attempts` | Integer | 否 | 4 | 单页 HTTP 请求最大尝试次数，范围 1 到 8。 |
| common-options | | 否 | | [Source 通用参数](../common-options/source-common-options.md)。 |

## 资源与输出

| 资源 | 范围 | `start_date` 对应的 API 筛选 | 数据内容 |
| --- | --- | --- | --- |
| `companies` | 令牌 | UTC 日期 | 每家公司一个对象 |
| `locations`、`departments`、`roles`、`users` | 公司 | UTC 日期 | 每条记录一个对象 |
| `time_punches`、`shifts` | 公司 | UTC 时间戳 | 每条记录一个对象 |
| `wages` | 用户 | 不支持 | 包含当前和即将生效工资的对象 |
| `assignments` | 用户 | 不支持 | 包含门店、部门和角色数组的对象 |
| `location_assignments`、`department_assignments`、`role_assignments` | 用户 | 不支持 | 每条分配记录一个对象 |

发现父级公司和用户时遍历全部游标，不应用输出记录的日期下限。用户发现包含 7shifts 返回的在职和停用用户。数据的父级范围来自请求路径，发现过程在各自范围内对 ID 去重。即使一页不足预期记录数或为空，只要有下一页游标就继续。公司列表接口没有公开的每页大小参数。

输出字段为 `resource STRING`、`company_id BIGINT`、可空的 `user_id BIGINT` 和 `payload STRING`。用户 ID 来自用户记录、用户请求路径或记录自带的用户 ID。数据字段为 `data` 中的完整 JSON 对象，保留嵌套值和数值精度。输出可能包含个人和薪资信息。

配置 `start_date` 后，只接受日期的端点会向前扩大一个 UTC 日，接受时间戳的端点会向前扩大一秒。对返回的 `modified` 执行包含边界的精确过滤，缺失或为空的修改时间仍保留。五个用户级端点仅支持全量扫描，配置 `start_date` 会报错。其余筛选条件使用服务端默认行为。

## 恢复与限制

源端并行度设为 1。检查点状态版本 2 保存遍历位置、分页游标，以及当前页已接受记录数和四个实际输出字段的 SHA-256 摘要，不保存凭据或记录内容。恢复时重新读取该页、应用相同时间下限，并在跳过任何记录前验证全部已接受前缀。前缀内容、顺序或长度变化时，在输出新记录前终止扫描。工资和分配信息的分组对象仍作为单行输出。最后一行与游标或父级切换在同一检查点边界内提交。不支持早期开发阶段的检查点格式。

完整响应校验和父级发现不持有输出器检查点锁。每次轮询最多接受一条已准备记录后即交还引擎，避免在整页限速输出期间持续持锁。单次输出器调用阻塞时，仍遵循引擎的取消和检查点行为。服务端游标变更或过期可能阻止恢复。API 不提供冻结快照：尚未接受的记录和后续发现结果仍可变化，包括服务端默认筛选返回的用户。新扫描使用配置中的时间下限和当前数据。最近一次完成检查点之后接受的记录仍可在恢复时重放，具体结果取决于下游恢复语义。

默认使用 HTTPS 验证证书及主机名。连接器拒绝重定向，认证或权限失败时直接报错。在配置的总时间和尝试次数内，会重试 HTTP 429、500、502、503 和 504。如果 `Retry-After` 指定的等待超过剩余时限，该页读取失败。提交作业时提供访问令牌，重新提交时可更新令牌。合作伙伴 OAuth 不在当前凭据契约内。

如果单个请求压缩前后超过 8 MiB、单端点超过 10,000 页、发现超过 4,096 家公司或一家公司超过 100,000 名用户，扫描失败。单个响应最多包含 10,000 条记录。游标过长或重复也会终止扫描。

## 示例

```hocon
env {
  job.mode = BATCH
  parallelism = 1
}

source {
  SevenShifts {
    token = ${?SEVEN_SHIFTS_TOKEN}
    resource = "time_punches"
    start_date = "2026-01-02T00:00:00Z"
  }
}

sink {
  Console {}
}
```

## 变更日志

<ChangeLog />
