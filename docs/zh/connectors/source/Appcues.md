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

import ChangeLog from '../changelog/connector-http-appcues.md';

# Appcues

Appcues 从 Appcues Public API 读取体验配置和账户定义。使用批处理模式，源端并行度设为 1。连接器通过验证主机名的 HTTPS 和 HTTP Basic 认证读取下列七类资源。 [Appcues Public API](https://api.appcues.com/v2/docs)

## 特性

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)

## 资源

通过 `GET /v2/accounts/{account_id}/{resource}` 获取每个资源的 JSON 数组，连接器读取完整响应后，按对应字段在本地应用时间窗口。

| 资源 | 时间字段 | 支持的格式 |
| --- | --- | --- |
| `launchpads` | `updated_at` | Unix 时间戳毫秒值 |
| `flows` | `created_at` | ISO 8601 或 Unix 时间戳毫秒值 |
| `banners` | `updated_at` | Unix 时间戳毫秒值 |
| `checklists` | `created_at` | ISO 8601 或 Unix 时间戳毫秒值 |
| `pins` | `updated_at` | Unix 时间戳毫秒值 |
| `tags` | `updated_at` | ISO 8601 |
| `segments` | `updated_at` | ISO 8601 |

毫秒值支持 JSON 整数及十进制字符串。ISO 8601 时间可带小数秒，也可不带。时间窗口包含 `start_date`，不包含 `end_date`。时间字段缺失或为 null 时跳过该记录。已提供的时间值无效时，当前资源在输出任何记录前失败。过滤前的完整响应受 `max_records` 约束。

## 源端选项

| 名称 | 类型 | 是否必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `username` | String | 是 | — | Appcues API 密钥，用作 Basic 认证的用户名。 |
| `password` | String | 是 | — | Appcues API 密钥的 secret，用作 Basic 认证的密码。 |
| `account_id` | String | 是 | — | Appcues 账户 ID。 |
| `start_date` | String | 是 | — | ISO 8601 格式且带时区的本地过滤起始时间，包含边界。 |
| `end_date` | String | 否 | 初次扫描时间 | ISO 8601 格式且带时区的过滤截止时间，不包含边界。 |
| `resources` | 字符串列表 | 否 | 上表所列七项及顺序 | 按读取顺序选择资源，名称不能重复。 |
| `api_url` | String | 否 | `https://api.appcues.com` | 美国或欧洲区的 HTTPS API 源地址。欧洲区使用 https://api.eu.appcues.com。 |
| `max_records` | Integer | 否 | `50000` | 每个资源过滤前的响应条数上限，范围为 1 到 100000。 |
| `max_response_bytes` | Integer | 否 | `16777216` | 压缩和解压后的响应体各自的字节上限，范围为 1024 到 33554432。 |
| `connect_timeout_ms` | Integer | 否 | `10000` | TCP 连接超时，范围为 1 到 300000。 |
| `read_timeout_ms` | Integer | 否 | `10000` | TLS 和响应读取的空闲超时，范围为 1 到 300000。 |
| `request_timeout_ms` | Integer | 否 | `30000` | 单个资源请求的总时限，包含重试等待，范围为 1 到 300000。 |
| `max_attempts` | Integer | 否 | `4` | 单个资源的请求次数上限，包含首次，范围为 1 到 10。 |

## 输出与恢复

输出包含四个字符串字段。

| 字段 | 值 |
| --- | --- |
| `resource` | 资源名称。 |
| `id` | 供应商记录 ID。 |
| `cursor_time` | 过滤字段归一化后的 UTC ISO 8601 时间。 |
| `data` | 完整记录的 JSON 文本，保留嵌套字段、null 和精确数值。 |

同一资源内按照归一化时间和 ID 排序。连接器读取完整资源数组，遇到分页、部分响应或意外续页信号时使任务失败。独立作业可以用相邻时间窗口继续读取历史数据。

作业开始读取前会固定窗口的截止时间。检查点恢复使用同一个窗口，跳过已完成的资源并重新请求待处理的响应。对于只输出了部分行的资源，源端在恢复行位置前会校验筛选后的响应摘要。供应商上已读取的记录仍可能独立变化。

TLS 使用 JVM 信任库，并校验所选地区端点的主机名。重定向和永久认证失败会使任务失败。临时 HTTP 429、500、502、503 和 504 响应在单个资源的总时限和次数限制内重试。源端错误和检查点状态不会包含凭据。

## 示例

把 API 密钥和 secret 作为 SeaTunnel 作业变量传入。

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Appcues {
    username = "${APPCUES_API_KEY}"
    password = "${APPCUES_API_SECRET}"
    account_id = "my-account"
    resources = ["flows", "segments"]
    start_date = "2026-01-01T00:00:00Z"
    end_date = "2026-02-01T00:00:00Z"
  }
}

sink {
  Console {}
}
```

## 变更日志

<ChangeLog />
