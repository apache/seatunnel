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

import ChangeLog from '../changelog/connector-http-avni.md';

# Avni

> Avni 集成 API 源连接器 · BATCH

## 描述

源连接器依次读取所选的受访者、项目登记、项目访视和普通访视。各类资源独立分页，保留服务端提供的关联标识、观测值和作废标记。扫描由编号为 0 的子任务执行，建议将源连接器并行度设为 1。

| 资源 | Avni 集成接口 |
| --- | --- |
| `subjects` | `/api/subjects` |
| `program_enrolments` | `/api/programEnrolments` |
| `program_encounters` | `/api/programEncounters` |
| `encounters` | `/api/encounters` |

## 身份认证

使用具备 User 角色及所选资源查看权限的 Avni 集成账号。该账号需支持无人值守的 AWS Cognito 用户名和密码登录。首次密码修改、多因素认证等登录挑战需要提前处理。连接器从 Avni 获取公开的 Cognito 应用标识，将账号凭据提交给独立的 Cognito 地址，然后用 `auth-token` 请求头将 ID 令牌发送给 Avni。数据请求收到 401 时尝试重新登录一次。[Avni API 指南](https://avni.readme.io/docs/api-guide)

## 参数

| 名称 | 类型 | 是否必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `username` | string | 是 | | Avni 集成账号用户名。 |
| `password` | string | 是 | | Avni 集成账号密码。 |
| `start_date` | string | 是 | | 审计时间下界，不包含该时刻。使用带时区偏移、毫秒精度的 ISO 8601 时间戳。 |
| `end_date` | string | 否 | 批任务启动时间减 10 秒 | 排他的时间上界。显式设置只能缩短默认时间窗。 |
| `api_origin` | string | 否 | `https://app.avniproject.org` | 固定的 Avni HTTPS 地址。 |
| `identity_origin` | string | 否 | `https://cognito-idp.ap-south-1.amazonaws.com` | 固定的 Cognito HTTPS 地址，须与 Avni 地址不同。 |
| `allow_custom_endpoints` | boolean | 否 | `false` | 允许显式配置可信的非标准 HTTPS 地址。 |
| `tls_trust_certificate` | string | 否 | | 仅对此连接器生效的 X.509 信任证书，支持 PEM 或 base64 DER。两处地址均须通过主机名校验。 |
| `resources` | list of string | 否 | 全部四类 | 资源名称见上表。空列表选择全部资源。 |
| `page_size` | int | 否 | `100` | 请求的页大小，范围 1 至 1,000。 |
| `max_pages_per_resource` | int | 否 | `10000` | 单类资源最多接受的页数。 |
| `connect_timeout_ms` | int | 否 | `10000` | TCP 连接超时。 |
| `request_timeout_ms` | int | 否 | `30000` | 单次 HTTPS 请求尝试的时限。 |
| `operation_timeout_ms` | int | 否 | `120000` | 单次读取的时限，包含身份发现、登录和重试。 |
| `min_request_interval_ms` | int | 否 | `100` | 请求尝试之间的最短间隔。 |
| `max_retries` | int | 否 | `3` | 可重试错误的额外尝试次数。 |
| `max_response_bytes` | int | 否 | `4194304` | 单个响应体传输和解码后的字节数上限。 |

## 输出和恢复

每行包含 `resource`、`id`、`last_modified_at` 和 `record`，均为字符串。标识来自 `ID`，时间从 `audit["Last modified at"]` 转为 UTC。`record` 保留原始 JSON 对象，其中可能含有个人或临床数据，请使用获准存储所选记录的目标端。

本批次的每一页使用同一时间窗，两个边界值均排除。检查点保留分页和页内行位置，能够区分同一审计毫秒的多条记录。恢复时先校验当前页摘要，再发送该页的剩余记录。如果集合变化或上游数据已失效，可能需要重新开始有界扫描。此接口读取当前可见记录，不提供事务快照、删除日志或持续 CDC。

如果后续批次可能遇到迟到数据，可使用重叠的时间窗，并按标识对账。如果两个排他时间窗精确相邻，恰好在共同边界上的记录会被两个窗口排除。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Avni {
    username = ${AVNI_USERNAME}
    password = ${AVNI_PASSWORD}
    start_date = "2025-06-01T00:00:00.000Z"
    resources = ["subjects", "program_enrolments", "program_encounters", "encounters"]
  }
}

sink {
  LocalFile {
    path = "/secure/avni-export"
    file_format_type = "json"
  }
}
```

<ChangeLog />
