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

import ChangeLog from '../changelog/connector-http-basecamp.md';

# Basecamp

> Basecamp 源连接器 · BATCH

## 描述

从一个 Basecamp 账户读取活动项目、日程工具、日程条目、待办和消息。连接器先遍历项目和日程，再读取待办与消息。只选择子资源时仍会发现其所属项目和工具。源连接器由编号为 0 的子任务执行账户扫描，建议并行度设为 1。

| 资源 | 读取行为 |
| --- | --- |
| `projects` | 列出可见的活动项目。 |
| `schedules` | 在各项目详情的 dock 中选择日程工具。 |
| `schedule_entries` | 列出启用日程的条目。 |
| `todos` | 按更新时间倒序读取账户内的 Todo 记录。 |
| `messages` | 按更新时间倒序读取账户内的 Message 记录。 |

选择 `schedules` 时会输出停用的日程工具。仅对启用的日程查询其条目。资源不存在或无权限时，任务会报告相应的 HTTP 状态与资源。

## 身份认证

使用已有的 Basecamp Bearer 访问令牌，或使用已注册的 Launchpad 集成应用凭据和刷新令牌。根据 Basecamp 要求，`user_agent` 填写可识别的应用名称和联系方式。数据请求发送至 `https://3.basecampapi.com` 下的目标账户路径，刷新请求发送至 `https://launchpad.37signals.com`。使用 Launchpad 凭据时，数据请求返回 401 后尝试刷新一次。[Basecamp 认证](https://github.com/basecamp/bc-api/blob/master/sections/authentication.md)

Launchpad 返回替换的刷新令牌时，当前 reader 会在内存中使用它。检查点不保存凭据，连接器不会写配置文件。重启任务时请提供有效的刷新令牌，尤其是原授权服务已使旧令牌失效时。直接配置的 Bearer 令牌可用至过期或撤销。

## 参数

| 名称 | 类型 | 是否必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `account_id` | string | 是 | | 十进制 Basecamp 账户标识。 |
| `user_agent` | string | 是 | | 可识别的应用名称与联系方式。 |
| `token` | string | 条件必填 | | 已有的 Bearer 令牌。选择此选项或下列 Launchpad 刷新参数。 |
| `client_id` | string | 条件必填 | | Launchpad 集成应用标识。 |
| `client_secret` | string | 条件必填 | | Launchpad 集成应用密钥。 |
| `refresh_token` | string | 条件必填 | | 有效的 Launchpad 刷新令牌。 |
| `tls_trust_certificate` | string | 否 | | 仅对此连接器生效的 X.509 信任证书，支持 PEM 或 Base64 DER。仍须通过 TLS 主机名校验。 |
| `resources` | list of string | 否 | 全部五类 | 上表所列资源名称。空列表选择全部资源。 |
| `start_date` | string | 读取待办或消息时必填 | | 包含边界的更新时间下界，使用带时区偏移的 ISO 8601 时间戳。 |
| `end_date` | string | 否 | 批任务启动时间 | 包含边界的更新时间上界。显式设置可以缩短默认窗口。 |
| `max_pages_per_collection` | int | 否 | `10000` | 项目列表、各类记录或单个日程条目的最大页数。 |
| `max_page_items` | int | 否 | `1000` | 单页记录和单个项目 dock 的最大条数。 |
| `connect_timeout_ms` | int | 否 | `10000` | TCP 连接超时。 |
| `request_timeout_ms` | int | 否 | `30000` | 单次 HTTPS 请求时限。 |
| `operation_timeout_ms` | int | 否 | `120000` | 单次读取的总时限，包含认证和重试。 |
| `min_request_interval_ms` | int | 否 | `100` | 各次请求尝试的最短间隔。 |
| `max_retries` | int | 否 | `3` | 暂时性错误的额外尝试次数。 |
| `max_response_bytes` | int | 否 | `4194304` | 单个响应体传输和解码后的字节数上限。 |

## 输出和恢复

源连接器输出 `resource`、`id`、`project_id`、`schedule_id`、`updated_at` 和 `record`，均为字符串。标识保持精确值。项目和日程上下文字段在适用时填充，待办和消息的项目标识来自记录的 bucket（若存在）。`record` 保留服务端 JSON 对象，包括富文本和嵌套字段。对日程行另外添加依据父关系生成的数值型 `project_id` 和对象型 `substream_data`。

待办和消息使用批次固定的时间范围，跨页保留具有相同更新时间的记录。保存点记录当前资源、父对象标识、经验证的 Link 查询及已输出的行位置。恢复时重新校验当前页及父项目的日程 dock，再继续输出。供应商接口不提供事务快照或删除历史。当前页变化时可能需要重新开始有界扫描。[Basecamp API 分页](https://github.com/basecamp/bc-api#pagination)

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Basecamp {
    account_id = "123456"
    user_agent = "Data export (integrations@example.com)"
    token = ${BASECAMP_ACCESS_TOKEN}
    start_date = "2025-06-01T00:00:00.000Z"
    resources = ["projects", "schedules", "schedule_entries", "todos", "messages"]
  }
}

sink {
  LocalFile {
    path = "/data/basecamp-export"
    file_format_type = "json"
  }
}
```

<ChangeLog />
