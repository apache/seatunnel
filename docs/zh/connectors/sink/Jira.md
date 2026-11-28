import ChangeLog from '../changelog/connector-http-jira.md';

# Jira

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

> Jira Cloud 目标连接器

## 概述

通过 Jira Cloud REST API v3 创建、编辑或删除问题单。每个 Sink 配置选择一种操作，每条输入记录单独执行。Jira Sink 随 `connector-http-jira` 插件提供。

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [CDC](../../introduction/concepts/connector-v2-features.md)
- [ ] [多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## Sink 选项

| 参数 | 类型 | 是否必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `base_url` | String | 是 | | 可信的 Jira Cloud 站点地址，例如 `https://example.atlassian.net`。允许端口和末尾斜杠。地址中不得包含 API 路径、用户信息、查询参数或片段。 |
| `api_version` | Integer | 否 | 3 | Jira REST API 版本，仅支持 `3`。 |
| `username` | String | 是 | | 用于 HTTP Basic 认证的 Atlassian 账号邮箱。 |
| `token` | String | 是 | | 用于直接访问 Jira Cloud 站点的 Atlassian API 令牌。共享配置中请使用密钥或变量替换。 |
| `operation` | Enum | 是 | | 取值为 `CREATE`、`UPDATE` 或 `DELETE`。 |
| `allow_insecure_http` | Boolean | 否 | false | 仅在显式信任的隔离开发端点启用明文 HTTP。访问 Jira Cloud 时请保持默认值。 |
| `request_timeout_ms` | Integer | 否 | 30000 | 每条记录从建立连接、请求、响应到重试等待的总时限，取值 1–300000 毫秒。 |
| `max_retries` | Integer | 否 | 2 | Jira 以 HTTP 429 拒绝 UPDATE 且返回有效的 `Retry-After` 时的最大重试次数，取值 0–10。 |
| common-options | | 否 | | [Sink 通用选项](../common-options/sink-common-options.md)。 |

## 输入 Schema

所有记录都必须为 `INSERT` 类型。所配置的操作决定输入字段，字段顺序不限。

| 操作 | 必需的 STRING 字段 | Jira 请求 |
| --- | --- | --- |
| `CREATE` | `fields` | `POST /rest/api/3/issue`。在 `fields` 中提供 `project`、`issuetype` 对象和非空 `summary`。Jira 可能根据项目或问题类型要求其他字段。 |
| `UPDATE` | `issue_id_or_key`、`fields` | `PUT /rest/api/3/issue/{issueIdOrKey}`。提供问题 ID 或 Key，以及要替换的字段。 |
| `DELETE` | `issue_id_or_key` | `DELETE /rest/api/3/issue/{issueIdOrKey}?deleteSubtasks=false`。如果问题还有子任务，Jira 会拒绝删除。 |

`fields` 是非空 JSON 对象，使用 Jira 字段名称和值格式，例如 `{"priority":{"id":"3"},"assignee":null}`。连接器会将显式 JSON null 传给 Jira，由 Jira 校验该字段是否允许清空。描述、环境和多行文本自定义字段采用 Atlassian Document Format。JSON 请求体上限为 1 MiB，输入 `fields` 对象最多嵌套 64 层，且不允许重复键。项目权限和字段格式参见 [Jira Issue API](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issues/)。

Basic 认证需使用可直接访问站点的 API 令牌，参见 [Atlassian 认证文档](https://developer.atlassian.com/cloud/jira/platform/basic-auth-for-rest-apis/)。连接器支持 Jira Cloud REST v3 的单条问题操作。REST v2、Data Center、网关范围令牌、OAuth、批量请求、工作流流转和添加评论等专用操作不在支持范围内。

## 写入确认与重放

CREATE 要求 HTTP 201 和有效的问题 ID、Key。UPDATE 和 DELETE 要求无响应体的 HTTP 204。写入器等待确认或错误后才继续处理下一条记录。检查点记录引擎进度。Jira 变更即时生效，SeaTunnel 无法回滚已执行的操作。

仅在 Jira 返回 HTTP 429 和有效的 `Retry-After` 时重试 UPDATE。等待时间达到 Jira 指定的最小值，且重试必须处于记录的总时限内。超时、传输失败、5xx 响应和无效成功响应均会使作业失败。Jira 仍可能已经执行操作。遇到此类故障，请先核对 Jira 再重启作业。引擎重试或数据重放可能重发已经执行的操作，重复执行 CREATE 会创建重复问题单。同一问题上的操作需要顺序执行时，请使用单个写入器。在 Zeta 中，若需人工核对后才能重放，可设置 `job.retry.times = 0`。

## 示例

```hocon
env {
  job.mode = BATCH
  parallelism = 1
  job.retry.times = 0
}

source {
  FakeSource {
    schema = { fields { fields = string } }
    rows = [
      {
        kind = INSERT
        fields = ["""{"project":{"key":"DEMO"},"issuetype":{"name":"Task"},"summary":"检查夜间导入"}"""]
      }
    ]
  }
}

sink {
  Jira {
    base_url = "https://example.atlassian.net"
    username = "${jira_username}"
    token = "${jira_token}"
    operation = CREATE
  }
}
```

## 更新日志

<ChangeLog />
