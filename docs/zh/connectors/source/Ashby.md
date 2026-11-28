import ChangeLog from '../changelog/connector-http-ashby.md';

{/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */}

# Ashby

每个 Ashby Source 读取一项招聘资源，并行度为 1。安装 `connector-http-ashby` 后，使用 `Ashby` 作为 Source 标识。请为 API 密钥授予资源表中的读取权限。连接器通过 TLS 使用 Basic 认证，以密钥作为用户名，密码为空。 [Ashby 认证](https://developers.ashbyhq.com/reference/authentication)

## 资源

| 资源 | Ashby POST 路径 | 读取权限 | 读取方式 |
| --- | --- | --- | --- |
| `application_criteria_evaluations` | `/application.listCriteriaEvaluations` | `candidatesRead` | 分页发现申请，再按申请分页 |
| `application_feedback` | `/applicationFeedback.list` | `candidatesRead` | 游标和同步令牌 |
| `application_history` | `/application.listHistory` | `candidatesRead` | 同步申请，再按申请分页 |
| `applications` | `/application.list` | `candidatesRead` | 游标和同步令牌 |
| `approvals` | `/approval.list` | `approvalsRead` | 游标和同步令牌 |
| `archive_reasons` | `/archiveReason.list` | `hiringProcessMetadataRead` | 单次响应 |
| `candidate_tags` | `/candidateTag.list` | `hiringProcessMetadataRead` | 游标和同步令牌 |
| `candidates` | `/candidate.list` | `candidatesRead` | 游标和同步令牌 |
| `custom_fields` | `/customField.list` | `hiringProcessMetadataRead` | 游标和同步令牌 |
| `departments` | `/department.list` | `organizationRead` | 游标和同步令牌 |
| `feedback_form_definitions` | `/feedbackFormDefinition.list` | `feedbackFormDefinitionsRead` 或 `hiringProcessMetadataRead` | 游标和同步令牌 |
| `interview_plans` | `/interviewPlan.list` | `interviewsRead` | 游标和同步令牌 |
| `interview_schedules` | `/interviewSchedule.list` | `interviewsRead` | 游标和同步令牌 |
| `interview_stage_groups` | `/interviewStageGroup.list` | `interviewsRead` | 单次响应 |
| `interview_stages` | `/interviewStage.list` | `interviewsRead` | 分页发现面试计划，每个计划返回一次阶段结果 |
| `interviewer_pools` | `/interviewerPool.list` | `hiringProcessMetadataRead` | 游标和同步令牌 |
| `interviews` | `/interview.list` | `interviewsRead` | 游标和同步令牌 |
| `job_boards` | `/jobBoard.list` | `jobsRead` | 单次响应 |
| `job_postings` | `/jobPosting.list` | `jobsRead` | 单次响应 |
| `job_templates` | `/jobTemplate.list` | `jobsRead` | 游标和同步令牌 |
| `jobs` | `/job.list` | `jobsRead` | 游标和同步令牌 |
| `locations` | `/location.list` | `organizationRead` | 游标和同步令牌 |
| `offers` | `/offer.list` | `offersRead` | 游标和同步令牌 |
| `openings` | `/opening.list` | `jobsRead` | 游标和同步令牌 |
| `projects` | `/project.list` | `candidatesRead` 或 `projectsRead` | 游标和同步令牌 |
| `source_tracking_links` | `/sourceTrackingLink.list` | `hiringProcessMetadataRead` | 游标和同步令牌 |
| `sources` | `/source.list` | `hiringProcessMetadataRead` | 单次响应 |
| `survey_form_definitions` | `/surveyFormDefinition.list` | `hiringProcessMetadataRead` | 游标和同步令牌 |
| `users` | `/user.list` | `organizationRead` | 游标和同步令牌 |

## 输出

每条结果输出一条 INSERT 记录。`payload` 保留供应商对象，便于下游使用候选人字段、精确数值、空值和嵌套记录。Ashby 返回归档或删除信息时，其字段也会保留。可依据这些字段和下游的数据策略执行合并或删除。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `resource` | STRING | 配置的资源名称。 |
| `record_id` | STRING | 供应商结果 ID。条件评估使用 `applicationId/id`。 |
| `parent_id` | STRING | 子记录所属的申请或面试计划 ID。直接读取的记录为 null。 |
| `payload` | STRING | UTF-8 JSON 对象。申请历史补充申请 ID、状态和时间，条件评估补充申请 ID，面试阶段补充计划归档标志。 |

## Source 参数

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `resource` | String | — | 必填。从资源表选择一项。 |
| `token` | String | — | 必填。具有所选资源读取权限的 API 密钥。 |
| `start_date` | String | — | 可选的 RFC 3339 起始时间，最多精确到毫秒。适用于申请、申请反馈、面试安排和申请历史。 |
| `sync.token` | String | — | 可选。此前从相同账户、资源和过滤条件取得的最终同步令牌。申请历史使用申请列表的令牌。JSON 配置中使用 `sync` 对象的 `token` 字段。 |
| `poll_interval_ms` | Integer | `0` | 0 表示读取一轮。100 至 86,400,000 毫秒可为原生同步资源和申请历史开启持续同步。 |
| `page_size` | Integer | `100` | 支持 limit 的接口所请求的页大小。范围为 1 至 100。 |
| `max_requests_per_pass` | Integer | `100000` | 单轮逻辑请求上限，包含父级发现和子页请求。范围为 1 至 1,000,000。 |
| `application_child_interval_ms` | Integer | `600` | 同一读取器的申请历史或条件评估 HTTP 请求间隔下限。范围为 0 至 30,000 毫秒。 |
| `max_response_bytes` | Integer | `8388608` | 每个成功响应解压后的字节上限。范围为 1,024 至 67,108,864。 |
| `max_attempts` | Integer | `4` | 每个可重试读取请求的尝试上限。范围为 1 至 10。 |
| `request_timeout_ms` | Integer | `30000` | 单个请求的总时限，包含响应体读取、限速等待和全部重试。范围为 100 至 300,000 毫秒。 |
| `endpoint` | String | `https://api.ashbyhq.com` | 可选的自定义 HTTPS 源站，只允许根路径，不含查询、片段或凭据。需同时设置 trust_certificate。 |
| `trust_certificate` | String | — | 自定义源站的工作节点本地 PEM 信任证书。连接时仍校验主机名。 |

## 日期、增量同步与恢复

- `applications`、`application_feedback` 和 `interview_schedules` 将 `start_date` 按毫秒传为 Ashby 的创建时间过滤条件。申请记录包含 `updatedAt` 时，还需满足更新时间大于或等于该日期。读取申请历史时，先在无创建时间限制的情况下发现父申请，再按 `updatedAt` 筛选。缺少更新时间的结果继续保留。
- 批作业读取一轮。流作业在输出当前轮最终响应并等待轮询间隔后开始下一轮。申请历史需要输出所有历史子页后才推进申请列表的同步令牌。后续轮次只能读取 Ashby 通过父申请变更接口返回的申请历史。
- 检查点保存当前轮次、游标和父级位置。页内恢复时会重新请求该页，只有响应摘要相同才从已保存的输出偏移继续。响应变化或恢复配置改变了读取范围时，作业会失败。需要重新全量读取时，请按下游键与 Sink 语义处理可能重复的记录。
- Ashby 文档说明游标与同步令牌有效期为 14 天。状态失效、增量结果超过供应商允许范围或续页信息矛盾时，作业会明确报错。请按应用敏感数据保护检查点。日常使用默认 API 源站。配置自定义源站时，请确认该源站可信，再向其提供 API 密钥。 [Ashby 分页与增量同步](https://developers.ashbyhq.com/docs/pagination-and-incremental-sync)

## 示例

下面配置按批读取申请。将 `job.mode` 设置为 `STREAMING`，并指定大于零的 `poll_interval_ms`，即可持续同步。

```hocon
env {
  job.mode = BATCH
  parallelism = 1
}

source {
  Ashby {
    resource = "applications"
    token = ${ASHBY_TOKEN}
    start_date = "2025-01-01T00:00:00Z"
    plugin_output = "ashby_rows"
  }
}

sink {
  Console {
    plugin_input = "ashby_rows"
  }
}
```

<ChangeLog />
