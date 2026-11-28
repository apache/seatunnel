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

import ChangeLog from '../changelog/connector-http-brevo.md';

# Brevo

安装 `connector-http-brevo`，每个 `Brevo` 源配置块选择一个资源，源并行度设为 `1`。工作区 API 密钥须具有所选资源及其父资源的读取权限。连接器通过 HTTPS 访问 `api.brevo.com`。[Brevo API 文档](https://developers.brevo.com/reference/quickstart-reference)

## 资源

| `resource` | Brevo GET 路径 | 记录 | 默认及最大页大小 |
| --- | --- | --- | --- |
| `contacts` | `/v3/contacts` | 联系人 | `1000` |
| `contacts_attributes` | `/v3/contacts/attributes` | 联系人属性定义 | — |
| `contacts_folders_lists` | `/v3/contacts/folders/{folderId}/lists` | 每个联系人文件夹下的列表 | `50` |
| `contacts_folders` | `/v3/contacts/folders` | 联系人文件夹 | `50` |
| `contacts_segments` | `/v3/contacts/segments` | 联系人分组 | `50` |
| `contacts_lists_contacts` | `/v3/contacts/lists/{listId}/contacts` | 每个已发现列表中的联系人 | `500` |
| `contacts_lists` | `/v3/contacts/lists` | 联系人列表 | `50` |
| `senders` | `/v3/senders` | 邮件发件人 | — |
| `companies` | `/v3/companies` | CRM 公司 | `500` |
| `companies_attributes` | `/v3/crm/attributes/companies` | 公司属性定义 | — |
| `crm_pipeline_stages` | `/v3/crm/pipeline/details` | 默认销售管道阶段 | — |
| `crm_pipeline_details_all` | `/v3/crm/pipeline/details/all` | 所有销售管道及阶段 | — |
| `crm_attributes_deals` | `/v3/crm/attributes/deals` | 交易属性定义 | — |
| `crm_deals` | `/v3/crm/deals` | CRM 交易 | `500` |
| `crm_tasktypes` | `/v3/crm/tasktypes` | CRM 任务类型 | — |
| `crm_tasks` | `/v3/crm/tasks` | CRM 任务 | `50` |
| `crm_notes` | `/v3/crm/notes` | CRM 备注 | `50` |
| `domains` | `/v3/senders/domains` | 发件人域名 | — |
| `webhooks` | `/v3/webhooks` | 默认事务类 webhook | — |
| `account` | `/v3/account` | 单个工作区账号对象 | — |
| `organization_invited_users` | `/v3/organization/invited/users` | 受邀用户 | — |
| `emailCampaigns` | `/v3/emailCampaigns` | 邮件营销活动 | `100` |
| `smsCampaigns` | `/v3/smsCampaigns` | 短信营销活动 | `1000` |
| `inbound_email_events` | `/v3/inbound/events` | 入站邮件事件 | `500` |
| `transactional_email_events` | `/v3/smtp/statistics/events` | 事务邮件事件 | `5000` |
| `processes` | `/v3/processes` | 后台处理任务 | `50` |
| `products` | `/v3/products` | 商品目录 | `1000` |
| `categories` | `/v3/categories` | 目录分类 | `100` |
| `email_templates` | `/v3/smtp/templates` | 事务邮件模板 | `1000` |

Brevo 原名 Sendinblue。资源名 `campaigns` 对应 `emailCampaigns`，`templates` 对应 `email_templates`，`contacts` 继续使用原名称。三个资源均访问当前 Brevo REST 服务。

如果工作区尚无 CRM 任务类型，Brevo 可能会在首次读取列表时初始化默认类型。

## 参数

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `token` | string | 必填 | 工作区 API 密钥，通过 `api-key` 请求头传递。 |
| `resource` | string | 必填 | 资源表中的一个导出名称。 |
| `start_date` | string | 无 | 包含起始时刻的 ISO-8601 时间，须带时区，精度不超过毫秒。两个邮件事件报表均必填。 |
| `end_date` | string | 无 | 包含结束时刻的 ISO-8601 时间。如果只配置起始时间，任务首次开始时固定结束时间。 |
| `page_size` | int | 资源默认值 | 所选分页接口每次请求的行数，范围为 1 至资源表所列上限。父资源发现使用自己的默认值。 |
| `max_pages` | int | `2000` | 每个编号分页序列允许的最大请求次数，包含用于确认结束的空页。范围 1–20000。 |
| `connect_timeout_ms` | int | `10000` | TCP 连接超时，单位毫秒。范围 1–120000。 |
| `read_timeout_ms` | int | `30000` | 套接字读取超时，单位毫秒。范围 1–120000。 |
| `request_deadline_ms` | int | `60000` | 单次请求及重试的总时限，单位毫秒。范围 1–300000。 |
| `max_attempts` | int | `4` | 可重试响应的最大尝试次数，包含首次请求。范围 1–10。 |
| `max_response_bytes` | int | `8388608` | gzip 解压前后各自允许的最大响应字节数。范围 1024–33554432。 |
| `tls_trust_certificate` | string | JVM 信任库 | 引擎本地 PEM 或 DER 文件，可包含一个或多个可信 X.509 证书。连接器使用该信任库并校验供应商主机名。 |

## 时间和分页

两个邮件事件报表均按连续的 30 天窗口读取。入站邮件使用毫秒时间戳。事务邮件接口按 UTC 日历日期取数，包含首尾日，连接器再精确筛选时间戳。其余有时间字段的资源支持全量或按区间读取。时间参数适用于联系人、列表联系人、公司、交易、CRM 任务及备注、营销活动、邮件事件、商品及分类。其他资源不接受时间参数。

联系人、公司、交易、商品、分类使用供应商记录的修改时间过滤能力，并在请求下界向前取一毫秒，保留恰好等于起始时刻的记录。营销活动、CRM 任务及备注按记录实际修改时间在本地筛选，完整遍历原始页面。[联系人接口](https://developers.brevo.com/reference/get-contacts) · [CRM 任务](https://developers.brevo.com/reference/get-all-tasks) · [CRM 备注](https://developers.brevo.com/reference/get-all-notes)

偏移量按原始记录数推进，公司编号分页从 1 开始。父资源遍历不受子资源时间区间影响。发件人域名接口未公开续页请求参数。如果响应表明仍有后续页，连接器会明确报错，避免将部分导出报告为成功。[发件人域名请求](https://developers.brevo.com/docs/domain-creation-and-management)

## 输出与恢复

| 字符串字段 | 值 |
| --- | --- |
| `entity_id` | 供应商提供的记录标识。事务邮件的消息 ID 可能对应多个事件，每个事件均保留。 |
| `parent_id` | `contacts_folders_lists` 的父文件夹 ID 或 `contacts_lists_contacts` 的父列表 ID。其他资源为 null。 |
| `modified_at` | 对应资源的更新时间或事件时间，规范为 UTC ISO-8601。没有可用时间时为 null。 |
| `data` | 完整 JSON 记录，保留嵌套字段与数值。子资源包含数值型 `folderId` 或 `listId`。公司和交易在可用时通过 `last_updated_at` 提供原始修改时间。 |

检查点保存当前时间窗、父资源位置、页号及行偏移量。恢复时会先核对当前页顺序与内容，再跳过已输出的记录。保存位置的数据发生变化时会明确失败。供应商接口后续页面仍可能变动，连接器无法提供同一时刻的完整快照。

连接器校验 TLS，只向固定 Brevo 主机发送密钥，拒绝重定向和 HTTP 200 错误对象。临时错误在总时限内重试，HTTP 429 依据供应商限流响应头等待。[Brevo 限流响应头](https://developers.brevo.com/docs/limit-headers)

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Brevo {
    token = ${BREVO_API_KEY}
    resource = "contacts_lists_contacts"
    start_date = "2024-01-01T00:00:00Z"
    end_date = "2024-01-31T23:59:59.999Z"
    page_size = 250
  }
}
sink {
  Console {}
}
```

<ChangeLog />
