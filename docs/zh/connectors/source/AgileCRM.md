import ChangeLog from '../changelog/connector-http-agilecrm.md';

# AgileCRM

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

`AgileCRM` Source 从 Agile CRM REST API 读取一种资源，每个作业执行一次有界扫描。请安装 `connector-http-agilecrm`，并使用有权读取目标资源及相关父级列表的账户邮箱和 API 密钥。 [Agile CRM REST API](https://github.com/agilecrm/rest-api)

## 配置项

| 名称 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `base_url` | String | 是 | | 受信任的租户源地址，例如 `https://example.agilecrm.com`，不含路径、查询、片段或凭据。 |
| `username` | String | 是 | | 用于 Basic 认证的 Agile CRM 账户邮箱。 |
| `password` | String | 是 | | 用于 Basic 认证的 Agile CRM API 密钥。 |
| `resource` | String | 是 | | 从下表选择一个资源。 |
| `page_size` | Integer | 否 | 100 | 游标分页的请求大小，范围 1 到 100。`tickets` 的请求大小上限为 25。 |
| `event_start` | Long | `events` 必填 | | 请求的事件窗口起点，使用非负 Unix 秒。 |
| `event_end` | Long | `events` 必填 | | 请求的事件窗口终点，使用 Unix 秒，必须大于 `event_start`。 |
| `allow_insecure_http` | Boolean | 否 | false | 对显式信任的隔离端点启用明文 HTTP。 |
| `request_timeout_ms` | Integer | 否 | 30000 | 单次请求总时限，含重试等待，范围为 1 到 300000 毫秒。 |
| `max_attempts` | Integer | 否 | 4 | 单次请求的最大 HTTP 尝试次数，范围为 1 到 8。 |
| common-options | | 否 | | [Source 通用参数](../common-options/source-common-options.md)。 |

## 资源

| 资源 | 读取方式 |
| --- | --- |
| `contacts` | 联系人列表，支持游标分页。 |
| `companies` | 只读列表 POST，表单携带分页大小、排序键和游标。 |
| `deals` | 商机列表，支持游标分页。 |
| `notes` | 每个已发现联系人的备注。 |
| `tasks` | 任务列表。 |
| `events` | 指定时间窗口的事件。 |
| `milestone` | 销售流程及其阶段。 |
| `campaigns` | 活动列表，支持游标分页，也接受单个活动对象。 |
| `documents` | 每个已发现联系人的文档元数据，附件链接随元数据输出。 |
| `ticket_filters` | 可见的工单筛选器。 |
| `tickets` | 通过官方文档所述的列表接口读取每个筛选器下的工单。 |

分页游标来自末条记录。返回条数小于请求大小时仍可继续。有空数组或末条记录的游标缺失、为 null 或为空字符串时结束。连接器从无额外过滤的父级列表发现和去重父 ID。子记录保留所属父 ID，因此同一工单在不同筛选器下可能分别出现。

工单接口文档未说明如何请求续页。每个筛选器只接受未达到请求大小且没有续页标记的结果。如果响应包含续页标记或达到请求大小，读取失败，避免接受可能被截断的结果。文档链接随记录输出，连接器不下载文件。

每行包含 `resource STRING`、可空 `record_id STRING`、可空 `parent_id STRING` 及 `payload STRING`。数据字段保留完整 JSON 对象，包括嵌套值及数值精度。备注和文档的父 ID 指向联系人，工单的父 ID 指向筛选器。输出可能包含客户信息。

## 读取与恢复

源端并行度设为 1。每个新作业从列表起点执行有界扫描。检查点保存遍历位置、ID 和游标，不含凭据及记录内容。恢复时需要保持相同的源地址、资源、分页大小和事件范围，可以更新 API 密钥。服务端游标过期或数据变动时，恢复可能失败，最近一次完成检查点之后输出的记录也可能重复。

响应压缩前后均不得超过 8 MiB，单页最多 10000 条记录。每个列表最多 10000 页，每次扫描最多 200000 个已接收页面和 100000 个父级 ID。发现重复游标时该页不输出记录。

默认使用 HTTPS 验证证书和主机名。重定向、身份认证错误和权限错误会直接终止。仅收到 HTTP 429、500、502、503 和 504 时可在限定次数内重试，包含只读公司列表。服务端 `Retry-After` 指定的最短等待时间必须落在请求总时限内。

## 示例

```hocon
env {
  job.mode = BATCH
  parallelism = 1
}

source {
  AgileCRM {
    base_url = "https://example.agilecrm.com"
    username = ${?AGILE_CRM_EMAIL}
    password = ${?AGILE_CRM_API_KEY}
    resource = "documents"
    page_size = 50
  }
}

sink {
  Console {}
}
```

## 变更日志

<ChangeLog />
