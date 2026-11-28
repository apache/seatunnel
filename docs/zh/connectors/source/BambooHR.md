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

import ChangeLog from '../changelog/connector-http-bamboohr.md';

# BambooHR

安装 `connector-http-bamboohr` 后，在 Source 并行度为 1 的批任务中使用 `BambooHR`。每个 Source 实例从公司专属域名的 BambooHR v1 HTTPS API 读取一种资源。配置公司子域名和有权读取对应资源的 [API 密钥](https://documentation.bamboohr.com/docs/getting-started)。

| 资源 | 记录 |
| --- | --- |
| `custom_reports_stream` | 即时自定义报表中的员工。须配置 `custom_reports_fields`。供应商包含在职和非在职员工，并可能省略未知或无权限的字段。[自定义报表接口](https://documentation.bamboohr.com/reference/request-custom-report) 已标记为弃用。 |
| `employees_directory_stream` | 公司已发布的员工目录。可见员工和字段取决于公司的目录共享配置。 |
| `meta_fields_stream` | 可用的员工字段定义，包括自定义字段别名。 |
| `time_off_requests_stream` | 休假日期与 API 查询日期区间相交的请求。存在 `created` 时，Source 同时按创建日期过滤。起止日期均包含当天。 |
| `timesheet_entries` | 公司日历日期区间内的打卡和按小时填报记录。按自然月查询，首尾可为不足一月的区间，起止日期均包含当天。 |
| `employees` | 按公司目录内的内部员工 ID 读取员工详情。`employee_fields` 指定要读取的字段。配置 `employee_changed_since` 时改为读取供应商的已变更员工 ID。 |

每行包含三个 `STRING` 字段。`data` 存储一条 JSON 对象，保留供应商字段名、嵌套值、数字和显式 null，未返回的字段仍保持缺省。`change_type` 和 `last_changed` 在快照读取时为 null。变更读取中，这两个字段分别记录供应商返回的 `Inserted`、`Updated` 或 `Deleted` 和变更时间。删除行的 `data` 只包含员工 ID。可访问公司目录中的员工条目并不代表可访问员工详情。供应商拒绝访问或查无该记录时，详情读取会明确失败。

## 选项

| 选项 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `company` | string | — | 必填，BambooHR 公司子域名，必须为单个 DNS 标签。 |
| `token` | string | — | 必填，API 密钥。作为 Basic 认证用户名，固定密码为 `x`。 |
| `resource` | string | — | 必填，以上资源名之一。 |
| `custom_reports_fields` | string | — | 自定义报表必填。逗号分隔的字段 ID 或字段名，最多 400 项。 |
| `employee_fields` | string | `firstName,lastName` | 员工详情需要读取的字段名、数字 ID 或自定义别名，逗号分隔，最多 400 项。 |
| `start_date` | string | 初始化时刻往前 30 天 23 小时 | 休假和工时记录的起始日期，包含当天。配置水位时按重读窗口计算。 |
| `end_date` | string | 初始化时的公司日期 | 休假和工时记录的结束日期，包含当天。 |
| `company_time_zone` | string | `UTC` | 与公司时区匹配的 Java 时区 ID，用于转换和校验日期。 |
| `time_off_watermark` | string | — | 可选，之前的休假申请创建日期。最多向前重读 365 天，受显式 `start_date` 限制。 |
| `timesheet_watermark` | string | — | 可选，之前的工时记录日期。最多向前重读 30 天，受 `start_date` 和供应商保留期限制。 |
| `employee_changed_since` | string | — | 可选，[已变更员工](https://documentation.bamboohr.com/reference/get-changed-employee-ids) 的起始时间，使用带明确时区偏移的 ISO 8601 时间戳。 |
| `connect_timeout_ms` | int | `10000` | TCP 连接超时，范围为 1–120000 毫秒。 |
| `read_timeout_ms` | int | `30000` | Socket 读取超时，范围为 1–120000 毫秒。 |
| `request_deadline_ms` | int | `60000` | 单次请求及其重试的总时限，范围为 1–300000 毫秒。 |
| `max_attempts` | int | `4` | 含首次请求在内的最大尝试次数，范围为 1–10。 |
| `max_response_bytes` | int | `8388608` | 响应压缩前后分别采用的大小上限，范围为 1024–33554432。 |
| `max_records_per_response` | int | `20000` | 单个响应的记录数或员工 ID 数上限，范围为 1–100000。 |

日期和水位选项支持 YYYY-MM-DD 或带时区偏移的 ISO 8601 时间戳。收到时间戳时，Source 按 `company_time_zone` 换算日期。工时记录优先使用供应商的 `date` 字段，缺省时以带偏移的 `start` 换算日期。如果两个日期都缺省，保留供应商在请求窗口中返回的行。工时查询最早从公司当前日期往前 364 天。

日期区间和可选员工变更截止时间在任务启动时固定。员工详情反映请求详情时的当前数据。供应商只返回每位员工的最近一次变更，可以通过重叠窗口同步，无法取得每次中间变更。

请求在总时限内按 `Retry-After` 重试。连接器校验证书和主机名，使用当前公司域名的 v1 路由，并拒绝重定向。恢复任务时应使用相同的供应商账号和权限。

Zeta 检查点只保存读取位置和摘要。恢复时如果正在读取的记录或员工 ID 列表已改变，Source 会明确失败，避免位置偏移。已经完成的资源在恢复后仍保持完成状态。新调度任务会计算新的区间。外部调度器若需要跨任务重读，请显式配置水位。六种资源均受权限限制。例如，在权限受限时，休假列表为空也可能是调用方无法查看相关员工的请求。

## 示例

```hocon
env {
    job.mode = "BATCH"
    parallelism = 1
}
source {
    BambooHR {
        company = "example"
        token = ${BAMBOOHR_TOKEN}
        resource = "employees"
        employee_fields = "firstName,lastName,department"
    }
}
sink {
    Console {}
}
```

<ChangeLog />
