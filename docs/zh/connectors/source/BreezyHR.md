<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Breezy HR Source

`BreezyHR` Source 从 Breezy HR v3 API 读取招聘数据。每次任务以源端并行度 1 对一家公司做有界扫描。发行包中缺少此连接器时，请安装 `connector-http-breezyhr`。

## 资源

| 资源 | API 操作 | 输出 |
| --- | --- | --- |
| `positions` | `GET /v3/company/{company_id}/positions` | 每个岗位一行。供应商默认排除已归档岗位。 |
| `candidates` | `GET /v3/company/{company_id}/position/{position_id}/candidates` | 每个已发现岗位下的每个候选人一行。即使不选择输出 `positions`，也会查询岗位以发现候选人。 |
| `pipelines` | `GET /v3/company/{company_id}/pipelines` | 响应字典中的每项一行，保留字典键和完整管道对象及其中有序的阶段。 |

在 **My Settings → API Keys** 创建个人访问令牌，也可以使用已签发的会话令牌。将 `GET /v3/companies` 返回的公司 `_id` 填入 `company_id`。连接器直接将令牌用作 `Authorization` 值。令牌所属用户须有公司访问权限，并能读取扫描范围内各岗位的候选人。岗位可见时，候选人接口仍可能拒绝访问。遇到拒绝时，任务会报告相应操作及 HTTP 状态并失败。 [认证](https://developer.breezy.hr/reference/authorization) · [岗位](https://developer.breezy.hr/reference/listpositions) · [候选人](https://developer.breezy.hr/reference/listcandidates) · [招聘管道](https://developer.breezy.hr/reference/listpipelines)

## 配置

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `token` | 是 | | 不带前缀的个人访问令牌或会话令牌。 |
| `company_id` | 是 | | 公司 API 标识，最多 128 个英文字母、数字、下划线或连字符。 |
| `resources` | 是 | | 从上表选择一个或多个资源名，每个资源只出现一次。 |
| `page_size` | 否 | 50 | 岗位和候选人请求的页大小，范围 1–50。 |
| `max_pages_per_scope` | 否 | 1000 | 岗位列表和每个岗位的候选人列表各自允许的成功页数，包含最终空页。范围 1–10000。 |
| `max_requests` | 否 | 100000 | 本次扫描允许完整处理的成功响应数，范围 1–1000000。 |
| `max_attempts` | 否 | 4 | 每页最大请求次数，含首次请求，范围 1–10。 |
| `request_timeout_ms` | 否 | 30000 | 单页请求的总期限，包含重试与解析。范围 100–300000 毫秒。 |
| `max_response_bytes` | 否 | 8388608 | 解压后响应体上限，单位字节，范围 1024–67108864。 |
| `endpoint` | 否 | `https://api.breezy.hr` | HTTPS 源地址。显式配置时需提供 `trust_certificate`。 |
| `trust_certificate` | 否 | | 自定义 HTTPS 源的 X.509 PEM 文件，路径位于 Worker 本地。 |

## 输出

`breezyhr.records` 表包含以下列，类型均为 `STRING`。

| 列 | 说明 |
| --- | --- |
| `resource` | 选中的资源名。 |
| `record_id` | 岗位或候选人的字符串 `_id`，或招聘管道在响应字典中的键。同一候选人 ID 可出现在多个岗位。 |
| `position_id` | 岗位自身 ID 或候选人所属岗位 ID。管道行为空。 |
| `position_type` | 岗位的 `type.name`，若存在。 |
| `country_id` | 岗位的 `location.country.id`，若存在。 |
| `country_name` | 岗位的 `location.country.name`，若存在。 |
| `stage_name` | 候选人的 `stage.name`，若存在。 |
| `payload` | 完整供应商对象的 JSON 文本，保留嵌套值、额外字段和数值精度。 |

未提供或不适用的投影字段为 null。为同一家公司的候选人在下游建立键时，请同时使用 `position_id` 和 `record_id`。候选人摘要可能包含联系方式及简历 URL，源端读取摘要并把 URL 作为数据保留。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  BreezyHR {
    company_id = "6a1ef2bc42b0"
    token = ${BREEZY_HR_TOKEN}
    resources = ["positions", "candidates", "pipelines"]
  }
}

sink {
  Console {}
}
```

## 分页与恢复

岗位和候选人从第 1 页开始，按 `updated` 排序。收到不足页大小的结果后继续查询，直到该列表返回空页。每个岗位的候选人独立分页。管道字典一次读取，最多 10000 项，同时受响应体大小限制。

检查点保存当前请求、待遍历岗位 ID、分页识别信息和页内已发记录数。页内恢复会重新请求该页，校验页面身份和内容后发送剩余记录。变更令牌、公司、资源选择、源地址、证书路径或页大小后，需要重新扫描。源端会拒绝页内重复、相邻页重叠和此前已返回的整页。Breezy HR 的页码接口没有快照隔离，源数据在扫描期间变动仍可影响列表成员和顺序。连接器提供支持检查点的有界读取。

HTTPS 客户端校验证书和主机名，并禁止重定向。符合条件的 GET 失败会在尝试次数和期限内重试。HTTP 429 必须提供可用的 `Retry-After` 或 `X-RateLimit-Reset`，采用两者中较长的等待时间。供应商文档将重置时间描述为时间戳，未指定数值单位。连接器支持 10 位 Unix 秒、13 位 Unix 毫秒和显式日期格式。等待时间无效或超过本页期限时，请求失败。 [Breezy HR 限流说明](https://developer.breezy.hr/reference/rate-limiting)
