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

import ChangeLog from '../changelog/connector-http-apptivo.md';

# Apptivo

安装 `connector-http-apptivo`，在批处理任务中使用 `Apptivo`。在 **Business Settings → API Access** 获取企业的 API Key 和 Access Key。连接器将两个密钥编码为查询参数并发送到配置的 HTTPS 源站。有专用部署地址时，可指定受信任的 HTTPS 源站。[Apptivo 开发指南](https://www.apptivo.com/developer-api/) · [API 权限与认证](https://www.apptivo.com/developer-api/getting-started/)

## 资源

| 资源 | DAO 操作 | 行 ID |
| --- | --- | --- |
| `customers` | `getAll` | `customerId` |
| `contacts` | `getAll` | `contactId` |
| `cases` | `getConfigData` | `case-config` |
| `leads` | `getAll` | `id` |
| `opportunities` | `getAll` | `opportunityId` |

四个 `getAll` 资源从零开始分页读取。Apptivo 返回分页计数时，连接器检查计数是否与当前页一致。`cases` 输出一个配置对象，包含状态、类型、优先级和负责人等设置。[客户列表](https://www.apptivo.com/developer-api/customers-api-reference/getallcustomers/) · [案例配置](https://www.apptivo.com/developer-api/cases-api-reference/getcasesconfigdata/)

## 参数

| 参数 | 类型 | 默认值 | 用途 |
| --- | --- | --- | --- |
| `token` | 字符串 | 必填 | Apptivo 企业 API Key |
| `access_key` | 字符串 | 必填 | Apptivo 企业 Access Key |
| `firm_id` | 字符串 | 无 | 期望的 Apptivo 企业正整数 ID，用于将检查点绑定到指定企业 |
| `resources` | 字符串列表 | 全部五项 | 按指定顺序读取的资源 |
| `api_origin` | 字符串 | `https://api.apptivo.com` | 固定 HTTPS 源站，不含路径或查询参数 |
| `tls_trust_certificate` | 字符串 | JVM 信任配置 | 本连接器使用的可选 X.509 信任证书，支持 PEM 或 base64 DER。仍进行 HTTPS 主机名校验。 |
| `page_size` | 整数 | 100 | 每页请求的记录数，范围 1 至 100 |
| `max_pages_per_resource` | 整数 | 1000 | 每个集合允许读取的最大页数 |
| `request_timeout_ms` | 整数 | 30000 | 单次 HTTPS 尝试的总超时 |
| `operation_timeout_ms` | 整数 | 120000 | 单个请求包含重试的总超时 |
| `min_request_interval_ms` | 整数 | 100 | HTTPS 尝试之间的最短时间 |
| `max_retries` | 整数 | 3 | 临时 HTTP 状态或 GET 连接故障的额外重试次数 |
| `max_response_bytes` | 整数 | 4194304 | 分别限制传输和解压后的 JSON 字节数 |

## 输出与恢复

输出三列字符串 `resource`、`id` 和 `record`。`record` 保留 Apptivo 完整 JSON 对象，包括嵌套字段和自定义字段。遇到提供方返回的 `FAILURE` 响应时，任务以不含凭据的诊断信息终止。

一个活跃读取器按资源顺序扫描。检查点记录资源、页偏移量、已接收记录位置、分页内容指纹，以及可用的供应商企业 ID。恢复时使用同一企业的密钥。Apptivo 提供 `firmId` 时，连接器会核对企业，并核对页内恢复位置对应的分页内容。内容改变时，恢复以错误终止。新建任务会重新读取所选集合。Apptivo 可能在读取期间更新记录，目标端交付语义取决于 Sink。

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Apptivo {
    token = ${APPTIVO_API_KEY}
    access_key = ${APPTIVO_ACCESS_KEY}
    resources = ["customers", "contacts", "cases", "leads", "opportunities"]
  }
}

sink {
  Console {}
}
```

通过任务配置的密钥注入机制提供两个密钥。

<ChangeLog />
