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

# Campayn Source

`Campayn` 通过 Campayn v1 API 有界读取一个租户，源端并行度为 1。发行包中缺少此连接器时，请安装 `connector-http-campayn`。

| 资源 | API 操作 |
| --- | --- |
| `lists` | `GET /api/v1/lists.json` |
| `forms` | `GET /api/v1/lists/{list_id}/forms.json` |
| `contacts` | `GET /api/v1/lists/{list_id}/contacts.json` |
| `emails` | `GET /api/v1/emails.json` |
| `reports` | `GET /api/v1/reports/calendar.json` |

在 Campayn 账户中获取 API 密钥。连接器为每个 GET 发送 `Authorization: TRUEREST apikey=<token>`，并由 `sub_domain` 确定租户 HTTPS 主机。读表单或联系人时会先发现可访问的名单，子资源保留真实名单 ID。即使未选择输出 `lists`，也会执行该查询。日历报表读取供应商返回的所有日期的已发送和已排期记录，尚未生成报告的 `report_url` 可以为 null。表单 HTML、预览和报告链接作为数据保留。 [Campayn API](https://campayn.com/api) · [接口文档](https://github.com/nebojsac/Campayn-API) · [日历报表](https://github.com/nebojsac/Campayn-API/blob/master/endpoints/reports.md)

## 配置

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `token` | 是 | | 不带认证前缀的 Campayn API 密钥。 |
| `sub_domain` | 是 | | `campayn.com` 下的单个 DNS 标签，最多 63 个英文字母、数字或连字符，首尾须为字母或数字。 |
| `resources` | 是 | | 从上表选择一个或多个资源，每个名称只出现一次。 |
| `max_lists` | 否 | 10000 | 遍历子资源时保存的名单 ID 上限，范围 1–50000。 |
| `max_requests` | 否 | 100000 | 扫描中允许处理的成功列表响应数，范围 1–1000000。 |
| `max_attempts` | 否 | 4 | 每个列表的 HTTPS 总尝试次数，范围 1–10。 |
| `request_timeout_ms` | 否 | 30000 | 单个列表的请求总期限，含重试和解析，范围 100–300000 毫秒。 |
| `max_response_bytes` | 否 | 8388608 | 单个列表解码后的响应大小，范围 1024–67108864 字节。 |
| `endpoint` | 否 | `https://<sub_domain>.campayn.com` | 可选 HTTPS 源地址，显式配置时需提供 `trust_certificate`。 |
| `trust_certificate` | 否 | | 自定义 HTTPS 源的 Worker 本地 X.509 PEM 文件路径。 |

## 输出

`campayn.records` 表包含五个 `STRING` 列。

| 列 | 说明 |
| --- | --- |
| `resource` | 资源名。 |
| `record_id` | 文本形式的供应商 ID。 |
| `sub_domain` | 转成小写的租户标签。 |
| `list_id` | 名单自身 ID，或表单和联系人所属的名单 ID。邮件和报表为 null。 |
| `payload` | 完整供应商记录的 JSON 文本，保留可选字段、嵌套值和数值精度。 |

表单和联系人 ID 的范围是所属名单，同一 ID 可能出现在不同名单中。若表单返回的名单 ID 与所请求的名单矛盾，连接器会在输出该列表前报错。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Campayn {
    token = ${CAMPAYN_API_TOKEN}
    sub_domain = "your-tenant"
    resources = ["lists", "forms", "contacts", "emails", "reports"]
  }
}

sink {
  Console {}
}
```

## 列表限制与恢复

已文档化的五个列表接口均返回完整的根数组，未提供续页参数。连接器校验整个数组后输出，每个列表最多处理 50000 条，同时受配置的解码大小限制。如果响应带有意外的续页头，该列表请求失败。

检查点保存当前资源、名单遍历位置和经校验的数组内进度。恢复未发完的列表时，连接器重新读取该数组并校验内容和顺序，再输出剩余记录。变更读取范围后需要重新扫描。这些读取接口不提供隔离快照或删除流。

HTTPS 客户端校验证书和主机名，禁止重定向，并支持有界重试和取消。GET 重试遵守合法 `Retry-After` 指示的等待时间。协商使用 gzip 时按解码后的响应字节数检查。
