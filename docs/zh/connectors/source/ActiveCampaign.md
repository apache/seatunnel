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

import ChangeLog from '../changelog/connector-http-activecampaign.md';

# ActiveCampaign

ActiveCampaign Source 通过 ActiveCampaign API v3 读取账户中的营销活动、联系人、列表、销售机会、分群和表单集合。安装 `connector-http-activecampaign`，在批处理作业中配置 `ActiveCampaign`。

从账户的 **设置 → 开发者** 获取 API 地址，在 `base_url` 中填写 HTTPS 源站地址。Source 通过 `Api-Token` 请求头认证，校验 TLS 证书和主机名，并拒绝 HTTP 重定向。[ActiveCampaign API 源站地址](https://developers.activecampaign.com/reference/url)

## 输出

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `resource` | STRING | `campaigns`、`contacts`、`lists`、`deals`、`segments` 或 `forms` |
| `id` | STRING | 记录标识，`record` 内也保留该标识及其原始 JSON 类型 |
| `record` | STRING | 序列化的 JSON 对象，保留嵌套字段、空值和完整数值精度 |

Source 输出对应资源数组中的记录。ActiveCampaign 权限可能限制销售机会记录的字段。列表接口按服务端默认设置返回邮件列表。分群资源使用 v3 的 `/segments` 集合接口。

## 配置项

| 配置项 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `base_url` | String | 必填 | 账户开发者设置中的 HTTPS 源站地址，可带端口 |
| `token` | String | 必填 | ActiveCampaign API Token |
| `resources` | String 列表 | 全部六项 | 按扫描顺序配置，各资源名称仅可出现一次 |
| `page_size` | Integer | `20` | 请求的分页上限，1 至 100 |
| `max_pages` | Integer | `10000` | 每个资源的分页数上限，1 至 100000 |
| `request_interval_ms` | Integer | `250` | 本作业发起相邻请求的最小间隔，200 至 60000 毫秒 |
| `max_attempts` | Integer | `4` | 每页请求的最大尝试次数，包含首次请求，1 至 8 |
| `socket_timeout_ms` | Integer | `15000` | 单次尝试的连接和读空闲超时，100 至 120000 毫秒 |
| `page_deadline_ms` | Integer | `60000` | 单页请求、重试和解析的总时限，500 至 600000 毫秒 |
| `max_response_bytes` | Integer | `8388608` | 单页响应解压后的字节上限，128 至 33554432 字节 |

## 分页和恢复

每个作业由一个 Source 读取器按顺序扫描资源。ActiveCampaign 对每个账户限制每秒五次请求，该额度由该账户下的其他作业和应用共享。连接器限制本作业的请求频率，重试次数有上限，并在单页时限内遵守有效的 `Retry-After`。[ActiveCampaign 限流说明](https://developers.activecampaign.com/reference/rate-limits)

分页偏移量按请求的页面大小递增。若返回合法的 `meta.total`，短页或空页仍可能有后续分页。没有总数时，短页表示资源结束。检测到重复页、连续过多空页或无效分页元数据时，作业会报告错误。[ActiveCampaign 分页说明](https://developers.activecampaign.com/reference/pagination)

检查点记录资源、页偏移量和已输出的行位置。恢复部分读取的页面时会重新请求并验证页面内容，再跳过已输出的行。恢复时使用相同的源站、资源顺序和页面大小。远端数据变化可能改变页面内容或记录位置，API 不提供跨页快照。扫描期间数据有变化时，可以用 `(resource, id)` 作为下游去重键。

## 示例

```hocon
env {
  job.mode = "BATCH"
}

source {
  ActiveCampaign {
    base_url = "https://account.api-us1.com"
    token = "${activecampaign_token}"
    resources = ["campaigns", "contacts", "lists", "deals", "segments", "forms"]
    page_size = 20
  }
}

sink {
  Console {}
}
```

<ChangeLog />
