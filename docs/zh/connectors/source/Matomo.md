import ChangeLog from '../changelog/connector-http-matomo.md';

# Matomo

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

Matomo 源通过 [Matomo Reporting API](https://developer.matomo.org/guides/reporting-api)
读取一个站点按天汇总的 `VisitsSummary.get` 数据。仅支持有界批处理，不读取访问明细、
不支持分群，也不保存增量游标。

## 参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `url` | String | 是 | — | HTTP(S) 报表 API 地址。按实例要求包含安装路径与 `index.php`，不要加凭据、查询参数或片段。 |
| `site_id` | Integer | 是 | — | Matomo 网站 ID，须为正数。 |
| `start_date` | String | 是 | — | 报表开始日（含），格式 `YYYY-MM-DD`。 |
| `end_date` | String | 是 | — | 报表结束日（含）。区间为 1–366 天。 |
| `token` | String | 否 | — | 对站点有查看权限的令牌。不配置时使用匿名 GET；配置后须使用 HTTPS，且令牌只放在 POST 表单请求体内。 |
| `timeout_ms` | Integer | 否 | 15000 | 连接和读取超时，范围 1–120000 毫秒。 |

使用 `BATCH` 模式，源端并行度为 1。不会跟随重定向。请求失败或字段有误时作业失败，
不会把不完整响应当作空报表。只有源站明确返回空数组的日期才不输出。
日期以 Matomo 站点时区为准。重复执行同一区间可能重复发出数据，
可使用 `(site_id, report_date)` 在目标端做对账。

## 输出字段

| 字段 | 类型 | 可为空 |
| --- | --- | --- |
| `site_id` | INT | 否 |
| `report_date` | STRING | 否 |
| `visits` | BIGINT | 否 |
| `actions` | BIGINT | 否 |
| `unique_visitors` | BIGINT | 是 |

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Matomo {
    url = "https://demo.matomo.cloud/"
    site_id = 1
    start_date = "2026-06-01"
    end_date = "2026-06-02"
  }
}
sink {
  Console {}
}
```

示例使用厂商的公开演示实例，无需令牌。演示数据可能变化。
访问私有站点时，请使用部署环境的密钥配置，不要将令牌提交到仓库。

<ChangeLog />
