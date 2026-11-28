import ChangeLog from '../changelog/connector-http-bluetally.md';

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

# BlueTally

安装 `connector-http-bluetally` 后，使用 `BlueTally` 作为 Source 标识。连接器通过 HTTPS 从 BlueTally v1 API 读取资产和员工数组。请在 BlueTally 的 **Settings > API Keys** 中创建 API 密钥。多租户账户必须配置 `tenant_id`。连接器执行有界扫描，源端并行度为 1。[认证与 API 限制](https://developer.bluetally.com/reference/getting-started-with-your-api)

## 输出

连接器输出名为 `records` 的表。

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `resource` | STRING | `assets` 或 `employees`。 |
| `record_id` | STRING | 将系统整数 `id` 精确表示为字符串。资产标签 `asset_id` 保留在 `payload` 中。 |
| `tenant_id` | INT | 配置的租户 ID，省略时为 null。 |
| `updated_at` | STRING | UTC ISO 8601 更新时间，保留小数秒精度。 |
| `payload` | STRING | 完整的 BlueTally 记录，编码为 JSON，保留扩展和嵌套字段。 |

## 配置项

| 名称 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `token` | STRING | 是 | | 以 Bearer 令牌在请求头中发送的 BlueTally API 密钥。 |
| `resources` | LIST of STRING | 是 | | 按配置顺序扫描 `assets`、`employees` 或两者。 |
| `start_date` | STRING | 是 | | 包含此更新时间，使用带时区偏移的 RFC 3339。 |
| `end_date` | STRING | 否 | 扫描开始时刻 | 排除此更新时间，使用带时区偏移的 RFC 3339。 |
| `tenant_id` | INT | 多租户账户必填 | | 正数租户 ID，随每个请求发送。 |
| `page_size` | INT | 否 | 50 | BlueTally 的 `limit`，范围 1 至 1000。 |
| `max_pages_per_resource` | INT | 否 | 1000 | 每个资源允许的成功请求数，包含末尾空页。范围 1 至 10000。 |
| `max_attempts` | INT | 否 | 4 | 单次可重试 GET 的总尝试次数。范围 1 至 10。 |
| `request_timeout_ms` | INT | 否 | 30000 | 获取单页的总超时，单位为毫秒，包含重试。范围 100 至 300000。 |
| `max_response_bytes` | INT | 否 | 8388608 | 成功响应解码后的最大字节数。范围 1024 至 67108864。 |
| `endpoint` | STRING | 否 | `https://app.bluetallyapp.com` | 自定义 HTTPS 源地址，不包含路径、查询参数、片段或凭据。必须与 `trust_certificate` 一起配置。 |
| `trust_certificate` | STRING | 否 | | 执行节点上的自定义源地址 PEM 信任证书。连接器继续验证主机名。 |

## 分页与恢复

连接器使用 `sort=updated_at&order=desc`，每个资源从偏移量零开始。下一次请求按响应的原始记录数推进。短页和记录全部位于输出窗口之外的页都会继续分页。空数组表示该资源结束。页内或跨页顺序错误、重复页、同页或相邻页 ID 重复，以及在收到空页之前触及页数上限，都会让作业失败。[资产列表](https://developer.bluetally.com/reference/list-assets) [员工列表](https://developer.bluetally.com/reference/list-employees)

省略结束时间时，连接器仅选择一次结束时间，并和资源及偏移量一起保存。恢复时先检查查询范围，再校验已经输出一部分的响应，然后继续输出剩余记录。偏移量分页接口无法为扫描期间新建、修改或删除的记录提供隔离快照。更新时间窗口用于筛选当前记录，不提供变更日志或删除事件。

连接器在超时和尝试次数限制内遵守有效的 `Retry-After`。如果 HTTP 429 没有提供正数重试等待时长，请求会失败。连接器禁止重定向，诊断中省略密钥与响应字段。资产和员工记录可能包含敏感信息。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  BlueTally {
    token = ${BLUETALLY_TOKEN}
    resources = ["assets", "employees"]
    start_date = "2024-03-01T00:00:00Z"
    end_date = "2024-04-01T00:00:00Z"
  }
}
```

## 变更日志

<ChangeLog />
