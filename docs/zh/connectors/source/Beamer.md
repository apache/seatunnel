import ChangeLog from '../changelog/connector-http-beamer.md';

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

# Beamer

安装 `connector-http-beamer` 后，使用 `Beamer` 作为 Source 标识。Beamer Source 读取净推荐值（NPS）反馈。连接器通过 HTTPS GET 请求访问 `/v0/nps`，并在 `Beamer-Api-Key` 请求头中发送密钥。请在 Beamer 的 **Settings > API** 中创建有权读取 NPS 反馈的 API 密钥。[Beamer API 文档](https://getbeamer-api.pages.dev/)

这是一个有界数据源，源端并行度为 1。它返回一张名为 `nps` 的表。

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `record_id` | STRING | Beamer 的反馈 ID。原字段缺失或为 null 时，此字段为 null。 |
| `created_at` | STRING | 转换为 UTC 的 ISO 8601 反馈创建时间，保留小数秒。 |
| `payload` | STRING | 完整反馈的 JSON 对象，保留反馈内容、评分、可选字段和 Beamer 返回的扩展字段。 |

## 配置项

| 名称 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `token` | STRING | 是 | | Beamer API 密钥。 |
| `start_date` | STRING | 是 | | 包含此创建时刻，使用带时区偏移的 RFC 3339。 |
| `end_date` | STRING | 否 | 扫描开始时刻 | 排除此创建时刻，使用带时区偏移的 RFC 3339。 |
| `page_size` | INT | 否 | 100 | Beamer 的 `maxResults`，范围为 1 至 100。 |
| `max_pages` | INT | 否 | 1000 | 允许成功读取的页数，包含最后一个空页。范围为 1 至 5000。 |
| `max_attempts` | INT | 否 | 4 | 可重试错误的单页请求总尝试次数。范围为 1 至 10。 |
| `request_timeout_ms` | INT | 否 | 30000 | 单页获取总超时，单位毫秒，包含重试。范围为 100 至 300000。 |
| `max_response_bytes` | INT | 否 | 8388608 | 成功响应解压后的最大字节数。范围为 1024 至 67108864。 |
| `endpoint` | STRING | 否 | `https://api.getbeamer.com` | 自定义 HTTPS 源地址。仅指定主机名及可选端口，需要配置 `trust_certificate`。 |
| `trust_certificate` | STRING | 否 | | 自定义源地址在执行节点上的 PEM 信任证书路径。连接器继续验证 TLS 主机名。 |

## 时间窗口与恢复

输出包含在 `start_date` 创建的反馈，并排除在 `end_date` 创建的反馈。省略结束时间时，连接器在扫描开始时选定一次，并将该值保存到检查点。请求下界向下取整到秒后再提前一秒，上界向上取整到秒。连接器在输出前应用原始精确窗口。

请求从 `page=0` 开始，使用固定的日期条件和 `maxResults`。短页或仅有边界记录的页仍会继续读取。空数组代表结束。重复页面、同页或相邻页 ID 重复、无效响应以及空页前耗尽页数上限都会让作业失败。恢复页内进度时，连接器先比对检查点保存的页面指纹，再输出剩余记录。

扫描按反馈创建时间进行。Beamer 的页码分页在扫描期间发生数据变动时无法提供隔离快照。恢复时请使用同一账户和时间窗口。分次读取时，可将一次扫描的结束时间作为下一次扫描的开始时间。

Beamer 将该接口的 HTTP 429 说明为账户请求额度耗尽，连接器会立即报告。可重试的临时网络或服务器错误遵守尝试次数和总超时，并等待有效 `Retry-After` 指定的时长。连接器禁止重定向，诊断中省略凭据、响应体及原始请求 URL。输出反馈可能包含邮箱等个人信息。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Beamer {
    token = ${BEAMER_API_KEY}
    start_date = "2024-03-01T00:00:00Z"
    end_date = "2024-04-01T00:00:00Z"
  }
}
```

## 变更日志

<ChangeLog />
