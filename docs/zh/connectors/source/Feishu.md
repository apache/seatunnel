import ChangeLog from '../changelog/connector-http-feishu.md';

# Feishu

{/* Licensed to the Apache Software Foundation (ASF) under one or more
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
    limitations under the License. */}

Feishu Source 以有界批处理方式读取飞书或 Lark 多维表格中一张数据表的全部记录。请将 Source 并行度设为 `1`，每张数据表配置一个 Source。全量读取后的数据由下游 Sink 决定采用追加还是覆盖。

## 访问权限

创建并发布具备多维表格读取权限的自建应用，给应用授予对应多维表格的访问权限。如果多维表格开启了高级权限，请按平台指引授予应用所需的管理权限，否则接口可以成功返回空结果。多维表格 URL 中包含表格 Token 和数据表 ID。如果表格嵌入知识库，请填写多维表格本身的 Token。

Source 用应用 ID 和密钥获取租户访问令牌，再查询数据表记录。请按账号所在区域配置飞书或 Lark API 地址，通过密钥管理机制提供凭证。

## Source 参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `app_id` | String | 是 | | 自建应用 ID。 |
| `client_secret` | String | 是 | | 自建应用密钥。 |
| `token` | String | 是 | | 多维表格 Token。 |
| `table_id` | String | 是 | | 数据表 ID。 |
| `api_base_url` | String | 否 | `https://open.feishu.cn` | HTTPS 服务地址。Lark 使用 `https://open.larksuite.com`。填写服务地址时省略 API 路径、查询参数、片段和嵌入式凭证。 |
| `allow_insecure_http` | Boolean | 否 | `false` | 允许隔离测试服务使用 HTTP。 |
| `page_size` | Integer | 否 | `100` | 每页请求的最大记录数，取值为 1 至 500。 |
| `max_pages` | Integer | 否 | `10000` | 单次全量读取的最大页数，取值为 1 至 10000。数据超出该限制时，作业失败。 |
| `page_timeout_ms` | Integer | 否 | `60000` | 单页请求的总期限，包括获取令牌、接收响应体和重试，取值为 1000 至 300000 毫秒。 |
| `max_retries` | Integer | 否 | `3` | 每次 API 请求发生限流或服务端错误时的最大重试次数，取值为 0 至 5。 |

多维表格 Token 和数据表 ID 支持字母、数字、下划线和连字符。Source 会拒绝重定向，并在单页期限内重试 HTTP 429、HTTP 5xx 和平台限流错误码 99991400。等待时间取 `Retry-After` 与平台 `x-ogw-ratelimit-reset` 响应头中两个有效值的较大值。剩余期限不足以等待时，作业失败。平台报告租户令牌过期时，每页可重新获取令牌并重发一次。分页响应体最大为 16 MiB，令牌响应体最大为 64 KiB。

## 输出字段

| 字段 | 类型 | 可空 | 说明 |
| --- | --- | --- | --- |
| `record_id` | STRING | 否 | 多维表格记录 ID。 |
| `id` | STRING | 是 | 平台返回的可选 ID。 |
| `fields` | STRING | 是 | 以 JSON 对象保存数据表的动态字段值。 |
| `record_json` | STRING | 否 | 平台返回的完整记录，以 JSON 保存。 |

JSON 字段保留嵌套对象和数组、Unicode 字符、显式 JSON null 以及平台返回的时间戳。原始记录中的 `fields` 缺失或为 JSON null 时，输出的 `fields` 列为 SQL null。完整记录会保留属性缺失与显式 JSON null 的区别。

## Checkpoint 与恢复

在开启 Checkpoint 的 Zeta 批作业中，状态版本 2 保存当前游标、已完成页的历史，以及当前页已接受记录数和四个实际输出字段的 SHA-256 摘要。完整响应和已接受前缀校验不持有检查点锁，每次轮询最多接受一条已准备记录后即交还引擎。最后一行与下一游标或终止状态、前缀重置一起原子提交。使用同一应用、API 地址和数据表恢复时，重新读取当前页并验证全部已接受前缀后才跳过；前缀内容、顺序或长度变化时在新输出前失败。不支持早期开发阶段的状态格式。最近完成检查点之后接受的记录仍可重放，具体影响取决于 Sink 的恢复语义。API 不提供冻结快照，尚未接受的记录仍可变化。请求等待期间保存点停止能进入清理，单次输出器调用阻塞时仍遵循引擎的取消和检查点行为。

如果数据表在读取过程中发生变化，飞书可以使游标失效或返回变化后的数据。需要页间结果一致时，请在整个读取过程中保持数据表稳定。该 Source 执行全量读取。如果平台拒绝已保存的游标，请发起新的全量读取。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Feishu {
    app_id = "${FEISHU_APP_ID}"
    client_secret = "${FEISHU_APP_SECRET}"
    token = "replace-with-base-token"
    table_id = "replace-with-table-id"
  }
}

sink {
  Console {}
}
```

<ChangeLog />
