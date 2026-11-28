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

import ChangeLog from '../changelog/connector-http-discourse.md';

# Discourse

Discourse Source 通过 [Discourse API](https://docs.discourse.org/) 读取一个站点的分类或近期主题的
元数据。连接器只发起只读请求，仅支持 BATCH 模式，Source 并行度必须为 1，每个 Source 实例读取一种资源。

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `base_url` | String | 是 | | 站点根地址，可带部署子路径。不要包含 API 端点、查询字符串、片段或凭证。 |
| `resource` | Enum | 是 | | `CATEGORIES` 或 `LATEST_TOPICS`。 |
| `include_subcategories` | Boolean | 否 | `true` | 只适用于 `CATEGORIES`。请求 API 返回子分类，并将返回的嵌套分类展开为独立行。 |
| `topic_limit` | Integer | 否 | `30` | 只适用于 `LATEST_TOPICS`。单页请求大小，范围为 1 到 100；实际数量由站点策略决定。 |
| `token` | String | 否 | | 通过 HTTPS 认证时，与 `username` 同时设置。站点允许匿名访问时，读取公开数据可省略两项。 |
| `username` | String | 否 | | Discourse API 用户名，与 `token` 同时设置。 |

连接器通过 `Api-Key` 和 `Api-Username` 请求头发送凭证，不将其放在 URL 中，也不跟随重定向。
使用最小权限的 API 密钥，并通过 SeaTunnel 的密钥或变量配置注入凭证，不要提交包含真实凭证的任务配置。
连接器不会自动重试失败请求（包括 HTTP 429）。若被限流，请按站点的重试策略等待后再启动任务。

## 输出

`CATEGORIES` 调用 `GET /categories.json`，输出 `id`（BIGINT），`name`、`slug`（STRING），
以及 `parent_category_id`、`topic_count`、`post_count`（可空 BIGINT）。顶层分类没有父分类。
ID 仅在同一站点和同一资源内有效，不保证跨站点唯一。只读取本次响应实际返回的分类及后代；
站点的延迟加载或分页策略可能省略其他分类，`include_subcategories` 不保证返回所有后代。

`LATEST_TOPICS` 调用 `GET /latest.json`，输出 `id`（BIGINT），`title`、`slug`（STRING），
`category_id`、`posts_count`、`views`（可空 BIGINT），`created_at`、`last_posted_at`
（可空 STRING，保留 API 返回的时间文本），以及 `closed`、`archived`（可空 BOOLEAN）。

结果仅包含所用身份可见的记录。连接器不跟随主题后续分页链接，不提供论坛全量导出、历史一致性快照或增量同步；
不读取单篇帖子、消息正文、用户记录或审计数据。重复运行时输出可能有重叠或变化；跨站写入同一目标时，可使用站点
标识和 `id` 去重。解码后的响应超过 16 MiB 时，任务失败，而不是输出不完整的结果。
HTTP 获取具有 30 秒的总耗时上限，即使响应持续传输也适用；超时时获取失败，不输出部分解码的记录。
响应最多包含 100 个头字段，每条状态行或头字段行最多为 16 KiB。

连接器会先完整校验响应，再输出任何记录。Reader 保留已校验的响应，每次轮询最多输出一条，
让引擎在记录之间执行检查点协作交接，不会因此再次请求 HTTP。单条记录的收集仍可能等待下游，
因此这不是检查点完成时间的保证。检查点仍保存原有的空 split 状态，不保存已准备的响应或行位置。
恢复后的 Reader 会重新获取当前完整响应，已输出的记录可能重复或变化，不提供恰好一次投递。

## 示例

```hocon
env {
  job.mode = BATCH
  parallelism = 1
}
source {
  Discourse {
    base_url = "https://meta.discourse.org"
    resource = LATEST_TOPICS
    topic_limit = 5
    plugin_output = "recent_discourse_topics"
  }
}
sink {
  Console {
    plugin_input = "recent_discourse_topics"
  }
}
```

## 变更日志

<ChangeLog />
