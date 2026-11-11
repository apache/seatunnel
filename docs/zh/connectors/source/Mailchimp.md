import ChangeLog from '../changelog/connector-http-mailchimp.md';

# Http-Mailchimp

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

> Mailchimp 列表成员源连接器

## 描述

通过 Mailchimp Marketing API 的
[`GET /3.0/lists/{list_id}/members`](https://mailchimp.com/developer/marketing/api/lists/members/list-members/)
读取一个指定受众列表的成员。仅支持有界批处理，不支持活动、报表、OAuth 或 CDC。
复用已有 HTTP 和 JSON 依赖，不引入 Mailchimp SDK。

## 主要特性

- [x] 批处理
- [ ] 流处理
- [ ] 精确一次
- [ ] 列投影
- [ ] 并行读取

源并行度必须为 1。

## 配置项

| 名称 | 类型 | 必填 | 默认值 | 描述 |
| --- | --- | --- | --- | --- |
| token | String | 是 | - | 包含数据中心后缀的 Mailchimp API 密钥，例如 `<key>-us1`。不是 OAuth 令牌。 |
| list_id | String | 是 | - | 一个受众列表 ID，仅支持字母、数字、下划线及连字符。 |
| schema | Config | 是 | - | 非空输出结构，使用 SeaTunnel 已有 JSON 格式。 |
| base_url | String | 否 | `https://<dc>.api.mailchimp.com/3.0` | 显式信任的 API 根地址。默认数据中心从 `token` 提取。 |
| allow_insecure_http | Boolean | 否 | `false` | 仅为隔离测试及虚拟凭据允许 HTTP。禁止用于真实 API 密钥。 |
| page_size | Int | 否 | `1000` | 每页成员数，范围 1 到 1000。 |
| retry | Int | 否 | `3` | 每页遇到 HTTP 429 或 5xx 时额外重试次数，范围 0 到 10。 |
| retry_backoff_ms | Int | 否 | `1000` | 初始重试间隔，范围 1 到 60000 毫秒，逐次加倍，最多 60000 毫秒。 |
| request_timeout_ms | Int | 否 | `30000` | 连接、连接池获取及套接字空闲超时，范围 1 到 300000 毫秒，并非整个批次的截止时间。 |

### 认证与端点信任

`token` 保存 API 密钥，复用 SeaTunnel 已有的敏感配置脱敏规则。
密钥仅通过 HTTP Basic Authorization 请求头发送，用户名为 `seatunnel`，密码为 API 密钥。
连接器不记录请求头、响应正文或成员数据，分片状态不包含凭据及成员数据。
仍需遵循引擎配置和密钥管理规则，使用真实凭据时不要开启 HTTP wire/header 调试日志。

自定义 `base_url` 是管理员指定的凭据接收端，必须信任该地址。地址必须使用 HTTPS，
包含主机和可选端口，路径为 `/3.0`（允许末尾斜杠），不允许用户信息、查询、片段或其他路径。
HTTP 必须显式设置 `allow_insecure_http = true`，且仅用于虚拟凭据的隔离测试。
连接器禁用重定向、Cookie 和 HTTP 客户端自动重试。重定向直接失败，不转发凭据。
不访问响应中的链接。

### 输出结构

按成员对象逐条使用已有 JSON 格式解析。常用字段包括 `id`、`email_address`、`status`、
`last_changed` 和 `merge_fields`。字段名与 API 的 JSON 键一致。
可将 `last_changed` 设为 STRING，以保留 ISO-8601 时区偏移。
缺失的可选字段返回 null；类型转换失败会终止读取，错误信息不包含成员数据。

`merge_fields` 不会自动展开。已知且类型不同的字段可使用嵌套 ROW；值类型一致时可使用 MAP。
示例使用 `map<string,string>`，请根据实际合并字段定义配置结构。
STRING 类型遵循已有 JSON 格式的转换规则，不提供连接器特有的 JSON 序列化。

## 示例

```hocon
env {
  parallelism = 1
  job.mode = BATCH
}
source {
  Mailchimp {
    token = "<MAILCHIMP_API_KEY-us1>"
    list_id = "<LIST_ID>"
    page_size = 1000
    schema {
      fields {
        id = string
        email_address = string
        status = string
        last_changed = string
        merge_fields = "map<string,string>"
      }
    }
  }
}
sink {
  Console {}
}
```

## 分页、恢复和一致性

请求使用 `count`（最多 1000）及从零开始的 `offset`。
首个成功页面的 `total_items` 决定本次批处理的有限上界，后续增长不会扩大该上界。
最后一次请求将 `count` 调整为剩余成员数。

每页必须包含 `members` 数组和 API 整数偏移范围内的非负整数 `total_items`，
并返回预期数量的成员对象。完成前出现短页或空页、总数减少、JSON 无效、响应结构异常
或字段转换失败时，批次失败，不会静默报告成功。
单页响应上限为 32 MiB；合并字段较大时请减小 `page_size`。

这**不是事务快照或精确快照**。并发新增、删除和更新可能移动偏移量，即使总数相同，
也可能造成重复或遗漏。需要稳定导出时应避免读取期间修改受众列表。
不提供 `since_last_changed` 过滤或持久化增量游标。

恢复时有意从偏移量零**重放整个批次**，包括已经发送的页面。这是至少一次的批次重放，
不是精确一次投递，也不保证捕获持续变化成员的所有版本。
下游可使用成员 `id` 去重或更新写入。
检查点不保存分页偏移量或已完成标记，网络等待不持有收集器的检查点锁。

HTTP 429 和 5xx 会在相同偏移量最多额外重试 `retry` 次。
支持秒数或 HTTP 日期格式的 `Retry-After`，每次等待最多 60 秒；无效值使用指数退避。
其他非 200 响应（包括 401 和重定向）、正文异常及传输错误直接失败。
重启任务将重放整个批次。读取过程中的取消操作会中止进行中的 I/O 并唤醒退避等待，不报告成功完成。

<ChangeLog />
