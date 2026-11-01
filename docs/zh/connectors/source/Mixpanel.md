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
import ChangeLog from '../changelog/connector-http-mixpanel.md';

# Mixpanel

> Mixpanel 原始事件导出 Source 连接器

## 描述

通过 [Raw Event Export API](https://docs.mixpanel.com/reference/raw-event-export) 的
`GET /api/2.0/export` 读取一个日期区间，起止日期均包含在内。仅支持批处理，Source 并行度为 1。
响应按 JSONL 逐行流式处理，不将整个导出加载到内存，也不发送可选的 `limit` 参数，
因此不会截断超过 100,000 条事件的导出。

连接器直接复用 HTTP base 模块现有的 Apache HTTP 客户端依赖进行流式读取，
使用共享 JSON 反序列化器转换 schema，不使用 HTTP base 客户端的完整字符串响应。

## 主要特性

- [x] 批处理
- [ ] 流处理
- [ ] 精确一次
- [ ] 列裁剪
- [ ] 并行读取

## 配置选项

| 名称 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| username | String | 是 | - | 有项目访问权限的服务账号用户名，不能包含冒号或控制字符。 |
| password | String | 是 | - | 服务账号密钥，不能为空或包含控制字符。 |
| project_id | Long | 是 | - | 正整数项目 ID。 |
| from_date | String | 是 | - | 包含在内的开始日期，严格使用 `yyyy-MM-dd`。 |
| to_date | String | 是 | - | 包含在内的结束日期，不得早于 `from_date`。 |
| region | String | 否 | `US` | 区分大小写：`US`、`EU` 或 `IN`。 |
| schema | Config | 是 | - | 输出 schema，使用共享 JSON 反序列化器支持的类型。 |
| field_mapping | Map | 否 | - | schema 列名到原始事件 RFC 6901 JSON Pointer 的映射。未映射的列读取同名顶层字段。 |
| base_url | String | 否 | 区域地址 | 显式信任的 HTTPS origin，覆盖区域地址。禁止 userinfo、除 `/` 外的路径、查询参数和 fragment。 |
| allow_insecure_http | Boolean | 否 | `false` | 仅用于可信离线测试服务，允许 HTTP。不得用于真实凭证。 |
| connect_timeout_ms | Int | 否 | `30000` | 连接及连接池获取超时，1 到 300000 毫秒。 |
| read_timeout_ms | Int | 否 | `120000` | socket 无数据活动超时，不是整个导出的总时限；1 到 600000 毫秒。 |
| max_retries | Int | 否 | `3` | 消费事件响应体之前对 429/5xx 的重试次数，0 到 10；0 禁用重试。 |
| retry_backoff_ms | Int | 否 | `1000` | 指数退避初始延迟，1 到 60000 毫秒，不得大于 `max_retry_backoff_ms`。 |
| max_retry_backoff_ms | Int | 否 | `60000` | 重试延迟上限，1 到 600000 毫秒，同时限制 `Retry-After`。 |
| max_event_bytes | Int | 否 | `1048576` | 单行字节数上限，不含 LF，但包含存在的 CR；1 到 16777216。超限时任务失败，不截断或跳过。 |

### 认证和地址信任

使用有权导出目标项目数据的[服务账号](https://docs.mixpanel.com/reference/service-accounts)。
用户名和密钥经过 Base64 编码后仅放入 `Authorization: Basic` 请求头，
`project_id` 作为查询参数。不支持项目 token 或已弃用的 project-secret 认证。

默认区域地址为 `https://data.mixpanel.com`（US）、`https://data-eu.mixpanel.com`（EU）
和 `https://data-in.mixpanel.com`（IN），参见官方
[EU](https://docs.mixpanel.com/docs/privacy/eu-residency) 和
[印度](https://docs.mixpanel.com/docs/privacy/in-residency) 数据驻留文档。

仅信任目标地址的管理员才能配置 `base_url`，因为该地址会接收服务账号凭证。
TLS 证书和主机名校验保持启用。连接器不跟随重定向，重定向响应会直接失败，
不会把凭证转发给跳转地址。HTTP 必须显式设置 `allow_insecure_http = true`，
且仅用于测试服务。连接器不记录凭证、请求、查询字符串或事件及错误响应体，
也不保留可能包含这些信息的 HTTP/JSON 异常原因。真实凭证场景下请关闭 HTTP wire/header 调试日志。

### 日期和时间戳

日期字符串原样传递给 Mixpanel，不使用 JVM 时区。新项目使用 UTC，
旧项目使用项目时区。当前官方 API 文档写明：2023 年 1 月 1 日之后创建的项目使用 UTC，
2023 年 1 月 11 日之前创建的项目使用项目时区，这两个边界存在重叠。
如果项目在此过渡期创建，请向 Mixpanel 确认实际日期语义。

起止日期都包含在区间内，相邻任务不要无意重复边界日期。
原始 `properties.time` 保持 Unix 秒值，不请求 `time_in_ms`，也不转换成本地时间戳。

### Schema 和字段映射

每条记录必须是包含字符串 `event` 和对象 `properties` 的合法 JSON 对象。
无效 JSON、同行额外 JSON token、错误结构和类型转换失败会导致任务失败。
空导出可以成功，空白行不会被静默跳过。最后一行可以没有换行符。
映射到不存在字段的输出列为 null。

映射使用 JSON Pointer，不是 JSONPath：`/properties/time` 读取嵌套时间，
`/properties/$insert_id` 读取插入 ID，空指针 `""` 读取完整原始事件。
属性名中的 `/` 使用 `~1` 转义，`~` 使用 `~0` 转义。映射键必须属于 schema。
STRING 列选择对象时会保留结构并序列化为 JSON 文本，但不会保留原始空白。
也可以为 `properties` 声明共享 JSON 格式支持的嵌套 ROW 或 MAP 类型。

## 示例

通过正常的任务配置流程替换占位符，不要在公开配置文件中放入真实密钥。

```hocon
env {
  parallelism = 1
  job.mode = BATCH
}
source {
  Mixpanel {
    username = "${MIXPANEL_USERNAME}"
    password = "${MIXPANEL_PASSWORD}"
    project_id = 42
    from_date = "2026-01-01"
    to_date = "2026-01-02"
    region = US
    schema {
      fields {
        event = string
        time = bigint
        distinct_id = string
        insert_id = string
        properties = string
        raw_event = string
      }
    }
    field_mapping {
      time = "/properties/time"
      distinct_id = "/properties/distinct_id"
      insert_id = "/properties/$insert_id"
      raw_event = ""
    }
  }
}
sink {
  Console {}
}
```

## 重试、取消与恢复

仅 HTTP 429 和 5xx 在消费事件响应体之前重试，最多额外发送 `max_retries` 次请求。
从 `retry_backoff_ms` 开始倍增延迟，以 `max_retry_backoff_ms` 为上限。
合法 `Retry-After`（秒数或 HTTP 日期）可以延长等待，但仍受此上限约束。
401/403、重定向等其他状态立即失败。传输错误不在本地重试，即使尚未消费响应体；
HTTP 客户端自动重试已禁用。

成功响应开始后，响应中断、无效事件或 schema 错误会使任务失败，不在原地重新导出。
cancel/close 会终止活动请求和 socket，并唤醒退避等待。导出失败，或在有效 EOF 后的本地完成意图
提交之前取消，都不会通知读取完成。完成意图提交后，取消不能撤回已经开始的通知。

连接器不保存可恢复的事件偏移。网络读取和重试等待不持有 checkpoint 锁，
只有行收集和本地完成意图更新使用短临界区。完成回调不持有 checkpoint 或生命周期锁，
因此即使远端完成确认阻塞，取消和 checkpoint 仍可继续。Checkpoint 状态有意不记录事件进度，
因此任务恢复会重读整个日期区间，属于至少一次重放，已发出的事件可能重复。
这不是精确一次投递，也不是远端不可变快照。Mixpanel 的原始导出本身也不去重。
可保留事件名、`properties.distinct_id`、`properties.time` 和 `properties.$insert_id`，
按官方[导出说明](https://docs.mixpanel.com/reference/raw-data-export-api)在下游去重。
迟到事件可能使同一区间的后续导出发生变化。

## 限制

不包含流式/增量游标、日期分片、schema 自动发现、用户或用户组画像、cohort、事件筛选或 Sink。
请根据导出时间和 Mixpanel 限流选择合适日期区间。
单事件大小限制只约束记录缓冲，不限制整个导出的大小；下游保留的行不在此 reader 的内存约束内。

<ChangeLog />
