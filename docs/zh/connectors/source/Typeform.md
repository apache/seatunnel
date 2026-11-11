import ChangeLog from '../changelog/connector-http-typeform.md';

# Http-Typeform

> Typeform 源连接器

## 描述

通过 Typeform Responses API（`GET /forms/{form_id}/responses`）读取一个表单的回复。基于
`connector-http-base` 构建，仅支持批处理模式。第一版只读取回复，不支持表单、工作区、Webhook、OAuth，
也不会把 `answers` 展开成多列。

## 主要特性

- [x] 批处理
- [ ] 流处理
- [ ] 精确一次
- [ ] 列投影
- [ ] 并行度

## 选项

| 名称 | 类型 | 必填 | 默认值 | 描述 |
| --- | --- | --- | --- | --- |
| api_key | String | 是 | - | 具有 `responses:read` 权限的 Typeform 个人访问令牌（原始令牌，不带 `Bearer ` 前缀）。 |
| form_id | String | 是 | - | 要读取的表单 ID，只能包含字母、数字、`_` 和 `-`。 |
| base_url | String | 否 | `https://api.typeform.com` | 使用 Typeform 欧盟数据中心的账号请使用 `https://api.eu.typeform.com` 或 `https://api.typeform.eu`。 |
| page_size | Int | 否 | `100` | 每页回复数量，范围 1 到 1000。 |
| since | String | 否 | - | 只读取该时间（含）之后的回复。Unix 秒或 ISO 8601 UTC，例如 `2026-01-01T00:00:00`。 |
| until | String | 否 | - | 只读取该时间（含）之前的回复，格式同 `since`。 |
| response_type | String | 否 | - | 逗号分隔，例如 `partial,completed`。未设置时 Typeform 只返回已完成的回复。 |
| retry | Int | 否 | `3` | HTTP 429 和 5xx 的最大重试次数。 |
| retry_backoff_multiplier_ms | Int | 否 | `100` | 退避倍数（毫秒）。 |
| retry_backoff_max_ms | Int | 否 | `10000` | 最大退避时间（毫秒）。 |

### api_key
只会通过 `Authorization: Bearer <token>` 请求头发送，绝不会出现在查询参数中。连接器不会在日志中
输出请求头或配置，也不会持久化任何状态，因此该令牌不会进入检查点。引擎在打印作业配置时是否对
`api_key` 做脱敏处理，不在本连接器的控制范围内。

### since / until / response_type
这三个参数会原样传给 Typeform。对于已完成的回复，Typeform 按 `submitted_at` 过滤 `since` 和 `until`；
对于其他回复类型则使用不同的时间戳（参见 Typeform 的 API 参考）。

### retry
请求之间的间隔至少 500 毫秒，因为 Typeform 每个账号每秒只允许 2 个请求。HTTP 429 和 5xx 会进行重试，
退避时间从 `retry_backoff_multiplier_ms` 开始翻倍，直到 `retry_backoff_max_ms`。不会遵循 `Retry-After`
响应头。

## 输出列

`response_id`、`landing_id`、`token`、`landed_at`、`submitted_at`（均为 STRING），以及
`answers`、`hidden`（原样输出的 JSON 字符串）。回复没有答案时（空数组或缺失），`answers` 为 NULL；
表单没有隐藏字段时 `hidden` 为 `{}`。

## 示例

```hocon
env {
  parallelism = 1
  job.mode = BATCH
}

source {
  Typeform {
    api_key = "<PERSONAL_ACCESS_TOKEN>"
    form_id = "<FORM_ID>"
    since = "2026-01-01T00:00:00"
    plugin_output = "typeform_responses"
  }
}

sink {
  Console {
    plugin_input = "typeform_responses"
  }
}
```

## 注意事项

每次运行都是表单回复的有界快照，可用 `since` 和 `until` 缩小范围。分页从最新的回复开始：把每页最后一条回复的
`token` 作为 `before` 游标发送，遇到空页即结束。读取期间新提交的回复出现在最新的一端，不会影响正在读取的分页。
排序方式不可配置，因为 `before` 游标依赖 Typeform 的默认排序。

连接器不保存增量状态。作业失败后重启会从第一页重新读取，可能重复发送已发送的行。每行都包含 `response_id`，
下游可据此去重或 upsert，但连接器不提供投递保证。Typeform 提示最近约 30 分钟内的回复可能尚未返回。

整个表单在一次不中断的读取中完成，请求之间有短暂间隔，因此表单很大时，触发 checkpoint 需要等到读取结束。

<ChangeLog />
