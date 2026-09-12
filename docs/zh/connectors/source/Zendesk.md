import ChangeLog from '../changelog/connector-http-zendesk.md';

# Zendesk

> Zendesk 数据源连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

用于从 [Zendesk REST API](https://developer.zendesk.com/api-reference/) 读取数据。它使用 Zendesk
账号邮箱和 API token 进行认证（以 HTTP Basic `Authorization` 请求头发送），并将某个 Zendesk 接口
（如 tickets、users、organizations）读取为 SeaTunnel 的行数据。

该连接器基于 [Http source](Http.md) 实现，继承了大部分选项。区别主要在于认证选项以及 `format`
的默认值。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 选项

|            名称             |  类型   | 是否必填 |   默认值   | 描述                                                                                                                                                                                       |
|-----------------------------|---------|----------|-----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                         | String  | 是       | -         | 要读取的 Zendesk REST API 接口地址，例如 `https://your-subdomain.zendesk.com/api/v2/tickets.json`。                                                                                       |
| email                       | String  | 是       | -         | 用于 API token 认证的 Zendesk 账号邮箱。它会与 `api_token` 组合为 `{email}/token:{api_token}` 并以 HTTP Basic `Authorization` 请求头发送。                                              |
| api_token                   | String  | 是       | -         | Zendesk API token。获取方式请参考 [Zendesk API token 文档](https://support.zendesk.com/hc/en-us/articles/4408889192858)。                                                                  |
| method                      | String  | 否       | get       | HTTP 请求方法，仅支持 `GET` 和 `POST`。                                                                                                                                                    |
| schema                      | Config  | 否       | -         | 数据的结构，包括字段名称和字段类型。更多详情请参考 [Schema Feature](../../introduction/concepts/schema-feature.md)。                                                                       |
| format                      | String  | 否       | text      | 上游数据的格式，目前仅支持 `json` 和 `text`，默认 `text`。Zendesk 接口始终返回 JSON，因此通常需要把 `format` 设为 `json`，并配合 `content_field` 把真正的结果数组抽取出来再映射成行。 |
| params                      | Map     | 否       | -         | 附加到请求 URL 的查询参数，可用于过滤、分页等场景。                                                                                                                                        |
| body                        | String  | 否       | -         | `POST`（或支持 body 的方法）请求体。当 `format = "json"` 时，body 必须是合法的 JSON。                                                                                                    |
| json_field                  | Config  | 否       | -         | 该参数用于配置 schema，因此必须与 `schema` 一起使用。它将响应中的 JSON 路径映射到 schema 字段。详情和示例请参考 [Http source](./Http.md) 连接器。                                        |
| content_field               | String  | 否       | -         | 该参数可以在映射为行之前，提取 JSON 响应中的某个子部分（例如顶层键 `tickets` 或 `users` 下的数组）。读取 `/api/v2/tickets.json` 时通常设置为 `content_field = "$.tickets.*"`。          |
| poll_interval_millis        | int     | 否       | -         | 流模式下两次连续请求之间的间隔，单位毫秒。批模式下无效。                                                                                                                                   |
| retry                       | int     | 否       | -         | HTTP 请求抛出 `IOException` 时的最大重试次数。                                                                                                                                            |
| retry_backoff_multiplier_ms | int     | 否       | 100       | 重试退避倍数，单位毫秒。                                                                                                                                                                    |
| retry_backoff_max_ms        | int     | 否       | 10000     | 最大重试退避时间，单位毫秒。                                                                                                                                                                |
| enable_multi_lines          | boolean | 否       | false     | 是否把响应解析为按换行分隔的多段 JSON。仅在 `format = "json"` 时生效。                                                                                                                     |
| common-options              | config  | 否       | -         | 数据源插件通用参数，详情请参考 [Source Common Options](../common-options/source-common-options.md)。                                                                                       |

### url [String]

要读取的 Zendesk REST API 接口地址，例如 `https://your-subdomain.zendesk.com/api/v2/tickets.json`。

### email [String]

用于 API token 认证的 Zendesk 账号邮箱。它会与 `api_token` 组合为 `{email}/token:{api_token}` 并以
HTTP Basic `Authorization` 请求头发送。

### api_token [String]

Zendesk API token。获取方式请参考 [Zendesk API token 文档](https://support.zendesk.com/hc/en-us/articles/4408889192858)。

### method [String]

HTTP 请求方法，仅支持 `GET` 和 `POST`。`POST` 一般需要配合 `body`，用于 Zendesk 上接受请求体做查询
或分页的接口。

### schema [Config]

数据的结构，包括字段名称和字段类型。更多详情请参考 [Schema Feature](../../introduction/concepts/schema-feature.md)。

### format [String]

上游数据的格式，目前仅支持 `json` 和 `text`，默认 `text`。Zendesk 接口始终返回 JSON，因此需要把
`format` 设为 `json`，并配合 `content_field` 抽取结果数组。

### params [Map]

附加到请求 URL 的查询参数，用于过滤、分页等场景。

### body [String]

`POST`（或支持 body 的方法）请求体。当 `format = "json"` 时，body 必须是合法的 JSON。

### json_field [Config]

该参数用于配置 schema，因此必须与 `schema` 一起使用。它将响应中的 JSON 路径映射到 schema 字段。
详情和示例请参考 [Http source](./Http.md) 连接器。

### content_field [String]

该参数可以在映射为行之前，提取 JSON 响应中的某个子部分（例如顶层键 `tickets` 或 `users` 下的
数组）。读取 `/api/v2/tickets.json` 时通常设置为 `content_field = "$.tickets.*"`。详情和示例请
参考 [Http source](./Http.md) 连接器。

### poll_interval_millis [int]

流模式下两次连续请求之间的间隔，单位毫秒。批模式下无效。

### retry [int]

HTTP 请求抛出 `IOException` 时的最大重试次数。重试间隔由 `retry_backoff_multiplier_ms` 和
`retry_backoff_max_ms` 共同决定。

### retry_backoff_multiplier_ms [int]

重试退避的基础单位，单位毫秒。重试之间的等待时间会在多次重试中逐渐增长，上限为
`retry_backoff_max_ms`。增长曲线并不是每次固定的倍数关系，具体的斐波那契策略请参考
`HttpClientProvider`（位于 `connector-http-base`）。

### retry_backoff_max_ms [int]

最大重试退避时间，单位毫秒。

### enable_multi_lines [boolean]

是否把响应解析为按换行分隔的多段 JSON。仅在 `format = "json"` 时生效。

### common options

数据源插件通用参数，详情请参考 [Source Common Options](../common-options/source-common-options.md)。

## 任务示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Zendesk {
    url = "https://your-subdomain.zendesk.com/api/v2/tickets.json"
    email = "agent@example.com"
    api_token = "${ZENDESK_API_TOKEN}"
    method = "GET"
    format = "json"
    content_field = "$.tickets.*"
    schema = {
      fields {
        id = bigint
        subject = string
        status = string
        priority = string
        created_at = string
        updated_at = string
      }
    }
  }
}

sink {
  Console {}
}
```

## 变更日志

<ChangeLog />
