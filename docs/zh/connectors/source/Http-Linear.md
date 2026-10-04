import ChangeLog from '../changelog/connector-http-linear.md';

# Http-Linear

> Linear 源连接器

## 描述

Linear 源连接器用于读取 [Linear](https://linear.app/) GraphQL API 数据。它基于 HTTP 源连接器实现，并按 Linear API 的要求将配置的 `api_key` 作为 `Authorization` 请求头发送。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 源选项

| 参数名                       | 类型    | 必须 | 默认值     | 描述                                                                                         |
|------------------------------|---------|------|------------|----------------------------------------------------------------------------------------------|
| url                          | String  | 是   | -          | Linear API 请求地址，通常为 `https://api.linear.app/graphql`。                                     |
| api_key                      | String  | 是   | -          | Linear API key。连接器会将其作为 `Authorization` 请求头发送。                                       |
| method                       | String  | 否   | get        | HTTP 请求方法，支持 `GET` 和 `POST`。                                                             |
| headers                      | Map     | 否   | -          | 额外的 HTTP 请求头。`Authorization` 会由 `api_key` 自动添加。                                      |
| params                       | Map     | 否   | -          | 请求附带的查询参数。                                                                             |
| body                         | String  | 否   | -          | HTTP 请求体，通常与 `method = "POST"` 一起使用以发送 GraphQL 查询。                                    |
| format                       | String  | 否   | text       | 响应格式，`json` 时需要配合 `schema`；`text` 时返回原始响应。                                       |
| schema                       | Config  | 否   | -          | 输出数据结构，`format = "json"` 时必填。                                                             |
| schema.fields                | Config  | 否   | -          | 字段名与 SeaTunnel 数据类型，用于解析 JSON 响应。                                                      |
| content_field                | String  | 否   | -          | 在 `schema` 解析之前先通过 JSONPath 抽取一段 JSON，例如 `$.data.issues.nodes.*`。                      |
| json_field                   | Config  | 否   | -          | 字段级 JSONPath 映射，与 `schema` 配合使用。                                                          |
| pageing                      | Config  | 否   | -          | HTTP 分页配置，继承自 HTTP 源连接器。                                                                |
| page_type                    | String  | 否   | PageNumber | 分页类型，支持 `PageNumber`（默认）和 `Cursor`。使用 `page_info` / `endCursor` 分页的 Linear 接口请使用 `Cursor`。 |
| cursor_field                 | String  | 否   | -          | 携带游标值的请求参数名称，与 `page_type = "Cursor"` 一起使用。                                           |
| cursor_response_field        | String  | 否   | -          | 响应体中游标所在的 JSONPath，与 `page_type = "Cursor"` 一起使用。                                       |
| poll_interval_millis         | Int     | 否   | -          | 流模式下的请求间隔（毫秒）。Linear 源连接器当前只支持批处理模式。                                            |
| retry                        | Int     | 否   | -          | HTTP 请求抛出 `IOException` 时的最大重试次数。                                                           |
| retry_backoff_multiplier_ms  | Int     | 否   | 100        | 重试退避乘数（毫秒）。                                                                                |
| retry_backoff_max_ms         | Int     | 否   | 10000      | 最大重试退避时间（毫秒）。                                                                            |
| enable_multi_lines           | Boolean | 否   | false      | 为 `true` 时，响应体中以换行分隔的多个 JSON 对象会被当作独立记录。                                           |
| keep_params_as_form          | Boolean | 否   | false      | 为 `true` 时，请求参数以表单编码放入请求体，而不是 URL 查询参数。                                           |
| keep_page_param_as_http_param | Boolean | 否  | false      | 为 `true` 时，翻页时页码参数保留在请求 URL 中，而不是替换到请求体内。                                        |
| batch_size                   | Int     | 否   | 100        | 总页数未知时，每次翻页请求返回的记录数。                                                                  |
| start_page_number            | Long    | 否   | 1          | 起始同步的页码。                                                                                      |
| total_page_size              | Long    | 否   | 0          | 要读取的总页数。`0` 表示一直按 `batch_size` 翻页，直到接口不再返回新数据。                              |
| use_placeholder_replacement  | Boolean | 否   | false      | 为 `true` 时，对请求头、参数和请求体使用 `${field}` 占位符替换；否则使用按键替换。                          |
| connect_timeout_ms           | Int     | 否   | 12000      | HTTP 连接超时（毫秒）。                                                                                |
| socket_timeout_ms            | Int     | 否   | 60000      | HTTP socket 超时（毫秒）。                                                                              |
| json_filed_missed_return_null | Boolean | 否  | false      | 配置的 JSON 字段缺失时返回 null。                                                                     |
| common-options               | Config  | 否   | -          | Source 插件通用参数，详见 [Source Common Options](../common-options/source-common-options.md)。        |

:::tip

`api_key` 是敏感凭据。请避免在共享的作业文件中硬编码真实密钥，尽量使用 SeaTunnel 变量替换或部署环境的保密机制。

:::

## 使用说明

- 作业配置中的插件名是 `Linear`。
- 需要带类型的 SeaTunnel 数据行时，请设置 `format = "json"` 并配置 `schema`。
- 响应带有 GraphQL 外层结构时，先用 `content_field`（例如 `$.data.issues.nodes.*`）解包，再交给 `schema` 解析。
- 仅当每个输出字段需要独立的 JSONPath 时才使用 `json_field`。
- `api_key` 会覆盖 `Authorization` 请求头，其余自定义请求头放在 `headers` 中即可。

## 任务示例

### 读取 Issues

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Linear {
    url = "https://api.linear.app/graphql"
    api_key = "<your_linear_api_key>"
    method = "POST"
    body = "{\"query\": \"{ issues { nodes { id title } } }\"}"
    format = "json"
    content_field = "$.data.issues.nodes.*"
    schema = {
      fields {
        id = string
        title = string
      }
    }
    plugin_output = "linear_data"
  }
}

sink {
  Console {
  }
}
```

### 使用 JSONPath 抽取字段

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Linear {
    url = "https://api.linear.app/graphql"
    api_key = "<your_linear_api_key>"
    method = "POST"
    body = "{\"query\": \"{ issues { nodes { id title } } }\"}"
    format = "json"
    json_field = {
      id = "$.data.issues.nodes[*].id"
      title = "$.data.issues.nodes[*].title"
    }
    schema = {
      fields {
        id = string
        title = string
      }
    }
  }
}
```

## 更新日志

<ChangeLog />
