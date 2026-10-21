import ChangeLog from '../changelog/connector-http-linear.md';

# Linear

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
| api_key                      | String  | 是   | -          | Linear API key。连接器会将其原样作为 `Authorization` 请求头的值发送，不会自动添加 `Bearer ` 前缀，因此 OAuth 访问令牌需要配置为 `api_key = "Bearer <token>"`。 |
| method                       | String  | 否   | get        | HTTP 请求方法，支持 `GET` 和 `POST`。                                                             |
| headers                      | Map     | 否   | -          | 额外的 HTTP 请求头。此处任何 `Authorization` 条目都会被 `api_key` 的值覆盖。                                      |
| params                       | Map     | 否   | -          | 请求附带的查询参数。                                                                             |
| body                         | String  | 否   | -          | HTTP 请求体，通常与 `method = "POST"` 一起使用以发送 GraphQL 查询。                                    |
| format                       | String  | 否   | text       | 响应格式，`json` 时需要配合 `schema`；`text` 时返回原始响应。共享的 HTTP 选项也接受 `binary`，但 `Linear` 源连接器不支持 `binary`，`binary_chunk_size` 在此处无效。 |
| schema                       | Config  | 否   | -          | 输出数据结构，`format = "json"` 时必填。                                                             |
| schema.fields                | Config  | 否   | -          | 字段名与 SeaTunnel 数据类型，用于解析 JSON 响应。                                                      |
| content_field                | String  | 否   | -          | 在 `schema` 解析之前先通过 JSONPath 抽取一段 JSON，例如 `$.data.issues.nodes.*`。                      |
| json_field                   | Config  | 否   | -          | 字段级 JSONPath 映射，与 `schema` 配合使用。                                                          |
| retry                        | Int     | 否   | -          | HTTP 请求抛出 `IOException` 时的最大重试次数。                                                           |
| retry_backoff_multiplier_ms  | Int     | 否   | 100        | 重试退避乘数（毫秒），仅在配置了 `retry` 时生效。 |
| retry_backoff_max_ms         | Int     | 否   | 10000      | 最大重试退避时间（毫秒），仅在配置了 `retry` 时生效。 |
| enable_multi_lines           | Boolean | 否   | false      | 为 `true` 时，响应体中以换行分隔的多个 JSON 对象会被当作独立记录。                                           |
| keep_params_as_form          | Boolean | 否   | false      | 为 `true` 时，请求参数以表单编码放入请求体，而不是 URL 查询参数。                                           |
| connect_timeout_ms           | Int     | 否   | 12000      | HTTP 连接超时（毫秒）。                                                                                |
| socket_timeout_ms            | Int     | 否   | 60000      | HTTP socket 超时（毫秒）。                                                                              |
| json_filed_missed_return_null | Boolean | 否   | false      | 配置的 JSON 字段缺失时返回 null。                                                                     |
| common-options               | Config  | 否   | -          | Source 插件通用参数，详见 [Source Common Options](../common-options/source-common-options.md)。        |

:::tip

`api_key` 是敏感凭据。与 `password`、`access_key` 等选项不同，`api_key` 不会在解析后的配置日志中遮蔽，默认也不参与配置加密或解密。若需保护该值，请将 `api_key` 加入 `env.shade.options`，并使用配置的 `shade.identifier` 加密其值。参见[配置加密与解密](../../introduction/configuration/config-encryption-decryption.md)。

:::

## 使用说明

- 作业配置中的插件名是 `Linear`。
- 需要带类型的 SeaTunnel 数据行时，请设置 `format = "json"` 并配置 `schema`。
- 响应带有 GraphQL 外层结构时，先用 `content_field`（例如 `$.data.issues.nodes.*`）解包，再交给 `schema` 解析。
- 仅当每个输出字段需要独立的 JSONPath 时才使用 `json_field`。
- 该连接器不支持分页，每个作业只读取单次请求返回的数据；HTTP 的 `pageing` 配置块及其选项在此无效。
- `api_key` 会原样作为 `Authorization` 请求头的值发送，并始终覆盖 `headers` 中的任何 `Authorization` 条目，其余自定义请求头放在 `headers` 中即可。

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
