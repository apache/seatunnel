import ChangeLog from '../changelog/connector-http-notion.md';

# Notion

> Notion 源连接器

## 描述

Notion 源连接器用于读取 Notion API 数据。它基于 HTTP 源连接器实现，并会根据配置自动添加 `Authorization: Bearer <password>` 和 `Notion-Version: <version>` 请求头。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 源选项

| 参数名                      | 类型    | 必须 | 默认值    | 描述                                                                                          |
|-----------------------------|---------|----|--------|---------------------------------------------------------------------------------------------|
| url                         | String  | 是  | -      | Notion API 请求地址，例如 `https://api.notion.com/v1/users`。                                              |
| password                    | String  | 是  | -      | Notion 集成令牌，连接器会将其作为 `Authorization` Bearer token 发送。                                              |
| version                     | String  | 是  | -      | Notion API 版本号，例如 `2022-06-28`，连接器会将其作为 `Notion-Version` 请求头发送。                                       |
| method                      | String  | 否  | get    | HTTP 请求方法，支持 `GET` 和 `POST`。                                                                       |
| headers                     | Map     | 否  | -      | 额外的 HTTP 请求头。`Authorization` 和 `Notion-Version` 会由 `password` 和 `version` 自动添加。                      |
| params                      | Map     | 否  | -      | 请求附带的查询参数。                                                                                      |
| body                        | String  | 否  | -      | HTTP 请求体，通常与 `method = "POST"` 一起使用。                                                                |
| format                      | String  | 否  | text   | 响应格式，`json` 时需要配合 `schema`；`text` 时返回原始响应。                                                       |
| schema                      | Config  | 否  | -      | 输出数据结构，`format = "json"` 时必填。                                                                       |
| schema.fields               | Config  | 否  | -      | 字段名与 SeaTunnel 数据类型，用于解析 JSON 响应。                                                                |
| content_field               | String  | 否  | -      | 在 `schema` 解析之前先通过 JSONPath 抽取一段 JSON。                                                            |
| json_field                  | Config  | 否  | -      | 字段级 JSONPath 映射，与 `schema` 配合使用。                                                                |
| pageing                     | Config  | 否  | -      | HTTP 分页配置，继承自 HTTP 源连接器。                                                                          |
| page_type                   | String  | 否  | PageNumber | 分页类型，支持 `PageNumber`（默认）和 `Cursor`。对于返回 `next_cursor` 的 Notion 接口请使用 `Cursor`。                       |
| cursor_field                | String  | 否  | -      | 携带游标值的请求参数名称，与 `page_type = "Cursor"` 一起使用。                                                         |
| cursor_response_field       | String  | 否  | -      | 响应体中游标所在的 JSONPath，与 `page_type = "Cursor"` 一起使用。                                                     |
| poll_interval_millis        | int     | 否  | -      | 流模式下的请求间隔（毫秒）。Notion 源连接器当前只支持批处理模式。                                                            |
| retry                       | int     | 否  | -      | HTTP 请求返回 `IOException` 时的最大重试次数。                                                              |
| retry_backoff_multiplier_ms | int     | 否  | 100    | HTTP 请求失败时的重试退避倍数（毫秒）。                                                                          |
| retry_backoff_max_ms        | int     | 否  | 10000  | HTTP 请求失败时的最大重试退避时间（毫秒）。                                                                        |
| enable_multi_lines          | boolean | 否  | false  | 是否启用多行模式，将响应体中按换行分隔的多个 JSON 对象视为独立记录。                                                            |
| keep_params_as_form         | boolean | 否  | false  | 是否将请求参数作为表单参数发送，而不是 URL 查询参数。                                                                   |
| keep_page_param_as_http_param | boolean | 否 | false | 分页时是否将分页参数保留在 URL 中，而不是在请求体内替换。                                                                  |
| batch_size                  | int     | 否  | 100    | 当总页数未知时，每次请求返回的记录数。                                                                              |
| start_page_number           | long    | 否  | 1      | 从哪一页开始读取。                                                                                       |
| total_page_size             | long    | 否  | 0      | 要读取的总页数。`0` 表示按照 `batch_size` 一直读取，直到 API 不再返回新页。                                              |
| use_placeholder_replacement | boolean | 否  | false  | 是否使用 `${field}` 占位符替换 headers、params 和 body 中的字段值，否则按键名替换。                                            |
| connect_timeout_ms          | int     | 否  | 12000  | HTTP 连接超时时间（毫秒），默认 12000ms。                                                                       |
| socket_timeout_ms           | int     | 否  | 60000  | HTTP 套接字超时时间（毫秒），默认 60000ms。                                                                      |
| json_filed_missed_return_null | boolean | 否 | false  | 配置的 JSON 字段缺失时是否返回 `null`，否则报错。                                                                  |
| common-options              | config  | 否  | -      | 源插件通用参数，详见 [源通用选项](../common-options/source-common-options.md)。                                       |

:::tip

`password` 是敏感的 Notion 集成令牌，请避免在共享的任务文件中硬编码真实令牌。可使用 SeaTunnel 变量替换或部署平台的密钥管理机制。

:::

## 使用提示

- 需要按字段读取时，把 `format` 设置为 `json` 并配置 `schema`。
- 当 Notion 把记录嵌套在数组中（例如 `results`）时，使用 `content_field` 抽取数组元素。
- 只有当不同字段位于不同 JSON 路径时，才需要使用 `json_field`。
- `password` 和 `version` 会覆盖 `Authorization` 和 `Notion-Version` 请求头，请把其他自定义请求头放在 `headers` 中。
- 对于 Notion 列表类接口，建议使用 `page_type = "Cursor"` 配合 `cursor_field = "start_cursor"` 与 `cursor_response_field = "$.next_cursor"`。
- Notion 源连接器当前只支持批处理模式，`poll_interval_millis` 仅为兼容 HTTP 基类而保留，不会启用流式行为。

## 任务示例

### 读取 Users

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Notion {
    url = "https://api.notion.com/v1/users"
    password = "<notion-integration-token>"
    version = "2022-06-28"
    method = "GET"
    format = "json"
    content_field = "$.results.*"
    schema = {
      fields {
        object = string
        id = string
        type = string
        person = {
          email = string
        }
        name = string
        avatar_url = string
      }
    }
  }
}

sink {
  Console {
  }
}
```

### 搜索 Pages（游标分页）

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Notion {
    url = "https://api.notion.com/v1/search"
    password = "<notion-integration-token>"
    version = "2022-06-28"
    method = "POST"
    body = "{\"page_size\": 100, \"filter\": {\"value\": \"page\", \"property\": \"object\"}}"
    format = "json"
    content_field = "$.results[*]"
    page_type = "Cursor"
    cursor_field = "start_cursor"
    cursor_response_field = "$.next_cursor"
    schema = {
      fields {
        id = string
        object = string
        created_time = string
        last_edited_time = string
        archived = boolean
      }
    }
  }
}
```

### 通过 JSONPath 抽取字段

```hocon
source {
  Notion {
    url = "https://api.notion.com/v1/users"
    password = "<notion-integration-token>"
    version = "2022-06-28"
    method = "GET"
    format = "json"
    json_field = {
      id = "$.results[*].id"
      type = "$.results[*].type"
      name = "$.results[*].name"
    }
    schema = {
      fields {
        id = string
        type = string
        name = string
      }
    }
  }
}
```

## 变更日志

<ChangeLog />