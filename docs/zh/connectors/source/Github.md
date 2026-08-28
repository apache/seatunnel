import ChangeLog from '../changelog/connector-http-github.md';

# Github

> Github 源连接器

## 描述

Github 源连接器用于读取 GitHub REST API 数据。它基于 HTTP 源连接器实现，并会自动把 `access_token` 组装成 `Authorization: Bearer <access_token>` 请求头。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 选项

| 参数名                      | 类型    | 必填 | 默认值 |
|-----------------------------|---------|------|--------|
| url                         | String  | 是   | -      |
| access_token                | String  | 是   | -      |
| method                      | String  | 否   | GET    |
| headers                     | Map     | 否   | -      |
| params                      | Map     | 否   | -      |
| body                        | String  | 否   | -      |
| format                      | String  | 否   | text   |
| schema                      | Config  | 否   | -      |
| schema.fields               | Config  | 否   | -      |
| json_field                  | Config  | 否   | -      |
| content_field               | String  | 否   | -      |
| pageing                     | Config  | 否   | -      |
| page_type                   | String  | 否   | PageNumber |
| cursor_field                | String  | 否   | -      |
| cursor_response_field       | String  | 否   | -      |
| poll_interval_millis        | int     | 否   | -      |
| retry                       | int     | 否   | -      |
| retry_backoff_multiplier_ms | int     | 否   | 100    |
| retry_backoff_max_ms        | int     | 否   | 10000  |
| enable_multi_lines          | boolean | 否   | false  |
| keep_params_as_form         | boolean | 否   | false  |
| keep_page_param_as_http_param | boolean | 否 | false  |
| batch_size                  | int     | 否   | 100    |
| start_page_number           | long    | 否   | 1      |
| total_page_size             | long    | 否   | 0      |
| use_placeholder_replacement | boolean | 否   | false  |
| connect_timeout_ms          | int     | 否   | 12000  |
| socket_timeout_ms           | int     | 否   | 60000  |
| json_filed_missed_return_null | boolean | 否 | false  |
| common-options              | config  | 否   | -      |

### url [String]

GitHub REST API 地址，例如 `https://api.github.com/orgs/apache/repos`。

### access_token [String]

GitHub 个人访问令牌。连接器会把它作为 Bearer token 写入 HTTP `Authorization` 请求头。

### method [String]

HTTP 请求方法。常见的 GitHub 读取场景使用 `GET`。

### headers [Map]

额外的 HTTP 请求头。除非你确实想覆盖由 `access_token` 生成的认证头，否则不要在这里配置 `Authorization`。

### params [Map]

HTTP 查询参数，例如 `per_page`、`page`、`since` 或其他 GitHub API 参数。

### body [String]

HTTP 请求体。只有目标 API 接口支持请求体时才需要配置。

### format [String]

响应数据格式，支持 `json` 和 `text`。如果希望输出带字段名的数据行，请使用 `json` 并配置 `schema`。

### schema [Config]

当 `format = "json"` 时，用于定义输出行结构。更多信息请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。

### json_field [Config]

把输出字段映射到 JSONPath 表达式。需要从嵌套 JSON 中取值时，可与 `schema` 一起使用。

### content_field [String]

用于先截取 JSON 片段的 JSONPath 表达式，例如 `$.items[*]`。

### pageing [Config]

继承自 HTTP 连接器的分页配置。任务配置中请保持 `pageing` 这个拼写。必要时可配合 `page_type = "Cursor"` 使用游标分页。

### page_type [String]

分页类型，支持 `PageNumber`（默认）和 `Cursor`。对于响应中包含 `next` 游标的接口，请使用 `Cursor`。

### cursor_field [String]

携带游标值的请求参数名称，与 `page_type = "Cursor"` 一起使用。

### cursor_response_field [String]

响应体中游标所在的 JSONPath，与 `page_type = "Cursor"` 一起使用。

### poll_interval_millis [int]

流处理任务中的请求间隔，单位毫秒。批处理任务只读取一次后结束。

### retry [int]

HTTP 请求因 `IOException` 失败时的最大重试次数。

### retry_backoff_multiplier_ms [int]

重试退避时间乘数，单位毫秒。

### retry_backoff_max_ms [int]

最大重试退避时间，单位毫秒。

### enable_multi_lines [boolean]

是否启用多行模式，将响应体中按换行分隔的多个 JSON 对象视为独立记录。

### keep_params_as_form [boolean]

是否将请求参数作为表单参数发送，而不是 URL 查询参数。

### keep_page_param_as_http_param [boolean]

分页时是否将分页参数保留在 URL 中，而不是在请求体内替换。

### batch_size [int]

当总页数未知时，每次请求返回的记录数。

### start_page_number [long]

从哪一页开始读取。

### total_page_size [long]

要读取的总页数。`0` 表示按照 `batch_size` 一直读取，直到 API 不再返回新页。

### use_placeholder_replacement [boolean]

是否使用 `${field}` 占位符替换 headers、params 和 body 中的字段值，否则按键名替换。

### connect_timeout_ms [int]

HTTP 连接超时时间（毫秒），默认 12000ms。

### socket_timeout_ms [int]

HTTP 套接字超时时间（毫秒），默认 60000ms。

### json_filed_missed_return_null [boolean]

设置为 `true` 时，JSON 字段缺失会返回 `null`；否则字段缺失会报错。

### common options

源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md)。

## 使用提示

- `access_token` 是敏感信息，请避免在共享的任务文件中硬编码真实令牌。可使用 SeaTunnel 变量替换或部署平台的密钥管理机制。
- 连接器始终会根据 `access_token` 添加 `Authorization: Bearer <access_token>` 请求头，请把其他自定义请求头放在 `headers` 中。
- 需要按字段读取时，把 `format` 设置为 `json` 并配置 `schema`。
- 当 GitHub 把记录嵌套在数组中时，使用 `content_field` 抽取数组元素。
- 使用页码分页时，保持 `page_type = "PageNumber"`，并通过 `params` 配置 `page` / `per_page`。
- 对于 GitHub Events 这类游标分页接口，请设置 `page_type = "Cursor"` 并配置 `cursor_field` 与 `cursor_response_field`。

## 任务示例

### 读取 GitHub 组织下的仓库

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Github {
    url = "https://api.github.com/orgs/apache/repos"
    access_token = "ghp_xxxxxxxxxxxx"
    method = "GET"
    format = "json"
    schema = {
      fields {
        id = int
        name = string
        description = string
        html_url = string
        stargazers_count = int
        forks = int
      }
    }
  }
}

sink {
  Console {
  }
}
```

### 读取分页的 GitHub API 结果

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Github {
    url = "https://api.github.com/orgs/apache/repos"
    access_token = "ghp_xxxxxxxxxxxx"
    method = "GET"
    params = {
      per_page = "100"
      page = "${page}"
    }
    pageing = {
      page_field = "page"
      total_page_size = 5
      start_page_number = 1
      use_placeholder_replacement = true
    }
    format = "json"
    schema = {
      fields {
        id = int
        name = string
        html_url = string
      }
    }
  }
}
```

### 流式读取 GitHub 事件

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  Github {
    url = "https://api.github.com/orgs/apache/events"
    access_token = "ghp_xxxxxxxxxxxx"
    method = "GET"
    format = "json"
    poll_interval_millis = 60000
    schema = {
      fields {
        id = string
        type = string
        created_at = string
      }
    }
  }
}
```

## 变更日志

<ChangeLog />