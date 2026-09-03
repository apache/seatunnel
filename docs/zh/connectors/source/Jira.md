import ChangeLog from '../changelog/connector-http-jira.md';

# Jira

> Jira 源连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

用于从 Jira REST API 读取数据。连接器会根据 `email` 和 `api_token` 自动生成 Jira Basic 认证请求头，然后复用 HTTP Source 的能力解析返回结果。

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

:::tip

Jira Source 只支持批处理。如果作业以流模式运行，连接器会报错。

:::

## 选项

| 名称                        | 类型   | 是否必填 | 默认值 | 说明 |
|-----------------------------|--------|----------|--------|------|
| url                         | String | 是       | -      | Jira REST API 地址。 |
| email                       | String | 是       | -      | 用于 Basic 认证的 Jira 账号邮箱。 |
| api_token                   | String | 是       | -      | 用于 Basic 认证的 Jira API Token。 |
| method                      | String | 否       | GET    | HTTP 请求方法，支持 `GET` 和 `POST`。 |
| headers                     | Map    | 否       | -      | 额外的 HTTP 请求头。除非需要覆盖自动生成的 Jira 认证头，否则不要在这里配置 `Authorization`。 |
| params                      | Map    | 否       | -      | HTTP 查询参数。 |
| body                        | String | 否       | -      | HTTP 请求体，通常和 `POST` 一起使用。 |
| format                      | String | 否       | TEXT   | 返回内容格式。如果要用 `schema`、`json_field` 或 `content_field` 解析 JSON，请设置为 `json`。 |
| schema                      | Config | 否       | -      | 输出字段结构。`format = "json"` 时必须配置。 |
| json_field                  | Config | 否       | -      | 用 JSONPath 把返回字段映射到输出列，必须和 `schema` 一起使用。 |
| content_field               | String | 否       | -      | 用 JSONPath 选出需要按行解析的数组或对象。 |
| pageing                     | Config | 否       | -      | 分页配置，见 [分页](#分页)。 |
| poll_interval_millis        | int    | 否       | -      | 轮询间隔，单位毫秒。Jira Source 只支持批处理，因此这个配置不适用于 Jira 流作业。 |
| retry                       | int    | 否       | -      | 请求出现 `IOException` 时的最大重试次数。 |
| retry_backoff_multiplier_ms | int    | 否       | 100    | 重试退避时间倍数，单位毫秒。 |
| retry_backoff_max_ms        | int    | 否       | 10000  | 最大重试退避时间，单位毫秒。 |
| json_filed_missed_return_null | boolean | 否     | false  | `json_field` 中配置的字段缺失时，是否返回 `null`。 |
| common-options              | config | 否       | -      | 源连接器通用配置，见 [源通用选项](../common-options/source-common-options.md)。 |

### 认证

在 Atlassian 账号中创建 Jira API Token 后，配置：

- `email`：Jira 账号邮箱。
- `api_token`：Jira API Token。

连接器会自动生成 Basic 认证请求头。请不要在 `headers` 中再单独添加 `Authorization`，否则会覆盖掉自动生成
的认证头，导致认证失败。

如果 Jira 站点部署在反向代理之后，请把代理相关的请求头（如 `X-Atlassian-Token`）放到 `headers` 中，不要
尝试覆盖 Basic 认证头。

### 返回结果解析

`format` 默认值是 `TEXT`，会把完整响应作为一个 `content` 字段输出。

如果需要结构化输出，请配置 `format = "json"` 和 `schema`：

```hocon
format = "json"
schema = {
  fields {
    expand = string
    startAt = int
    maxResults = int
    total = string
  }
}
```

如果行数据在嵌套 JSON 节点里，用 `content_field` 选出对应节点。如果输出列需要从不同 JSONPath 中提取，用 `json_field`。

### 分页

目标 API 需要分页参数时，可以配置 `pageing`。

| 名称 | 类型 | 是否必填 | 默认值 | 说明 |
|------|------|----------|--------|------|
| total_page_size | long | 否 | 0 | 请求的总页数。 |
| batch_size | int | 否 | 100 | 每次请求返回的数据条数。 |
| start_page_number | long | 否 | 1 | 起始页码。 |
| page_field | String | 否 | page | 页码分页时，请求参数中的页码字段名。 |
| page_type | String | 否 | PageNumber | 分页类型，支持 `PageNumber` 和 `Cursor`。 |
| cursor_field | String | 否 | - | 游标分页时，请求参数中的游标字段名。 |
| cursor_response_field | String | 否 | - | 从响应中读取下一页游标的 JSONPath 字段。 |
| use_placeholder_replacement | boolean | 否 | false | 是否在请求头、参数和请求体中使用 `${field}` 占位符替换。 |

Jira REST 搜索接口（`/rest/api/3/search`）既支持 `startAt` / `maxResults` 这种偏移量分页，也支持基于
`nextPageToken` 的游标分页。请根据目标端点实际支持的形式选择 —— 大多数云端接口仍然接受旧的偏移量分页，
而较新的接口只接受游标分页。

## 示例

### 通过 REST 搜索接口读取 Jira Issues

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jira {
    plugin_output = "jira"
    url = "https://example.atlassian.net/rest/api/3/search"
    email = "admin@example.com"
    api_token = "replace-with-token"
    method = "GET"
    format = "json"
    schema = {
      fields {
        expand = string
        startAt = int
        maxResults = int
        total = string
      }
    }
  }
}

sink {
  Console {
    plugin_input = "jira"
  }
}
```

### 读取支持游标分页的 Jira 端点

对于只支持游标分页的接口（例如较新的 comment、sprint 接口），把 `page_type` 切换为 `Cursor`，并把
`cursor_response_field` 指向响应中返回下一页游标的字段。连接器会一直翻页，直到响应里找不到游标为止。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jira {
    plugin_output = "jira_paged"
    url = "https://example.atlassian.net/rest/api/3/search"
    email = "admin@example.com"
    api_token = "replace-with-token"
    method = "POST"
    headers = {
      "Content-Type" = "application/json"
    }
    body = "{\"jql\":\"project = DEMO AND created >= -30d\",\"maxResults\":100,\"fields\":[\"summary\",\"status\"]}"
    format = "json"
    content_field = "$.issues.*"
    pageing = {
      page_type = "Cursor"
      cursor_field = "nextPageToken"
      cursor_response_field = "$.nextPageToken"
      batch_size = 100
    }
    schema = {
      fields {
        id = string
        key = string
        summary = string
        status_name = string
      }
    }
  }
}

sink {
  Console {
    plugin_input = "jira_paged"
  }
}
```

### 通过 JQL + `content_field` 发起 POST 请求

Jira 较新的搜索接口接受 JSON 形式的 JQL，请用 `POST` 提交。当 JQL 较长或包含需要 URL 编码的字符时，这种方式
比把它们拼到 `params` 里更可靠。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jira {
    plugin_output = "jira_jql"
    url = "https://example.atlassian.net/rest/api/3/search"
    email = "admin@example.com"
    api_token = "replace-with-token"
    method = "POST"
    body = "{\"jql\":\"project = DEMO AND created >= -7d\"}"
    format = "json"
    content_field = "$.issues.*"
    schema = {
      fields {
        id = string
        key = string
        summary = string
      }
    }
  }
}

sink {
  Console {
    plugin_input = "jira_jql"
  }
}
```

## 变更日志

<ChangeLog />
