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
| poll_interval_millis        | int     | 否   | -      |
| retry                       | int     | 否   | -      |
| retry_backoff_multiplier_ms | int     | 否   | 100    |
| retry_backoff_max_ms        | int     | 否   | 10000  |
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

继承自 HTTP 连接器的分页配置。任务配置中请保持 `pageing` 这个拼写。

### poll_interval_millis [int]

流处理任务中的请求间隔，单位毫秒。批处理任务只读取一次后结束。

### retry [int]

HTTP 请求因 `IOException` 失败时的最大重试次数。

### retry_backoff_multiplier_ms [int]

重试退避时间乘数，单位毫秒。

### retry_backoff_max_ms [int]

最大重试退避时间，单位毫秒。

### json_filed_missed_return_null [boolean]

设置为 `true` 时，JSON 字段缺失会返回 `null`；否则字段缺失会报错。

### common options

源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md)。

## 示例

读取 GitHub 组织下的仓库：

```hocon
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
```

读取分页的 GitHub API 结果：

```hocon
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

## 变更日志

<ChangeLog />
