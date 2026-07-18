import ChangeLog from '../changelog/connector-http-gitlab.md';

# Gitlab

> Gitlab 源连接器

## 描述

Gitlab 源连接器用于读取 GitLab REST API 数据。它基于 HTTP 源连接器实现，并会自动把 `access_token` 作为 GitLab `PRIVATE-TOKEN` 请求头发送。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
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

GitLab REST API 地址，例如 `https://gitlab.com/api/v4/projects`。

### access_token [String]

GitLab 个人访问令牌。连接器会把它写入 HTTP `PRIVATE-TOKEN` 请求头。

### method [String]

HTTP 请求方法。常见的 GitLab 读取场景使用 `GET`。

### headers [Map]

额外的 HTTP 请求头。除非你确实想覆盖由 `access_token` 生成的认证头，否则不要在这里配置 `PRIVATE-TOKEN`。

### params [Map]

HTTP 查询参数，例如 `per_page`、`page`、`owned` 或其他 GitLab API 参数。

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

该选项继承自 HTTP 连接器，但 Gitlab 源连接器当前只支持批处理模式。

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

读取 GitLab 项目：

```hocon
source {
  Gitlab {
    url = "https://gitlab.com/api/v4/projects"
    access_token = "glpat-xxxxxxxxxxxx"
    method = "GET"
    format = "json"
    schema = {
      fields {
        id = int
        description = string
        name = string
        name_with_namespace = string
        path = string
        http_url_to_repo = string
      }
    }
  }
}
```

读取分页的 GitLab API 结果：

```hocon
source {
  Gitlab {
    url = "https://gitlab.com/api/v4/projects"
    access_token = "glpat-xxxxxxxxxxxx"
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
        path = string
      }
    }
  }
}
```

## 变更日志

<ChangeLog />
