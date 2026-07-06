import ChangeLog from '../changelog/connector-http-persistiq.md';

# Persistiq

> Persistiq 源连接器

## 描述

Persistiq 源连接器用于从 Persistiq HTTP API 读取数据。它基于 HTTP 源连接器实现，并会把 `password` 配置为 Persistiq 需要的 `x-api-key` 请求头。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [模式投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 支持的数据源信息

| 数据源 | 依赖 |
|--------|------|
| Persistiq | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-http-persistiq) |

## 源选项

| 名称 | 类型 | 是否必须 | 默认值 | 描述 |
|------|------|----------|--------|------|
| url | String | 是 | - | Persistiq API 请求 URL。 |
| password | String | 是 | - | Persistiq API Key。连接器会把它写入 `x-api-key` 请求头。 |
| method | String | 否 | GET | HTTP 请求方法，支持 `GET` 和 `POST`。 |
| schema | Config | 否 | - | SeaTunnel 数据结构。`format = json` 时必须配置。 |
| schema.fields | Config | 否 | - | 输出字段名称和类型。 |
| format | String | 否 | text | 响应格式，支持 `json`、`text` 和 `binary`。读取 Persistiq API 时通常使用 `json`。 |
| content_field | String | 否 | - | 用 JSONPath 先抽取某个 JSON 对象或数组，再按 `schema` 解析。 |
| json_field | Config | 否 | - | 单个输出字段的 JSONPath 映射，需要和 `schema` 一起使用。 |
| headers | Map | 否 | - | 额外 HTTP 请求头。连接器会根据 `password` 自动添加 API Key 请求头。 |
| params | Map | 否 | - | HTTP 查询参数。 |
| body | String | 否 | - | HTTP 请求体，通常和 `method = POST` 一起使用。 |
| pageing | Config | 否 | - | 分页配置。参数名需要保持 `pageing` 这个拼写。 |
| poll_interval_millis | Int | 否 | - | 流式作业的请求间隔，单位毫秒。 |
| retry | Int | 否 | - | 请求因 `IOException` 失败时的最大重试次数。 |
| retry_backoff_multiplier_ms | Int | 否 | 100 | 重试退避倍数，单位毫秒。 |
| retry_backoff_max_ms | Int | 否 | 10000 | 最大重试退避时间，单位毫秒。 |
| enable_multi_lines | Boolean | 否 | false | 是否按行拆分文本响应。 |
| connect_timeout_ms | Int | 否 | 12000 | HTTP 连接超时时间，单位毫秒。 |
| socket_timeout_ms | Int | 否 | 60000 | HTTP Socket 超时时间，单位毫秒。 |
| json_filed_missed_return_null | Boolean | 否 | false | JSON 字段缺失时是否返回 null；参数名需要保持 `json_filed_missed_return_null` 这个拼写。 |
| common-options | Config | 否 | - | 源插件通用参数，详见 [源通用选项](../common-options/source-common-options.md)。 |

## 参数说明

- `format = json` 时需要配置 `schema`；否则连接器会把响应内容作为单个 `content` 文本字段读取。
- Persistiq 列表接口通常会把记录包在外层对象里。可以配置 `content_field = "$.users.*"`，让每个用户对象输出为一行。
- 该连接器只有 Source，没有对应 Sink；不提供 CDC、多表分片发现或精确一次语义。
- 不建议把密钥直接写在共享配置文件里，生产环境优先使用部署平台的密钥管理能力。

## 示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Persistiq {
    plugin_output = "persistiq_users"
    url = "https://api.persistiq.com/v1/users"
    password = "YOUR_PERSISTIQ_API_KEY"
    method = "GET"
    format = "json"
    content_field = "$.users.*"
    schema = {
      fields {
        id = string
        name = string
        email = string
        activated = boolean
        default_mailbox_id = string
        salesforce_id = string
      }
    }
  }
}

sink {
  Console {
    plugin_input = "persistiq_users"
  }
}
```

## 变更日志

<ChangeLog />
