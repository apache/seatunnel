import ChangeLog from '../changelog/connector-http-notion.md';

# Notion

> Notion 源连接器

## 描述

用于从 Notion 读取数据。

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [ ] [流](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行性](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)

## 选项

| 参数名 | 类型 | 必须 | 默认值 | 描述 |
|--------|------|------|--------|------|
| url | String | 是 | - | HTTP 请求 URL |
| password | String | 是 | - | API 密钥用于登录 |
| version | String | 是 | - | Notion API 版本 |
| method | String | 否 | get | HTTP 请求方法，仅支持 GET、POST 方法 |
| schema.fields | Config | 否 | - | 上游数据的模式字段 |
| format | String | 否 | json | 上游数据的格式，现在仅支持 `json` `text`，默认 `json`。 |
| params | Map | 否 | - | HTTP 参数 |
| body | String | 否 | - | HTTP 请求体 |
| json_field | Config | 否 | - | JSON 字段配置 |
| content_json | String | 否 | - | 内容 JSON 配置 |
| poll_interval_millis | int | 否 | - | 流模式下请求 HTTP API 的间隔（毫秒） |
| retry | int | 否 | - | 如果 HTTP 请求返回 `IOException` 的最大重试次数 |
| retry_backoff_multiplier_ms | int | 否 | 100 | HTTP 请求失败时的重试退避倍数（毫秒） |
| retry_backoff_max_ms | int | 否 | 10000 | HTTP 请求失败时的最大重试退避时间（毫秒） |
| enable_multi_lines | boolean | 否 | false | 是否启用多行模式 |
| common-options | config | 否 | - | 源插件通用参数 |

### url [String]

HTTP 请求 URL

### password [String]

API 密钥用于登录，您可以在以下链接获取更多详情：

https://developers.notion.com/docs/authorization

### version [String]

Notion API 是版本化的。API 版本以发布版本的日期命名

### method [String]

HTTP 请求方法，仅支持 GET、POST 方法

### params [Map]

HTTP 参数

### body [String]

HTTP 请求体

### poll_interval_millis [int]

流模式下请求 HTTP API 的间隔（毫秒）

### retry [int]

如果 HTTP 请求返回 `IOException` 的最大重试次数

### retry_backoff_multiplier_ms [int]

HTTP 请求失败时的重试退避倍数（毫秒）

### retry_backoff_max_ms [int]

HTTP 请求失败时的最大重试退避时间（毫秒）

### format [String]

上游数据的格式，现在仅支持 `json` `text`，默认 `json`。

### schema [Config]

#### fields [Config]

上游数据的模式字段。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。

### content_json [String]

此参数可以获取一些 JSON 数据。

### json_field [Config]

此参数帮助您配置模式，因此此参数必须与 schema 一起使用。

## 变更日志

<ChangeLog />

