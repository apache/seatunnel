import ChangeLog from '../changelog/connector-http-klaviyo.md';

# Klaviyo

> Klaviyo 源连接器

## 描述

用于从 Klaviyo 读取数据。

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [ ] [流](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行性](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)

## 选项

| 参数名                         | 类型      | 必须 | 默认值   | 描述                                                                                                         |
|-----------------------------|---------|----|-------|------------------------------------------------------------------------------------------------------------|
| url                         | String  | 是  | -     | HTTP 请求 URL                                                                                                |
| private_key                 | String  | 是  | -     | 用于登录的 API 私钥，您可以在此链接获取更多详情：https://developers.klaviyo.com/en/docs/authenticate_#private-key-authentication |
| revision                    | String  | 是  | -     | API 端点版本（格式：YYYY-MM-DD）                                                                                    |
| method                      | String  | 否  | get   | HTTP 请求方法，仅支持 GET、POST 方法                                                                                  |
| schema                      | Config  | 否  | -     | 上游数据的模式。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。                                |
| schema.fields               | Config  | 否  | -     | 上游数据的模式字段                                                                                                  |
| format                      | String  | 否  | json  | 上游数据的格式，现在仅支持 `json` `text`，默认 `json`。                                                                     |
| params                      | Map     | 否  | -     | HTTP 参数                                                                                                    |
| body                        | String  | 否  | -     | HTTP 请求体                                                                                                   |
| json_field                  | Config  | 否  | -     | JSON 字段配置                                                                                                  |
| content_json                | String  | 否  | -     | 内容 JSON 字段                                                                                                 |
| poll_interval_millis        | int     | 否  | -     | 流模式下请求 HTTP API 的间隔（毫秒）                                                                                    |
| retry                       | int     | 否  | -     | 如果 HTTP 请求返回 `IOException` 时的最大重试次数                                                                        |
| retry_backoff_multiplier_ms | int     | 否  | 100   | HTTP 请求失败时的重试退避倍数（毫秒）                                                                                      |
| retry_backoff_max_ms        | int     | 否  | 10000 | HTTP 请求失败时的最大重试退避时间（毫秒）                                                                                    |
| enable_multi_lines          | boolean | 否  | false | 启用多行                                                                                                       |
| common-options              | config  | 否  | -     | 源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md) 详见。                                        |

### url [String]

HTTP 请求 URL

### private_key [String]

用于登录的 API 私钥，您可以在此链接获取更多详情：

https://developers.klaviyo.com/en/docs/authenticate_#private-key-authentication

### revision [String]

API 端点版本（格式：YYYY-MM-DD）

### method [String]

HTTP 请求方法，仅支持 GET、POST 方法

### params [Map]

HTTP 参数

### body [String]

HTTP 请求体

### poll_interval_millis [int]

流模式下请求 HTTP API 的间隔（毫秒）

### retry [int]

如果 HTTP 请求返回 `IOException` 时的最大重试次数

### retry_backoff_multiplier_ms [int]

HTTP 请求失败时的重试退避倍数（毫秒）

### retry_backoff_max_ms [int]

HTTP 请求失败时的最大重试退避时间（毫秒）

### format [String]

上游数据的格式，现在仅支持 `json` `text`，默认 `json`。

当您指定格式为 `json` 时，您还应该指定 schema 选项，例如：

上游数据如下：

```json
{
  "code": 200,
  "data": "get success",
  "success": true
}
```

您应该指定 schema 如下：

```hocon
schema {
    fields {
        code = int
        data = string
        success = boolean
    }
}
```

连接器将生成如下数据：

| code | data | success |
|------|------|---------|
| 200 | get success | true |

当您指定格式为 `text` 时，连接器将对上游数据不做任何处理，例如：

上游数据如下：

```json
{
  "code": 200,
  "data": "get success",
  "success": true
}
```

连接器将生成如下数据：

| content |
|---------|
| {"code": 200, "data": "get success", "success": true} |

### schema [Config]

#### fields [Config]

上游数据的模式字段。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。

### content_json [String]

此参数可以获取一些 JSON 数据。如果您只需要 'book' 部分中的数据，请配置 `content_field = "$.store.book.*"`。

### json_field [Config]

此参数帮助您配置模式，因此此参数必须与 schema 一起使用。

### common options

源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md) 详见。

## 示例

```hocon
Klaviyo {
    url = "https://a.klaviyo.com/api/lists/"
    private_key = "SeaTunnel-test"
    revision = "2020-10-17"
    method = "GET"
    format = "json"
    schema = {
          fields {
            type = string
            id = string
            attributes = {
                  name = string
                  created = string
                  updated = string
            }
            links = {
                  self = string
            }
          }
    }
}
```

## 变更日志

<ChangeLog />

