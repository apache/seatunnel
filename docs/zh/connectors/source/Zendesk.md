import ChangeLog from '../changelog/connector-http-zendesk.md';

# Zendesk

> Zendesk 数据源连接器

## 描述

用于从 [Zendesk REST API](https://developer.zendesk.com/api-reference/) 读取数据。它使用 Zendesk 账号邮箱和 API token 进行认证（以 HTTP Basic `Authorization` 请求头发送），并将某个 Zendesk 接口（如 tickets、users、organizations）读取为 SeaTunnel 的行数据。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 选项

|            名称             |  类型   | 是否必填 |   默认值   |
|-----------------------------|---------|----------|-----------|
| url                         | String  | 是       | -         |
| email                       | String  | 是       | -         |
| api_token                   | String  | 是       | -         |
| method                      | String  | 否       | get       |
| schema                      | Config  | 否       | -         |
| format                      | String  | 否       | json      |
| params                      | Map     | 否       | -         |
| body                        | String  | 否       | -         |
| json_field                  | Config  | 否       | -         |
| content_json                | String  | 否       | -         |
| poll_interval_millis        | int     | 否       | -         |
| retry                       | int     | 否       | -         |
| retry_backoff_multiplier_ms | int     | 否       | 100       |
| retry_backoff_max_ms        | int     | 否       | 10000     |
| enable_multi_lines          | boolean | 否       | false     |
| common-options              | config  | 否       | -         |

### url [String]

要读取的 Zendesk REST API 接口地址，例如 `https://your-subdomain.zendesk.com/api/v2/tickets.json`。

### email [String]

用于 API token 认证的 Zendesk 账号邮箱。它会与 `api_token` 组合为 `{email}/token:{api_token}` 并以 HTTP Basic `Authorization` 请求头发送。

### api_token [String]

Zendesk API token。获取方式请参考 [Zendesk API token 文档](https://support.zendesk.com/hc/en-us/articles/4408889192858)。

### method [String]

http 请求方法，仅支持 GET、POST 方法。

### schema [Config]

数据的结构，包括字段名称和字段类型。更多详情请参考 [Schema Feature](../../introduction/concepts/schema-feature.md)。

### format [String]

上游数据的格式，目前仅支持 `json` 和 `text`，默认 `json`。

### params [Map]

http 请求参数。

### json_field [Config]

该参数用于配置 schema，因此必须与 schema 一起使用。它将响应中的 JSON 路径映射到 schema 字段。详情和示例请参考 [Http source](./Http.md) 连接器。

### content_json [String]

该参数可以在映射为行之前，提取 JSON 响应中的某个子部分（例如顶层键 `tickets` 或 `users` 下的数组）。详情和示例请参考 [Http source](./Http.md) 连接器。

### common options

数据源插件通用参数，详情请参考 [Source Common Options](../common-options/source-common-options.md)。

## 示例

```hocon
source {
  Zendesk {
    url = "https://your-subdomain.zendesk.com/api/v2/tickets.json"
    email = "agent@example.com"
    api_token = "${ZENDESK_API_TOKEN}"
    method = "GET"
    format = "json"
    content_json = "$.tickets.*"
    schema = {
      fields {
        id = bigint
        subject = string
        status = string
        priority = string
        created_at = string
        updated_at = string
      }
    }
  }
}
```

## 变更日志

<ChangeLog />
