import ChangeLog from '../changelog/connector-http-airtable.md';

# Airtable

> Airtable Sink 连接器

## 描述

用于将数据写入 Airtable。

## 关键特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持多表写入](../../introduction/concepts/connector-v2-features.md)

## 选项

| 参数名 | 类型 | 必须 | 默认值 |
|--------|------|------|--------|
| token                       | String  | 是 | -             |
| base_id                     | String  | 是 | -             |
| table                       | String  | 是 | -             |
| api_base_url                | String  | 否 | https://api.airtable.com |
| typecast                    | boolean | 否 | false         |
| batch_size                  | int     | 否 | 10            |
| request_interval_ms         | int     | 否 | 220           |
| rate_limit_backoff_ms       | int     | 否 | 30000         |
| rate_limit_max_retries      | int     | 否 | 3             |
| common-options              |         | 否 | -             |

### token [String]

Airtable 个人访问令牌。可在 https://airtable.com/create/tokens 创建。

### base_id [String]

Airtable Base ID（以 `app` 开头）。

### table [String]

要写入的表名或表 ID。

### api_base_url [String]

Airtable API 基础 URL，默认 `https://api.airtable.com`。

### typecast [boolean]

如果为 true，Airtable 会自动将值转换为匹配的字段类型。默认 false。

### batch_size [int]

每次 API 请求的记录数，受 Airtable API 限制最大为 10。默认 10。

### request_interval_ms [int]

API 请求之间的最小间隔（毫秒），默认 220ms。

### rate_limit_backoff_ms [int]

收到 429（限流）响应时的基础退避时间（毫秒），默认 30000ms。

### rate_limit_max_retries [int]

收到 429 响应后的最大重试次数，默认 3。

### common options

汇插件通用参数，请参考 [Sink Common Options](../common-options/sink-common-options.md)。

## 示例

```hocon
sink {
  Airtable {
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Shipments"
    typecast = true
    batch_size = 10
  }
}
```

## 变更日志

<ChangeLog />
