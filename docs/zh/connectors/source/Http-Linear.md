import ChangeLog from '../changelog/connector-http-linear.md';

# Linear

> Linear source connector

## 支持哪些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)

## 描述

通过 HTTP 从 Linear API 读取数据。连接器会查询 Linear GraphQL 接口，并将返回的记录转换为数据行。

作业配置中的连接器名称为 `Linear`。

## 源选项

| 名称 | 类型 | 是否必填 | 默认值 | 描述 |
| --- | --- | --- | --- | --- |
| url | String | 是 | - | Linear API 接口地址，例如 `https://api.linear.app/graphql` |
| api_key | String | 是 | - | 用于认证的 Linear API Key |
| method | String | 否 | GET | Http 请求方法，仅支持 GET 和 POST。GraphQL 查询需要使用 `POST`。 |
| body | String | 否 | - | Http 请求体。对 Linear 而言即 GraphQL 查询字符串，例如 `{"query": "{ issues { nodes { id title } } }"}` |
| headers | Map | 否 | - | Http 请求头 |
| params | Map | 否 | - | Http 请求参数 |
| format | String | 否 | text | 上游数据格式，支持 `json`、`text`、`binary`，默认为 `text` |
| schema | Config | 否 | - | Http 与 SeaTunnel 的数据结构映射。当 `format = json` 时必填。更多细节请参考 [Schema Feature](../../introduction/concepts/schema-feature.md) |
| json_field | Config | 否 | - | 用于辅助配置 schema，必须与 `schema` 一起使用 |
| content_field | String | 否 | - | 提取 JSON 响应中的部分数据，例如 `$.data.issues.nodes` |
| retry | Int | 否 | - | Http 请求抛出 `IOException` 时的最大重试次数 |
| retry_backoff_multiplier_ms | Int | 否 | 100 | Http 请求失败时重试退避时间（毫秒）的乘数 |
| retry_backoff_max_ms | Int | 否 | 10000 | Http 请求失败时重试退避的最大时间（毫秒） |
| common-options | | 否 | - | Source 插件通用参数，详情请参考 [Source Common Options](../common-options/source-common-options.md) |

## 示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}
source {
  Linear {
    url = "https://api.linear.app/graphql"
    api_key = "your_linear_api_key"
    method = "POST"
    body = "{\"query\": \"{ issues { nodes { id title } } }\"}"
    plugin_output = "linear_data"
  }
}
sink {
  Console {
    plugin_input = "linear_data"
  }
}
```

## Changelog

<ChangeLog />
