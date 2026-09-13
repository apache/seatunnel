import ChangeLog from '../changelog/connector-http-posthog.md';

# PostHog

> PostHog 源连接器

## 描述

从 PostHog 读取一次 HogQL 查询结果。该连接器使用同步 Query API，并作为有界批处理源运行。

对于大规模历史数据导出，请使用 [PostHog Batch Exports](https://posthog.com/docs/cdp/batch-exports)，不要执行单个超大 HogQL 查询。

## 主要特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [ ] [流](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 源选项

| 参数名 | 类型 | 必须 | 默认值 | 描述 |
| --- | --- | --- | --- | --- |
| base_url | String | 否 | https://us.posthog.com | PostHog 实例基础 URL。PostHog 欧洲云使用 `https://eu.posthog.com`，自托管部署使用对应实例 URL。 |
| project_id | String | 是 | - | PostHog 项目 ID。 |
| api_key | String | 是 | - | 具有 `query:read` 权限的 PostHog 个人 API 密钥。 |
| query | String | 是 | - | 通过 PostHog Query API 执行的 HogQL 查询。 |
| schema | Config | 是 | - | 输出结构。每个字段名必须与 HogQL 返回的列名或别名匹配。 |
| headers | Map | 否 | - | 额外的 HTTP 请求头。连接器会设置认证头和 JSON 请求头。 |
| retry | int | 否 | 0 | I/O 失败后的最大 HTTP 请求尝试次数。 |
| retry_backoff_multiplier_ms | int | 否 | 100 | 重试退避乘数，单位毫秒。 |
| retry_backoff_max_ms | int | 否 | 10000 | 最大重试退避时间，单位毫秒。 |
| connect_timeout_ms | int | 否 | 12000 | HTTP 连接超时时间，单位毫秒。 |
| socket_timeout_ms | int | 否 | 60000 | HTTP 套接字超时时间，单位毫秒。 |
| common-options | Config | 否 | - | 源插件通用参数，详见[源通用选项](../common-options/source-common-options.md)。 |

## 使用提示

- 连接器只执行一次配置的查询，然后结束。请添加 `LIMIT` 和适当的时间过滤条件，确保结果可由一次 PostHog Query API 响应返回。
- HogQL 表达式可能生成默认列名。请使用 `AS` 别名，使每个返回列与 `schema` 字段匹配。
- 请通过 SeaTunnel 变量替换或部署平台的密钥管理机制提供 `api_key`，不要在共享配置中保存真实密钥。
- 连接器不使用 PostHog 已弃用的事件列表接口，也不会改写用户提供的 HogQL 查询。

## 任务示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  PostHog {
    base_url = "https://us.posthog.com"
    project_id = "12345"
    api_key = "${POSTHOG_API_KEY}"
    query = "SELECT event, distinct_id, timestamp FROM events WHERE timestamp >= now() - INTERVAL 1 DAY ORDER BY timestamp LIMIT 10000"
    schema = {
      fields {
        event = string
        distinct_id = string
        timestamp = timestamp
      }
    }
  }
}

sink {
  Console {
  }
}
```

<ChangeLog />
