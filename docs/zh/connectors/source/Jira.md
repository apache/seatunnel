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
| headers                     | Map    | 否       | -      | 额外的 HTTP 请求头。连接器会用 `email` 和 `api_token` 生成的 Basic 认证头覆盖其中的 `Authorization`。 |
| params                      | Map    | 否       | -      | HTTP 查询参数。 |
| body                        | String | 否       | -      | HTTP 请求体，通常和 `POST` 一起使用。 |
| format                      | String | 否       | TEXT   | 返回内容格式。如果要用 `schema`、`json_field` 或 `content_field` 解析 JSON，请设置为 `json`。 |
| schema                      | Config | 否       | -      | 输出字段结构。`format = "json"` 时必须配置。 |
| json_field                  | Config | 否       | -      | 用 JSONPath 把返回字段映射到输出列，必须和 `schema` 一起使用。 |
| content_field               | String | 否       | -      | 用 JSONPath 选出需要按行解析的数组或对象。 |
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

连接器根据 `email` 和 `api_token` 自动生成 Basic 认证请求头。如果 `headers` 中也配置了 `Authorization`，
该值会被连接器生成的 Basic 认证头覆盖。

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

### 分页限制

当前 Jira reader 没有把分页状态传给 `HttpSourceReader`。每个 BATCH 任务只读取一个 HTTP 响应，
配置 `pageing` 不会继续读取后续页。应通过 API 过滤条件及 `maxResults` 将所需结果限定在一个响应内。
完整的多页导出需要单独实现分页链路并通过相应测试。

Jira Cloud 增强搜索使用 `/rest/api/3/search/jql`；还有更多结果时，API 会返回 `nextPageToken`，
但当前连接器不会自动提交该游标。因此，当响应包含下一页游标时，不能把下面的示例视为完整导出。
请参阅 [Atlassian 增强搜索参考](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-search/)。
自建 Jira 部署应使用其安装版本文档规定的端点。

## 示例

### 读取一个 Jira Cloud 搜索响应

此 GET 示例从单个响应中提取 Issue 的 `id` 和 `key`，不会遍历 `nextPageToken`。
如果 API 提示还有更多结果，应缩小 JQL 过滤范围。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jira {
    plugin_output = "jira"
    url = "https://example.atlassian.net/rest/api/3/search/jql"
    email = "admin@example.com"
    api_token = "replace-with-token"
    method = "GET"
    params = {
      jql = "project = DEMO AND created >= -30d ORDER BY created ASC"
      maxResults = "100"
    }
    format = "json"
    content_field = "$.issues.*"
    schema = {
      fields {
        id = string
        key = string
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

## 变更日志

<ChangeLog />
