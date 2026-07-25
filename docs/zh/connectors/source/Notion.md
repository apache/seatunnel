import ChangeLog from '../changelog/connector-http-notion.md';

# Notion

> Notion 源连接器

## 描述

Notion 源连接器用于从 Notion API 读取数据。它基于 HTTP 源连接器实现，并会根据 `password` 和 `version` 配置自动添加 `Authorization: Bearer <password>` 与 `Notion-Version: <version>` 请求头。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义切分](../../introduction/concepts/connector-v2-features.md)

## 源选项

| 名称                          | 类型      | 是否必需 | 默认值   | 描述 |
|-----------------------------|---------|------|-------|------|
| url                         | String  | 是    | -     | Notion API 请求 URL，例如 `https://api.notion.com/v1/users`。 |
| password                    | String  | 是    | -     | Notion 集成 Token。连接器会把它作为 `Authorization` Bearer Token 发送。 |
| version                     | String  | 是    | -     | Notion API 版本，例如 `2022-06-28`。连接器会把它作为 `Notion-Version` 请求头发送。 |
| method                      | String  | 否    | get   | HTTP 请求方法。支持 `GET` 和 `POST`。 |
| headers                     | Map     | 否    | -     | 额外 HTTP 请求头。`Authorization` 和 `Notion-Version` 会由 `password` 与 `version` 设置。 |
| params                      | Map     | 否    | -     | 随请求发送的查询参数。 |
| body                        | String  | 否    | -     | HTTP 请求体，通常与 `method = "POST"` 一起使用。 |
| format                      | String  | 否    | text  | 响应格式。读取 Notion JSON 并转换为 SeaTunnel 字段时使用 `json`；返回原始响应内容时使用 `text`。 |
| schema                      | Config  | 否    | -     | 输出结构。`format = "json"` 时需要配置。 |
| schema.fields               | Config  | 否    | -     | 用于解析 JSON 响应的字段名和 SeaTunnel 数据类型。 |
| content_field               | String  | 否    | -     | JSONPath 表达式，用于先选取响应中的嵌套内容，再按 `schema` 解析。 |
| json_field                  | Config  | 否    | -     | 字段级 JSONPath 映射。当不同输出字段来自不同 JSON 路径时，与 `schema` 一起使用。 |
| pageing                     | Config  | 否    | -     | 继承自 HTTP 源连接器的分页配置。 |
| poll_interval_millis        | Int     | 否    | -     | 以流模式使用时，两次请求之间的间隔毫秒数。 |
| retry                       | Int     | 否    | -     | HTTP 请求发生 `IOException` 时的最大重试次数。 |
| retry_backoff_multiplier_ms | Int     | 否    | 100   | 重试退避时间倍数，单位毫秒。 |
| retry_backoff_max_ms        | Int     | 否    | 10000 | 最大重试退避时间，单位毫秒。 |
| json_filed_missed_return_null | Boolean | 否  | false | 配置的 JSON 字段不存在时返回 null。 |
| common-options              | Config  | 否    | -     | 源插件通用参数，详情请参考 [Source 通用选项](../common-options/source-common-options.md)。 |

:::tip 提示

`password` 是敏感的 Notion 集成 Token。请避免在共享作业文件中硬编码真实 Token，优先使用 SeaTunnel 变量替换或部署环境的密钥管理方式注入。

:::

## 使用说明

- 如果希望把 Notion JSON 响应解析成带类型的 SeaTunnel 行，请设置 `format = "json"` 并配置 `schema`。
- 当 Notion 响应把记录包在嵌套数组中时，可以使用 `content_field`，例如 `$.results.*`。
- 只有当每个输出字段都需要单独的 JSONPath 表达式时，才需要使用 `json_field`。
- `password` 和 `version` 会覆盖 `Authorization` 与 `Notion-Version` 请求头，`headers` 中只需要放其他自定义请求头。

## 任务示例

### 读取用户列表

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Notion {
    url = "https://api.notion.com/v1/users"
    password = "<notion-integration-token>"
    version = "2022-06-28"
    method = "GET"
    format = "json"
    content_field = "$.results.*"
    schema = {
      fields {
        object = string
        id = string
        type = string
        person = {
          email = string
        }
        name = string
        avatar_url = string
      }
    }
  }
}

sink {
  Console {
  }
}
```

### 使用 JSONPath 提取字段

```hocon
source {
  Notion {
    url = "https://api.notion.com/v1/users"
    password = "<notion-integration-token>"
    version = "2022-06-28"
    method = "GET"
    format = "json"
    json_field = {
      id = "$.results[*].id"
      type = "$.results[*].type"
      name = "$.results[*].name"
    }
    schema = {
      fields {
        id = string
        type = string
        name = string
      }
    }
  }
}
```

## 变更日志

<ChangeLog />
