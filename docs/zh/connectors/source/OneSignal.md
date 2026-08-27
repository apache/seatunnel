import ChangeLog from '../changelog/connector-http-onesignal.md';

# OneSignal

> OneSignal 源连接器

## 描述

OneSignal 源连接器用于从 OneSignal 的 REST API 读取数据。它基于 HTTP 源连接器实现，并把 `password` 自动转换为 `Authorization: Basic <password>` 请求头，因此不需要在 `headers` 中再手动配置 `Authorization`。

使用该连接器可以把 OneSignal 中的 App、Players、Segments、Notifications 等资源读取为 SeaTunnel 行数据。

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [ ] [流](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行性](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)

## 源选项

| 参数名                         | 类型      | 必须 | 默认值   | 描述                                                                                          |
|-----------------------------|---------|----|-------|---------------------------------------------------------------------------------------------|
| url                         | String  | 是  | -     | OneSignal REST API 地址，常见接口如 `https://onesignal.com/api/v1/apps`、`https://onesignal.com/api/v1/players`。          |
| password                    | String  | 是  | -     | OneSignal 用户 Auth Key，连接器会将其作为 `Authorization: Basic <password>` 请求头发送。可在 [OneSignal 账号与密钥](https://documentation.onesignal.com/docs/accounts-and-keys#user-auth-key) 中创建。 |
| method                      | String  | 否  | get   | HTTP 请求方法，支持 `GET` 和 `POST`。                                                                 |
| headers                     | Map     | 否  | -     | 额外的 HTTP 请求头。除非需要覆盖由 `password` 生成的头，否则不要在这里配置 `Authorization`。                                  |
| params                      | Map     | 否  | -     | HTTP 查询参数，例如 `limit`、`offset` 等 OneSignal API 参数。                                                  |
| body                        | String  | 否  | -     | HTTP 请求体，对支持 JSON 负载的接口有用。                                                                      |
| format                      | String  | 否  | json  | 响应格式，`json` 时需要配合 `schema`；`text` 时返回原始响应。                                                       |
| schema                      | Config  | 否  | -     | 输出数据结构，`format = "json"` 时必填。详见 [Schema 特性](../../introduction/concepts/schema-feature.md)。                 |
| schema.fields               | Config  | 否  | -     | 字段名与 SeaTunnel 数据类型，用于解析 JSON 响应。                                                                |
| json_field                  | Config  | 否  | -     | 字段级 JSONPath 映射，与 `schema` 配合使用。                                                                |
| content_field               | String  | 否  | -     | 在 `schema` 解析之前先通过 JSONPath 抽取一段 JSON，例如 `$.players[*]` 可展开列表响应。                                  |
| pageing                     | Config  | 否  | -     | HTTP 分页配置，继承自 HTTP 源连接器。OneSignal 分页接口通常使用 `page` / `per_page` 参数。                                |
| poll_interval_millis        | int     | 否  | -     | 流式任务下两次请求之间的间隔（毫秒）。批模式下连接器读取一次后即结束。                                                            |
| retry                       | int     | 否  | -     | HTTP 请求返回 `IOException` 时的最大重试次数。                                                              |
| retry_backoff_multiplier_ms | int     | 否  | 100   | HTTP 请求失败时的重试退避倍数（毫秒）。                                                                          |
| retry_backoff_max_ms        | int     | 否  | 10000 | HTTP 请求失败时的最大重试退避时间（毫秒）。                                                                        |
| enable_multi_lines          | boolean | 否  | false | 是否启用多行模式，将响应体中按换行分隔的多个 JSON 对象视为独立记录。                                                            |
| json_filed_missed_return_null | boolean | 否 | false | 配置的 JSON 字段缺失时是否返回 `null`，否则报错。                                                                 |
| common-options              | config  | 否  | -     | 源插件通用参数，详见 [源通用选项](../common-options/source-common-options.md)。                                       |

## 使用提示

- `password` 是敏感信息，请避免在共享的任务文件中硬编码真实密钥。可以使用 SeaTunnel 变量替换或部署平台的密钥管理机制。
- 连接器始终会根据 `password` 添加 `Authorization` 请求头，请把其他自定义请求头放在 `headers` 中。
- 需要按字段读取时，把 `format` 设置为 `json` 并配置 `schema`。
- 当 OneSignal 把记录嵌套在数组中（例如 `players` 列表）时，使用 `content_field` 抽取数组元素。
- 只有当不同字段位于不同 JSON 路径时，才需要使用 `json_field`。
- OneSignal 分页接口使用 `page` 和 `per_page` 查询参数，可以通过 `params` 与 `pageing` 配合来逐页读取。

## 任务示例

### 读取 App 列表

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  OneSignal {
    url = "https://onesignal.com/api/v1/apps"
    password = "<onesignal-user-auth-key>"
    method = "GET"
    format = "json"
    schema = {
      fields {
        id = string
        name = string
        gcm_key = string
        chrome_key = string
        site_name = string
        created_at = string
        updated_at = string
        players = int
        messageable_players = int
      }
    }
  }
}

sink {
  Console {
  }
}
```

### 读取 Players 列表（分页）

通过 `params` 配合 `pageing` 读取 OneSignal 分页接口：

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  OneSignal {
    url = "https://onesignal.com/api/v1/players"
    password = "<onesignal-user-auth-key>"
    method = "GET"
    params = {
      app_id = "<your-app-id>"
      limit = "50"
      offset = "0"
    }
    pageing = {
      page_field = "offset"
      start_page_number = 0
      page_step = 50
      total_page_size = 10
      use_placeholder_replacement = false
    }
    format = "json"
    content_field = "$.players[*]"
    schema = {
      fields {
        id = string
        identifier = string
        device_type = int
        sessions = int
        language = string
        game_version = string
      }
    }
  }
}
```

### 通过 JSONPath 抽取字段

当不同字段位于不同 JSON 路径时，使用 `json_field`：

```hocon
source {
  OneSignal {
    url = "https://onesignal.com/api/v1/apps"
    password = "<onesignal-user-auth-key>"
    method = "GET"
    format = "json"
    json_field = {
      id = "$.id"
      name = "$.name"
      players = "$.players"
      site_name = "$.site_name"
    }
    schema = {
      fields {
        id = string
        name = string
        players = int
        site_name = string
      }
    }
  }
}
```

## 变更日志

<ChangeLog />