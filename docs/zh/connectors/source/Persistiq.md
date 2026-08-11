import ChangeLog from '../changelog/connector-http-persistiq.md';

# Persistiq

> Persistiq 源连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

用于从 Persistiq 读取数据。

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [模式投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 选项

| 参数名                         | 类型      | 是否必填 | 默认值 | 描述                                                                                          |
|-----------------------------|---------|------|-------|---------------------------------------------------------------------------------------------|
| url                         | String  | 是   | -     | Persistiq API 端点 URL，例如 `https://api.persistiq.com/v1/users`。                                            |
| password                    | String  | 是   | -     | Persistiq API Key，在 Persistiq 账号后台创建。Persistiq 使用 API Key 而不是用户名/密码组合；连接器会把它作为 Basic 认证的密码（用户名按 Persistiq 约定留空）。 |
| method                      | String  | 否   | GET   | HTTP 请求方法，仅支持 `GET`、`POST`。                                                                       |
| schema                      | Config  | 否   | -     | 输出字段结构，`format = "json"` 时必须配置。详见 [Schema 特性](../../introduction/concepts/schema-feature.md)。                            |
| schema.fields               | Config  | 否   | -     | `schema` 的字段定义。                                                                                 |
| format                      | String  | 否   | TEXT  | 返回内容格式，支持 `json`、`text`，默认 `TEXT`（把整个响应作为单个 `content` 字段）。                                                 |
| params                      | Map     | 否   | -     | 追加到 URL 的 HTTP 查询参数。                                                                              |
| body                        | String  | 否   | -     | HTTP 请求体。需要发送 JSON payload 时配合 `POST` 一起使用。                                                          |
| json_field                  | Config  | 否   | -     | 用 JSONPath 把返回字段映射到输出列，必须和 `schema` 一起使用。                                                              |
| content_field               | String  | 否   | -     | 用 JSONPath 选出需要按行解析的数组或对象。                                                                          |
| pageing                     | Config  | 否   | -     | 分页配置，见 [分页](#分页)。                                                                                |
| poll_interval_millis        | int     | 否   | -     | 轮询间隔（毫秒），仅在 `STREAMING` 模式下生效。Persistiq 源主要为批处理设计，但可以以流模式周期性轮询 API。                                       |
| retry                       | int     | 否   | -     | HTTP 请求返回 `IOException` 时的最大重试次数。                                                                  |
| retry_backoff_multiplier_ms | int     | 否   | 100   | 请求失败时重试退避时间的倍数（毫秒）。                                                                              |
| retry_backoff_max_ms        | int     | 否   | 10000 | 请求失败时的最大重试退避时间（毫秒）。                                                                              |
| enable_multi_lines          | boolean | 否   | false | `format = "text"` 时，是否允许响应体里包含用换行分隔的多个 JSON 对象。                                                       |
| connect_timeout_ms          | int     | 否   | 12000 | TCP 连接超时（毫秒）。                                                                                   |
| socket_timeout_ms           | int     | 否   | 60000 | Socket 读超时（毫秒）。                                                                                 |
| json_filed_missed_return_null | boolean | 否 | false | `json_field` 中配置的字段在响应里缺失时，是否返回 `null`。                                                                |
| common-options              | config  | 否   | -     | 源插件通用参数，详见 [源通用选项](../common-options/source-common-options.md)。                                                  |

### url [String]

HTTP 请求 URL。

### password [String]

API 密钥用于登录，您可以在 Persistiq 网站获取。

### method [String]

HTTP 请求方法，仅支持 GET、POST 方法。

### params [Map]

HTTP 参数。

### body [String]

HTTP 请求体。

### poll_interval_millis [int]

流模式下请求 HTTP API 的间隔（毫秒）。

### retry [int]

如果 HTTP 请求返回 `IOException` 的最大重试次数。

### retry_backoff_multiplier_ms [int]

HTTP 请求失败时的重试退避倍数（毫秒）。

### retry_backoff_max_ms [int]

HTTP 请求失败时的最大重试退避时间（毫秒）。

### format [String]

上游数据的格式，现在仅支持 `json`、`text`，默认 `text`。

当 `format` 设置为 `json` 时，需要同时配置 `schema`，例如：

上游数据如下：

```json
{
  "code": 200,
  "data": "get success",
  "success": true
}
```

可以把 `schema` 配置成：

```hocon

schema {
    fields {
        code = int
        data = string
        success = boolean
    }
}

```

连接器会生成如下数据：

| code |    data     | success |
|------|-------------|---------|
| 200  | get success | true    |

当 `format` 设置为 `text` 时，连接器不会对上游数据做处理，例如上游数据为：

```json
{
  "code": 200,
  "data": "get success",
  "success": true
}
```

连接器会生成如下数据：

|                         content                          |
|----------------------------------------------------------|
| {"code":  200, "data":  "get success", "success":  true} |

### schema [Config]

#### fields [Config]

上游数据的模式字段。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。

### content_field [String]

使用 JSONPath 从响应里挑出要按行解析的子节点。例如只想取 `book` 部分的数据，可以配置
`content_field = "$.store.book.*"`。

如果返回数据如下：

```json
{
  "store": {
    "book": [
      {
        "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
      },
      {
        "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99
      }
    ],
    "bicycle": {
      "color": "red",
      "price": 19.95
    }
  },
  "expensive": 10
}
```

配置 `content_field = "$.store.book.*"` 后，会得到如下结果：

```json
[
  {
    "category": "reference",
    "author": "Nigel Rees",
    "title": "Sayings of the Century",
    "price": 8.95
  },
  {
    "category": "fiction",
    "author": "Evelyn Waugh",
    "title": "Sword of Honour",
    "price": 12.99
  }
]
```

此时可以用一个更简单的 `schema` 拿到目标字段，例如：

```hocon
source {
  Persistiq {
    url = "https://api.persistiq.com/v1/users"
    password = "your-api-key"
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
```

### json_field [Config]

此参数帮助您配置模式，因此此参数必须与 schema 一起使用。

如果响应数据如下：

```json
{
  "store": {
    "book": [
      {
        "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
      },
      {
        "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99
      }
    ],
    "bicycle": {
      "color": "red",
      "price": 19.95
    }
  },
  "expensive": 10
}
```

可以通过如下配置提取 `book` 中的内容：

```hocon
source {
  Persistiq {
    url = "https://api.persistiq.com/v1/users"
    password = "your-api-key"
    format = "json"
    json_field = {
      category = "$.store.book[*].category"
      author = "$.store.book[*].author"
      title = "$.store.book[*].title"
      price = "$.store.book[*].price"
    }
    schema = {
      fields {
        category = string
        author = string
        title = string
        price = string
      }
    }
  }
}
```

### 分页

Persistiq API 需要分页参数时使用 `pageing`。Persistiq 大多数端点使用 offset/limit 分页，默认的
`page_type = "PageNumber"` 通常就够了。

| 名称 | 类型 | 是否必填 | 默认值 | 说明 |
|------|------|----------|--------|------|
| total_page_size | long | 否 | 0 | 请求的总页数。 |
| batch_size | int | 否 | 100 | 每次请求返回的数据条数。 |
| start_page_number | long | 否 | 1 | 起始页码。 |
| page_field | String | 否 | page | 页码分页时，请求参数中的页码字段名。 |
| page_type | String | 否 | PageNumber | 分页类型，支持 `PageNumber` 和 `Cursor`。 |
| cursor_field | String | 否 | - | 游标分页时，请求参数中的游标字段名。 |
| cursor_response_field | String | 否 | - | 从响应中读取下一页游标的 JSONPath 字段。 |
| use_placeholder_replacement | boolean | 否 | false | 是否在请求头、参数和请求体中使用 `${field}` 占位符替换。 |

### 通用选项

源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md) 详见。

## 示例

### 从 Persistiq 读取用户列表

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Persistiq {
    url = "https://api.persistiq.com/v1/users"
    password = "your-api-key"
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
  Console {}
}
```

### 使用 `json_field` 投影分页结果

当行数据直接在响应根节点下、但只需要其中部分字段时，用 `json_field` 把 JSONPath 表达式投影到输出列。
这种方式无需声明较重的 `content_field`，在分页接口下表现也更好。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Persistiq {
    url = "https://api.persistiq.com/v1/users"
    password = "your-api-key"
    format = "json"
    pageing = {
      total_page_size = 50
      batch_size = 100
      page_field = "page"
      start_page_number = 1
    }
    json_field = {
      id = "$.users[*].id"
      name = "$.users[*].name"
      email = "$.users[*].email"
    }
    schema = {
      fields {
        id = string
        name = string
        email = string
      }
    }
  }
}

sink {
  Console {}
}
```

### 使用 STREAMING 模式轮询 Persistiq

Persistiq 不提供流式端点，但连接器仍然支持 `STREAMING` 模式，每隔 `poll_interval_millis` 毫秒轮询一次
API 并把结果行发往下游。Checkpoint 只跟踪已消费的偏移，不保存上游状态。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 60000
}

source {
  Persistiq {
    url = "https://api.persistiq.com/v1/users"
    password = "your-api-key"
    poll_interval_millis = 60000
    format = "json"
    content_field = "$.users.*"
    schema = {
      fields {
        id = string
        name = string
        email = string
      }
    }
  }
}

sink {
  Console {}
}
```

## 变更日志

<ChangeLog />
