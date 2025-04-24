import ChangeLog from '../changelog/connector-http-github.md';

# Github

> Github 源连接器

## 描述

用于从 Github 读取数据。

## 关键特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [ ] [精确一次](../../concept/connector-v2-features.md)
- [ ] [列投影](../../concept/connector-v2-features.md)
- [ ] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../concept/connector-v2-features.md)

## 选项

| 名称                      | 类型     | 必填 | 默认值  |
|---------------------------|----------|------|--------|
| url                       | String   | 是   | -      |
| access_token              | String   | 否   | -      |
| method                    | String   | 否   | get    |
| schema.fields             | Config   | 否   | -      |
| format                    | String   | 否   | json   |
| params                    | Map      | 否   | -      |
| body                      | String   | 否   | -      |
| json_field                | Config   | 否   | -      |
| content_json              | String   | 否   | -      |
| poll_interval_millis      | int      | 否   | -      |
| retry                     | int      | 否   | -      |
| retry_backoff_multiplier_ms | int    | 否   | 100    |
| retry_backoff_max_ms      | int      | 否   | 10000  |
| enable_multi_lines        | boolean  | 否   | false  |
| common-options            | config   | 否   | -      |

### url [String]

HTTP 请求 URL。

### access_token [String]

GitHub个人访问令牌，请参阅：[创建个人访问令牌 - Github文档](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token)

### method [String]

HTTP 请求方法。目前支持 `GET` 和 `POST`。

### params [Map]

http 参数

### body [String]

HTTP 请求体

### poll_interval_millis [int]

流模式下请求 API 的间隔时间（毫秒）。

### retry [int]

请求失败（`IOException`）时最大重试次数。

### retry_backoff_multiplier_ms [int]

请求失败时的退避时间（毫秒）乘数。

### retry_backoff_max_ms [int]

请求失败时的最大退避时间（毫秒）。

### format [String]

上游数据的格式，现在仅支持`json` `text`，默认是`json`。

若你的数据格式为 `json`，需同时配置 schema 选项，例如：

上游数据如下：

```json
{
  "code": 200,
  "data": "get success",
  "success": true
}
```

您应该配置 schema 为以下内容：

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

| code |    data     | success |
|------|-------------|---------|
| 200  | get success | true    |

若你设置格式为 `text`，连接器不会对上游数据做出任何改变，示例：

上游数据如下：

```json
{
  "code": 200,
  "data": "get success",
  "success": true
}
```

连接器将生成如下数据：

|                         content                          |
|----------------------------------------------------------|
| {"code":  200, "data":  "get success", "success":  true} |

### schema [Config]

#### fields [Config]

上游数据的字段定义。

### content_json [String]

该参数可用于提取一些 json 数据。如果你只需要 “book” 部分的数据，可以配置 `content_field = "$.store.book.*"`.

如果你的返回数据如下所示：

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

你可以配置 `content_field = "$.store.book.*"` 并且结果返回如下：

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

然后你可以通过更简单的 schema 配置获取所需的结果，例如：

```hocon
Http {
  url = "http://mockserver:1080/contentjson/mock"
  method = "GET"
  format = "json"
  content_field = "$.store.book.*"
  schema = {
    fields {
      category = string
      author = string
      title = string
      price = string
    }
  }
}
```

这是一个例子:

- 测试数据可参考此链接：[mockserver-config.json](../../../../seatunnel-e2e/seatunnel-connector-v2-e2e/connector-http-e2e/src/test/resources/mockserver-config.json)
- 任务配置示例可参考此链接：[http_contentjson_to_assert.conf](../../../../seatunnel-e2e/seatunnel-connector-v2-e2e/connector-http-e2e/src/test/resources/http_contentjson_to_assert.conf).

### json_field [Config]

该参数用于帮助你配置 schema，因此必须与 schema 一起使用。

如果你的数据如下所示：

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

你可以通过如下任务配置获取 “book” 部分的内容：

```hocon
source {
  Http {
    url = "http://mockserver:1080/jsonpath/mock"
    method = "GET"
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

- 测试数据可参考此链接：[mockserver-config.json](../../../../seatunnel-e2e/seatunnel-connector-v2-e2e/connector-http-e2e/src/test/resources/mockserver-config.json)
- 任务配置示例可参考此链接：[http_jsonpath_to_assert.conf](../../../../seatunnel-e2e/seatunnel-connector-v2-e2e/connector-http-e2e/src/test/resources/http_jsonpath_to_assert.conf).

### common options

源插件通用参数，请参考 [常用选项](../source-common-options.md)获取详细说明。

## 示例

```hocon
Github {
  url = "https://api.github.com/orgs/apache/repos"
  access_token = "xxxx"
  method = "GET"
  format = "json"
  schema = {
    fields {
      id = int
      name = string
      description = string
      html_url = string
      stargazers_count = int
      forks = int
    }
  }
}
```

## 变更日志

<ChangeLog />