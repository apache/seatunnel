import ChangeLog from '../changelog/connector-http-gitlab.md';

# Gitlab

> Gitlab 源连接器

## 描述

用于从 Gitlab 读取数据。

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [ ] [流](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行性](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)

## 选项

|            参数名             |  类型   | 必须 | 默认值 |
|-----------------------------|---------|------|--------|
| url                         | String  | 是   | -      |
| access_token                | String  | 是   | -      |
| method                      | String  | 否   | get    |
| schema.fields               | Config  | 否   | -      |
| format                      | String  | 否   | json   |
| params                      | Map     | 否   | -      |
| body                        | String  | 否   | -      |
| json_field                  | Config  | 否   | -      |
| content_json                | String  | 否   | -      |
| poll_interval_millis        | int     | 否   | -      |
| retry                       | int     | 否   | -      |
| retry_backoff_multiplier_ms | int     | 否   | 100    |
| retry_backoff_max_ms        | int     | 否   | 10000  |
| enable_multi_lines          | boolean | 否   | false  |
| common-options              | config  | 否   | -      |

### url [String]

http 请求 url

### access_token [String]

个人访问令牌

### method [String]

http 请求方法，仅支持 GET、POST 方法

### params [Map]

http 参数

### body [String]

http 请求体

### poll_interval_millis [int]

在流模式下请求 http api 的间隔（毫秒）

### retry [int]

如果 http 请求返回 `IOException` 的最大重试次数

### retry_backoff_multiplier_ms [int]

如果 http 请求失败，重试退避时间（毫秒）乘数

### retry_backoff_max_ms [int]

如果 http 请求失败，最大重试退避时间（毫秒）

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

| code |    data     | success |
|------|-------------|---------|
| 200  | get success | true    |

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

|                         content                          |
|----------------------------------------------------------|
| {"code":  200, "data":  "get success", "success":  true} |

### schema [Config]

#### fields [Config]

上游数据的模式字段。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。

### content_json [String]

此参数可以获取一些 json 数据。如果您只需要 'book' 部分中的数据，请配置 `content_field = "$.store.book.*"`。

如果您的返回数据看起来像这样。

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

您可以配置 `content_field = "$.store.book.*"`，返回的结果看起来像这样：

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

然后您可以使用更简单的 schema 获得所需的结果，如

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

这是一个示例：

- 测试数据可以在此链接找到 [mockserver-config.json](../../../../seatunnel-e2e/seatunnel-connector-v2-e2e/connector-http-e2e/src/test/resources/mockserver-config.json)
- 查看此链接了解任务配置 [http_contentjson_to_assert.conf](../../../../seatunnel-e2e/seatunnel-connector-v2-e2e/connector-http-e2e/src/test/resources/http_contentjson_to_assert.conf)。

### json_field [Config]

此参数可帮助您配置 schema，因此此参数必须与 schema 一起使用。

如果您的数据看起来像这样：

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

您可以通过配置任务如下来获取 'book' 的内容：

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

- 测试数据可以在此链接找到 [mockserver-config.json](../../../../seatunnel-e2e/seatunnel-connector-v2-e2e/connector-http-e2e/src/test/resources/mockserver-config.json)
- 查看此链接了解任务配置 [http_jsonpath_to_assert.conf](../../../../seatunnel-e2e/seatunnel-connector-v2-e2e/connector-http-e2e/src/test/resources/http_jsonpath_to_assert.conf)。

### 通用选项

源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md) 详见

## 示例

```hocon
Gitlab{
    url = "https://gitlab.com/api/v4/projects"
    access_token = "xxxxx"
    schema {
       fields {
         id = int
         description = string
         name = string
         name_with_namespace = string
         path = string
         http_url_to_repo = string
       }
    }
}
```

## 变更日志

<ChangeLog />


