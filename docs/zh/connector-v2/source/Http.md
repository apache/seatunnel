import ChangeLog from '../changelog/connector-http.md';

# Http

> Http 源连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [x] [流处理](../../concept/connector-v2-features.md)
- [ ] [精确一次](../../concept/connector-v2-features.md)
- [ ] [列投影](../../concept/connector-v2-features.md)
- [ ] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../concept/connector-v2-features.md)

## 描述

用于从 Http 读取数据。

## 支持的数据源信息

为了使用 Http 连接器，需要以下依赖项。
可以通过 install-plugin.sh 或从 Maven 中央仓库下载。

| 数据源 | 支持的版本 | 依赖 |
|--------|------------|------|
| Http   | 通用       | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-http) |

## 源选项

| 名称                          | 类型    | 是否必须 | 默认值      | 描述                                                                                                                                                                       |
|-------------------------------|---------|----------|-------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                           | String  | 是       | -           | Http 请求 URL。                                                                                                                                                                 |
| schema                        | Config  | 否       | -           | Http 和 seatunnel 数据结构映射                                                                                                                                                         |
| schema.fields                 | Config  | 否       | -           | 上游数据的 schema 字段                                                                                                                                                                |
| json_field                    | Config  | 否       | -           | 此参数帮助您配置 schema，因此此参数必须与 schema 一起使用。                                                                                         |
| pageing                       | Config  | 否       | -           | 此参数用于分页查询                                                                                                                                                         |
| pageing.page_field            | String  | 否       | -           | 此参数用于指定请求中的页面字段名称。它可以在 headers、params 或 body 中使用占位符，如 ${page_field}。                             |
| pageing.use_placeholder_replacement | Boolean | 否 | false | 如果为 true，则使用占位符替换（${field}）用于 headers、parameters 和 body 值，否则使用基于键的替换。                                                  |
| pageing.total_page_size       | Int     | 否       | -           | 此参数用于控制总页数                                                                                                                       |
| pageing.batch_size            | Int     | 否       | -           | 每个请求返回的批量大小，用于在总页数未知时确定是否继续                                                            |
| pageing.start_page_number     | Int     | 否       | 1           | 指定同步开始的页码                                                                                                                         |
| pageing.page_type             | String  | 否       | PageNumber  | 此参数用于指定页面类型，如果未设置则为 PageNumber，仅支持 `PageNumber` 和 `Cursor`。                                  |
| pageing.cursor_field          | String  | 否       | -           | 此参数用于指定请求参数中的游标字段名称。                                                                                       |
| pageing.cursor_response_field | String  | 否       | -           | 此参数指定从中检索游标的响应字段。                                                                                            |
| content_json                  | String  | 否       | -           | 此参数可以获取一些 json 数据。如果您只需要 'book' 部分的数据，配置 `content_field = "$.store.book.*"`。                                              |
| format                        | String  | 否       | text        | 上游数据的格式，目前仅支持 `json` `text`，默认为 `text`。                                                                                                      |
| method                        | String  | 否       | get         | Http 请求方法，仅支持 GET、POST 方法。                                                                                                                              |
| headers                       | Map     | 否       | -           | Http 头信息。                                                                                                                                                                     |
| params                        | Map     | 否       | -           | Http 参数。                                                                                                                                                                      |
| body                          | String  | 否       | -           | Http 请求体，程序将自动添加 http header application/json，body 是 jsonbody。                                                                                       |
| poll_interval_millis          | Int     | 否       | -           | 流模式下请求 http api 的间隔（毫秒）。                                                                                                                                 |
| retry                         | Int     | 否       | -           | 如果请求 http 返回 `IOException` 的最大重试次数。                                                                                                                      |
| retry_backoff_multiplier_ms   | Int     | 否       | 100         | 请求 http 失败时的重试退避时间（毫秒）乘数。                                                                                                                |
| retry_backoff_max_ms          | Int     | 否       | 10000       | 请求 http 失败时的最大重试退避时间（毫秒）                                                                                                                    |
| enable_multi_lines            | Boolean | 否       | false       |                                                                                                                                                                                   |
| connect_timeout_ms            | Int     | 否       | 12000       | 连接超时设置，默认 12 秒。                                                                                                                                          |
| socket_timeout_ms             | Int     | 否       | 60000       | Socket 超时设置，默认 60 秒。                                                                                                                                              |
| common-options                |         | 否       | -           | 源插件通用参数，请参考 [Source Common Options](../source-common-options.md) 获取详细信息                                                                 |
| keep_params_as_form           | Boolean | 否       | false       | 是否按照表单提交参数，用于兼容旧行为。当为 true 时，params 参数的值通过表单提交。 |
| keep_page_param_as_http_param | Boolean | 否       | false       | 是否将分页参数设置为 params。用于兼容旧行为。                                                                                          |
| json_filed_missed_return_null | Boolean | 否      | false        | 当 JSON 字段缺失时，设置为 true 并返回 null，否则返回错误。|

## 如何创建 Http 数据同步作业

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Http {
    plugin_output = "http"
    url = "http://mockserver:1080/example/http"
    method = "GET"
    format = "json"
    schema = {
      fields {
        c_map = "map<string, string>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_bytes = bytes
        c_date = date
        c_decimal = "decimal(38, 18)"
        c_timestamp = timestamp
        c_row = {
          C_MAP = "map<string, string>"
          C_ARRAY = "array<int>"
          C_STRING = string
          C_BOOLEAN = boolean
          C_TINYINT = tinyint
          C_SMALLINT = smallint
          C_INT = int
          C_BIGINT = bigint
          C_FLOAT = float
          C_DOUBLE = double
          C_BYTES = bytes
          C_DATE = date
          C_DECIMAL = "decimal(38, 18)"
          C_TIMESTAMP = timestamp
        }
      }
    }
  }
}

# 控制台打印读取的 Http 数据
sink {
  Console {
    parallelism = 1
  }
}
```

## 参数解释

### format

当您指定 format 为 `json` 时，您还应该指定 schema 选项，例如：

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

当您指定 format 为 `text` 时，连接器不会对上游数据做任何处理，例如：

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

### keep_params_as_form
为了兼容旧版本的 http。
当设置为 true 时，`<params>` 和 `<pageing>` 将以表单形式提交。
当设置为 false 时，`<params>` 将添加到 url 路径中，而 `<pageing>` 不会添加到 body 或表单中。它将替换 params 和 body 中的占位符。

### keep_page_param_as_http_param
是否将分页参数设置为 params。
当设置为 true 时，`<pageing>` 设置为 `<params>`。
当设置为 false 时，当页面字段存在于 `<body>` 或 `<params>` 中时，替换值。

当设置为 false 时，配置示例：
```hocon
body="""{"id":1,"page":"${page}"}"""
```

```hocon
params={
 page: "${page}"
}
```

### params
默认情况下，参数将添加到 url 路径中。
如果您需要保持旧版本行为，请检查 keep_params_as_form。

### body
HTTP body 用于在请求或响应中携带实际数据，包括 JSON、表单提交。

参考格式如下：
```hocon
body="{"id":1,"name":"setunnel"}"
```

对于表单提交，请按如下设置 content-type。
```hocon
headers {
    Content-Type = "application/x-www-form-urlencoded"
}
```

### content_json

此参数可以获取一些 json 数据。如果您只需要 'book' 部分的数据，配置 `content_field = "$.store.book.*"`。

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

您可以配置 `content_field = "$.store.book.*"` 并且返回的结果看起来像这样：

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

然后您可以使用更简单的 schema 获取所需的结果，如

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

这里是一个示例：

- 测试数据可以在此链接找到 [mockserver-config.json](seatunnel-e2e/seatunnel-connector-v2-e2e/connector-http-e2e/src/test/resources/mockserver-config.json)
- 任务配置请参考此链接 [http_contentjson_to_assert.conf](seatunnel-e2e/seatunnel-connector-v2-e2e/connector-http-e2e/src/test/resources/http_contentjson_to_assert.conf)。

### json_field

此参数帮助您配置 schema，因此此参数必须与 schema 一起使用。

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

您可以通过如下配置任务来获取 'book' 的内容：

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

- 测试数据可以在此链接找到 [mockserver-config.json](seatunnel-e2e/seatunnel-connector-v2-e2e/connector-http-e2e/src/test/resources/mockserver-config.json)
- 任务配置请参考此链接 [http_jsonpath_to_assert.conf](seatunnel-e2e/seatunnel-connector-v2-e2e/connector-http-e2e/src/test/resources/http_jsonpath_to_assert.conf)。

### pageing
当前支持的分页类型是 `PageNumber` 和 `Cursor`。
如果您需要使用分页，您需要配置 `pageing`。默认分页类型是 `PageNumber`。


#### 1. PageNumber
使用 `PageNumber` 分页时，您可以在 HTTP 请求的不同部分包含页面参数：

- **在 URL 参数中**：将页面参数添加到 `params` 部分
- **在请求体中**：在 `body` JSON 中包含页面参数
- **在头信息中**：将页面参数添加到 `headers` 部分

您可以使用占位符如 `${page}` 与 `use_placeholder_replacement = true` 来动态更新这些值。占位符可以以各种格式使用：

- 作为独立值：`"${page}"`
- 带前缀/后缀：`"10${page}"` 或 `"page-${page}"`
- 作为不带引号的数字：`${page}`（在 JSON 体中）
- 在嵌套 JSON 结构中：`{"pagination":{"page":${page}}}`

##### 示例 1：在 body 和 params 中使用页面参数

```hocon
source {
    Http {
      url = "http://localhost:8080/mock/queryData"
      method = "POST"
      format = "json"
      body="""{"id":1,"page":"${page}"}"""
      content_field = "$.data.*"
      params={
       page: "${page}"
      }
      pageing={
       #你可以不设置此参数，默认值是 PageNumber
       page_type="PageNumber"
       total_page_size=20
       page_field=page
       use_placeholder_replacement=true
       #当不知道 total_page_size 时使用 batch_size，如果读取大小<batch_size 则完成，否则继续
       #batch_size=10
      }
      schema = {
        fields {
          name = string
          age = string
        }
      }
    }
}
```

##### 示例 2：在 headers 中使用页面参数

```hocon
source {
    Http {
      url = "http://localhost:8080/mock/queryData"
      method = "GET"
      format = "json"
      headers={
        Page-Number = "${pageNo}"
        Authorization = "Bearer token-123"
      }
      pageing={
        page_field = pageNo
        start_page_number = 1
        batch_size = 10
        use_placeholder_replacement = true
      }
      schema = {
        fields {
          name = string
          age = string
        }
      }
    }
}
```

##### 示例 3：使用基于键的替换（不使用占位符）

```hocon
source {
    Http {
      url = "http://localhost:8080/mock/queryData"
      method = "GET"
      format = "json"
      params={
        page = "1"
      }
      pageing={
        page_field = page
        start_page_number = 1
        batch_size = 10
        use_placeholder_replacement = false
      }
      schema = {
        fields {
          name = string
          age = string
        }
      }
    }
}
```

##### 示例 4：在 headers 中使用带前缀的页码

```hocon
source {
    Http {
      url = "http://localhost:8080/mock/queryData"
      method = "GET"
      format = "json"
      headers = {
        Page-Number = "10${page}"  # 当 page=5 时将变为 "105"
        Authorization = "Bearer token-123"
      }
      pageing = {
        page_field = page
        start_page_number = 5
        batch_size = 10
        use_placeholder_replacement = true
      }
      schema = {
        fields {
          name = string
          age = string
        }
      }
    }
}
```

##### 示例 5：在 body 中使用不带引号的页码

```hocon
source {
    Http {
      url = "http://localhost:8080/mock/queryData"
      method = "POST"
      format = "json"
      body = """{"a":${page},"limit":10}"""  # 不带引号的数字
      pageing = {
        page_field = page
        start_page_number = 1
        batch_size = 10
        use_placeholder_replacement = true
      }
      schema = {
        fields {
          name = string
          age = string
        }
      }
    }
}
```

##### 示例 6：使用带页面参数的嵌套 JSON 结构

```hocon
source {
    Http {
      url = "http://localhost:8080/mock/queryData"
      method = "POST"
      format = "json"
      body = """{"pagination":{"page":${page},"size":10},"filters":{"active":true}}"""  # 嵌套结构
      pageing = {
        page_field = page
        start_page_number = 1
        total_page_size = 20
        use_placeholder_replacement = true
      }
      schema = {
        fields {
          name = string
          age = string
        }
      }
    }
}
```

#### 2. Cursor
`pageing.page_type` 参数必须设置为 `Cursor`。
`cursor_field` 是请求参数中游标的字段名称。
`cursor_response_field` 是响应数据中分页令牌字段的名称，我们应该将其添加到请求的分页字段中。
````hocon

source {
    Http {
      plugin_output = "http"
      url = "http://localhost:8080/mock/cursor_data"
      method = "GET"
      format = "json"
      content_field = "$.data.*"
      keep_page_param_as_http_param = true
      pageing ={
        page_type="Cursor"
        cursor_field ="cursor"
        cursor_response_field="$.paging.cursors.next"
      }
    schema = {
      fields {
        content=string
        id=int
        name=string
      }
    }
   json_field = {
    content = "$.data[*].content"
    id = "$.data[*].id"
    name = "$.data[*].name"
   }
  }
}

```

## 变更日志

<ChangeLog />
