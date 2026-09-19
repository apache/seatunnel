import ChangeLog from '../changelog/connector-http-myhours.md';

# My Hours

> My Hours 源连接器

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

用于通过 My Hours REST API 读取数据。连接器会先使用配置的 `email` 和 `password` 登录获取访问令牌，
然后在后续请求中自动携带该令牌。

My Hours 连接器与其他基于 HTTP 的源连接器共用同一套 HTTP 请求、重试和分页能力。把 `email` 和 `password` 配置为 My Hours 账号，再把 `url` 指向要调用的接口。

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

:::tip

在流模式下，连接器会反复请求配置的接口。可以用 `poll_interval_millis` 控制请求间隔。

:::

## 支持的数据源信息

为了使用 My Hours 连接器，需要以下依赖项。
可以通过 install-plugin.sh 或从 Maven 中央存储库下载。

| 数据源 | 支持的版本 | 依赖 |
|--------|-----------|------|
| My Hours | universal | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-http-base) |

## 源选项

| 参数名                         | 类型      | 必须 | 默认值 | 描述                                                                                               |
|-------------------------------|---------|------|--------|----------------------------------------------------------------------------------------------------|
| url                           | String  | 是   | -      | My Hours API 请求 URL。                                                                            |
| email                         | String  | 是   | -      | My Hours 登录邮箱。                                                                                |
| password                      | String  | 是   | -      | My Hours 登录密码。                                                                                |
| schema                        | Config  | 否   | -      | 当 `format` 为 `json` 时需要配置。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。 |
| schema.fields                 | Config  | 否   | -      | 上游数据字段。                                                                                      |
| json_field                    | Config  | 否   | -      | 通过 JSONPath 从响应中抽取字段，需要与 `schema` 一起使用。                                           |
| content_field                 | String  | 否   | -      | 在解析 schema 前抽取 JSON 响应中的一部分，例如 `$.store.book.*`。                                  |
| format                        | String  | 否   | text   | 响应格式，支持 `json` 和 `text`。使用 `schema`、`json_field` 或 `content_field` 时请设置为 `json`。 |
| method                        | String  | 否   | GET    | HTTP 请求方法，支持 `GET` 和 `POST`。                                                              |
| headers                       | Map     | 否   | -      | 额外 HTTP 请求头。连接器登录后会自动添加 My Hours `Authorization` 请求头。                         |
| params                        | Map     | 否   | -      | HTTP 查询参数。                                                                                    |
| body                          | String  | 否   | -      | HTTP 请求体。                                                                                      |
| poll_interval_millis          | Int     | 否   | -      | 流模式下请求 HTTP API 的间隔，单位毫秒。                                                            |
| retry                         | Int     | 否   | -      | 请求抛出 `IOException` 时的最大重试次数。                                                           |
| retry_backoff_multiplier_ms   | Int     | 否   | 100    | 重试退避倍数，单位毫秒。                                                                            |
| retry_backoff_max_ms          | Int     | 否   | 10000  | 最大重试退避时间，单位毫秒。                                                                        |
| json_filed_missed_return_null | Boolean | 否   | false  | 配置的 JSON 字段缺失时返回 `null`。                                                                |
| pageing                       | Config  | 否   | -      | 分页设置，用于支持分页的 My Hours 接口，见 [分页](#分页)。                                          |
| common-options                |         | 否   | -      | 源插件通用参数，详见 [源通用选项](../common-options/source-common-options.md)。                  |

## 如何创建 My Hours 数据同步作业

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  MyHours {
    url = "https://api2.myhours.com/api/Projects/getAll"
    email = "seatunnel@test.com"
    password = "********"
    method = "GET"
    format = "json"
    schema {
       fields {
         name = string
         archived = boolean
         dateArchived = string
         dateCreated = string
         clientName = string
         budgetAlertPercent = string
         budgetType = int
         totalTimeLogged = double
         budgetValue = double
         totalAmount = double
         totalExpense = double
         laborCost = double
         totalCost = double
         billableTimeLogged = double
         totalBillableAmount = double
         billable = boolean
         roundType = int
         roundInterval = int
         budgetSpentPercentage = double
         budgetTarget = int
         budgetPeriodType = string
         budgetSpent = string
         id = string
       }
    }
  }
}

# 控制台打印读取的数据
sink {
  Console {
    parallelism = 1
  }
}
```

### 流模式下轮询读取

对于数据随时间增长的 My Hours 接口，使用 `STREAMING` 模式运行连接器，并
通过 `poll_interval_millis` 控制 SeaTunnel 重新发起请求的频率。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 60000
}

source {
  MyHours {
    plugin_output = "myhours_stream"
    url = "https://api2.myhours.com/api/Projects/getAll"
    email = "seatunnel@test.com"
    password = "********"
    method = "GET"
    poll_interval_millis = 30000
    format = "json"
    schema = {
      fields {
        id = string
        name = string
        archived = boolean
      }
    }
  }
}

sink {
  Console {
    plugin_input = "myhours_stream"
  }
}
```

### 分页批量读取

对于支持分页的 My Hours 接口，配置 `pageing` 让连接器持续翻页，直到达到配置的总页数为止。

```hocon
source {
  MyHours {
    plugin_output = "myhours_pages"
    url = "https://api2.myhours.com/api/Clients/getAll"
    email = "seatunnel@test.com"
    password = "********"
    method = "GET"
    format = "json"
    pageing = {
      total_page_size = 10
      batch_size = 100
      page_field = "page"
      page_type = "PageNumber"
    }
    schema = {
      fields {
        id = string
        name = string
        archived = boolean
      }
    }
  }
}
```

## 参数解释

### 认证

把 `email` 和 `password` 配置为 My Hours 账号。连接器会先用这两个凭据换取
访问令牌，然后在后续请求中通过 My Hours `Authorization` 请求头带上该令牌。
`headers` 字段用于补充接口需要的其他请求头。

### 分页

对于支持分页的 My Hours 接口，可以使用 `pageing`。

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

### format

当您指定格式为 `json` 时，您还应该指定 schema 选项。

### content_field

此参数可以获取响应中的一部分 JSON 数据。如果只需要 `book` 部分，可配置 `content_field = "$.store.book.*"`。

### json_field

此参数帮助您配置模式，因此此参数必须与 schema 一起使用。

## 变更日志

<ChangeLog />
