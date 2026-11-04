import ChangeLog from '../changelog/connector-http-myhours.md';

# My Hours

> My Hours 源连接器

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

My Hours Source 提供通过共享 HTTP 请求和重试逻辑读取 My Hours REST 接口的配置。

:::caution 认证限制

当前邮箱/密码认证会发送配置的数据请求，而不是携带凭据的登录请求。下方数据 URL 模板仅用于说明配置结构，不能完成该登录流程。实际使用前需要单独修复连接器认证并完成验证；当前也不支持自动刷新访问令牌。

:::

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
| My Hours | universal | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-http-myhours) |

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
| common-options                |         | 否   | -      | 源插件通用参数，详见 [源通用选项](../common-options/source-common-options.md)。                  |

## 配置参考（当前认证流程受限）

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

## 参数解释

### 认证

配置要求提供 `email` 和 `password`，但当前实现存在本页开头说明的认证限制，也不支持自动刷新访问令牌。

### 请求范围与轮询限制

每次配置的请求只读取一份 HTTP 响应。当前连接器不支持自动分页：`pageing` 不会推进页码，也不会跟随下一页链接或游标。需要多页数据时，请在 API 请求中指定某一页，或在连接器外编排多次请求。

流模式会重复发送配置的请求，可能再次输出相同的行。连接器不会把 API 游标保存到 checkpoint 来实现增量恢复，因此重复轮询和重启都可能产生重复数据。

### format

当您指定格式为 `json` 时，您还应该指定 schema 选项。

### content_field

此参数可以获取响应中的一部分 JSON 数据。如果只需要 `book` 部分，可配置 `content_field = "$.store.book.*"`。

### json_field

此参数帮助您配置模式，因此此参数必须与 schema 一起使用。

## 变更日志

<ChangeLog />
