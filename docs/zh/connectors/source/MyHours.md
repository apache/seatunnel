import ChangeLog from '../changelog/connector-http-myhours.md';

# My Hours

> My Hours 源连接器

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [ ] [流](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行性](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)

## 描述

用于从 My Hours 读取数据。

## 支持的数据源信息

为了使用 My Hours 连接器，需要以下依赖项。
可以通过 install-plugin.sh 或从 Maven 中央存储库下载。

| 数据源 | 支持的版本 | 依赖 |
|--------|-----------|------|
| My Hours | universal | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel) |

## 源选项

| 参数名                         | 类型      | 必须 | 默认值   | 描述                                                                                          |
|-----------------------------|---------|----|-------|---------------------------------------------------------------------------------------------|
| url                         | String  | 是  | -     | HTTP 请求 URL                                                                                 |
| email                       | String  | 是  | -     | My Hours 登录电子邮件地址                                                                           |
| password                    | String  | 是  | -     | My Hours 登录密码                                                                               |
| schema                      | Config  | 否  | -     | HTTP 和 SeaTunnel 数据结构映射。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。 |
| schema.fields               | Config  | 否  | -     | 上游数据的模式字段                                                                                   |
| json_field                  | Config  | 否  | -     | 此参数帮助您配置模式，因此此参数必须与 schema 一起使用。                                                            |
| content_json                | String  | 否  | -     | 此参数可以获取一些 JSON 数据。                                                                          |
| format                      | String  | 否  | json  | 上游数据的格式，现在仅支持 `json` `text`，默认 `json`。                                                      |
| method                      | String  | 否  | get   | HTTP 请求方法，仅支持 GET、POST 方法。                                                                  |
| headers                     | Map     | 否  | -     | HTTP 请求头                                                                                    |
| params                      | Map     | 否  | -     | HTTP 参数                                                                                     |
| body                        | String  | 否  | -     | HTTP 请求体                                                                                    |
| poll_interval_millis        | Int     | 否  | -     | 流模式下请求 HTTP API 的间隔（毫秒）                                                                     |
| retry                       | Int     | 否  | -     | 如果 HTTP 请求返回 `IOException` 的最大重试次数                                                          |
| retry_backoff_multiplier_ms | Int     | 否  | 100   | HTTP 请求失败时的重试退避倍数（毫秒）                                                                       |
| retry_backoff_max_ms        | Int     | 否  | 10000 | HTTP 请求失败时的最大重试退避时间（毫秒）                                                                     |
| enable_multi_lines          | Boolean | 否  | false | 是否启用多行模式                                                                                    |
| common-options              |         | 否  | -     | 源插件通用参数                                                                                     |

## 如何创建 My Hours 数据同步作业

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  MyHours{
    url = "https://api2.myhours.com/api/Projects/getAll"
    email = "seatunnel@test.com"
    password = "seatunnel"
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

### format

当您指定格式为 `json` 时，您还应该指定 schema 选项。

### content_json

此参数可以获取一些 JSON 数据。如果您只需要 'book' 部分中的数据，配置 `content_field = "$.store.book.*"`。

### json_field

此参数帮助您配置模式，因此此参数必须与 schema 一起使用。

## 变更日志

<ChangeLog />

