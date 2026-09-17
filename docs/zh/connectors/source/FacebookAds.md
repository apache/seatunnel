# FacebookAds

> Facebook Ads 源连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行读取](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表读取](../../introduction/concepts/connector-v2-features.md)

## 描述

通过 Meta Marketing API（Graph API，
`GET /{version}/act_{ad_account_id}/{edge}`）读取 Facebook（Meta）广告账户
边（edge）数据（`campaigns`、`adsets`、`ads`、`insights` 等）。支持单资源
模式和多表（`tables_configs`）批量读取。结果通过 `paging.cursors.after`
游标逐页流式读取，内存中同一时间只保留一页数据。

认证使用具有 `ads_read` 权限的长期 access token，以
`Authorization: Bearer` 请求头发送（不作为查询参数，避免泄漏到日志中）。

Graph API 没有字段元数据服务，因此所有输出列均为 **STRING** 类型——
Facebook 本身就以 JSON 字符串返回大多数指标，嵌套对象/数组会输出为 JSON
文本。schema 按 `fields` 列表顺序构建，第 i 列始终对应第 i 个所选字段。

## 支持的数据源信息

| 数据源       | 支持版本                                        |
|--------------|-------------------------------------------------|
| Facebook Ads | Graph API v23.0（默认，可通过 `api_version` 配置） |

## 前置条件

1. 已添加 **Marketing API** 产品的 Meta 开发者应用。
2. 具有 `ads_read` 权限的 **access token**——例如长期用户 token（60 天）
   或 Business Manager 中的系统用户 token（不过期）。
3. 要查询的**广告账户 ID**（纯数字，例如 `1234567890`；允许携带 `act_`
   前缀，连接器会自动去除）。

## 源选项

| 名称               | 类型    | 必填 | 默认值  | 描述                                                                                       |
|--------------------|---------|------|---------|--------------------------------------------------------------------------------------------|
| access_token       | String  | 是   | -       | 具有 `ads_read` 权限的 Meta Marketing API access token。                                     |
| ad_account_id      | String  | 是   | -       | 要查询的广告账户 ID，纯数字，例如 `1234567890`。允许携带 `act_` 前缀。                        |
| api_version        | String  | 否   | v23.0   | Facebook Graph API 版本。                                                                    |
| resource           | String  | 否*  | -       | 单表模式下的广告账户边，例如 `campaigns`、`adsets`、`ads`、`insights`。需要配合 `fields`。与 `tables_configs` 互斥。 |
| fields             | List    | 否   | -       | 要选取的字段名有序列表，例如 `[id, name, status]`。输出 schema 按此顺序构建。                 |
| filtering          | String  | 否   | -       | 作为 Graph API `filtering` 参数传递的 JSON 数组，例如 `[{"field":"effective_status","operator":"IN","value":["ACTIVE"]}]`。 |
| params             | Map     | 否   | -       | 附加到请求的额外查询参数，例如 `insights` 边的 `{date_preset = last_30d, level = campaign}`。不允许包含 `fields`、`limit`、`after`、`filtering`、`access_token`。 |
| request_timeout_ms | Integer | 否   | 60000   | 单次调用的 HTTP 请求超时（毫秒）。                                                            |
| max_retries        | Integer | 否   | 3       | 单个请求瞬时失败（429/5xx/限流/网络错误）的最大重试次数。                                     |
| retry_backoff_ms   | Long    | 否   | 1000    | 重试之间的基础退避时间（毫秒）；每次重试翻倍。                                                |
| page_size          | Integer | 否   | -       | 每次请求的分页大小（Graph API `limit` 参数）。不设置时使用服务端默认值。                       |

\* `resource` 和 `tables_configs` 必须且只能提供一个。

Graph API 没有 `SELECT *`，字段列表必须通过 `fields` 显式给出。非法字段名
在读取任何数据之前即被拒绝，错误信息中会指出具体字段名。Facebook 限流
（HTTP 400/403 且错误码为 4、17、32、613，或 HTTP 429）会以指数退避重试；
其他客户端错误不重试，并原样透出 API 错误信息。

### tables_configs 条目选项

| 名称          | 类型   | 必填 | 描述                                                                       |
|---------------|--------|------|-----------------------------------------------------------------------------|
| table_path    | String | 是   | 格式：`database.resource`，例如 `facebook_ads.campaigns`。resource 部分指定要读取的边。 |
| fields        | List   | 是   | 该表的字段名有序列表。                                                        |
| filtering     | String | 否   | 该表的 Graph API `filtering` JSON 数组。                                      |
| params        | Map    | 否   | 该表的额外查询参数。                                                          |
| ad_account_id | String | 否   | 表级广告账户 ID 覆盖；未设置时回落到全局 `ad_account_id`。                     |

## 数据类型映射

| Facebook Ads 数据类型 | SeaTunnel 数据类型 | 说明                                                    |
|-----------------------|--------------------|----------------------------------------------------------|
| 任意标量值            | STRING             | Graph API 本身就以 JSON 字符串返回大多数指标。            |
| 对象 / 数组           | STRING             | 嵌套对象和数组输出为 JSON 文本。                          |

结果行中缺失的字段（API 会完全省略空字段）输出为 `null`。

## 示例

### 单资源

```hocon
source {
  FacebookAds {
    access_token  = "your_access_token"
    ad_account_id = "1234567890"

    resource = "campaigns"
    fields   = ["id", "name", "status", "objective", "created_time"]
    filtering = "[{\"field\":\"effective_status\",\"operator\":\"IN\",\"value\":[\"ACTIVE\"]}]"
  }
}
```

### 带额外参数的 insights

```hocon
source {
  FacebookAds {
    access_token  = "your_access_token"
    ad_account_id = "1234567890"

    resource = "insights"
    fields   = ["campaign_id", "campaign_name", "impressions", "clicks", "spend"]
    params = {
      date_preset = "last_30d"
      level       = "campaign"
    }
  }
}
```

### 多表（带表级广告账户 ID）

```hocon
source {
  FacebookAds {
    access_token  = "your_access_token"
    ad_account_id = "1234567890"

    tables_configs = [
      {
        table_path = "facebook_ads.campaigns"
        fields     = ["id", "name", "status"]
      },
      {
        table_path    = "facebook_ads.insights"
        fields        = ["campaign_id", "impressions", "spend"]
        params        = { date_preset = "last_30d", level = "campaign" }
        ad_account_id = "2345678901"
      }
    ]
  }
}
```

## 限制

- 仅支持批处理；无增量/CDC 读取（可在 `insights` 边上通过 `params` 的
  `time_range` 或 `date_preset` 做窗口化抽取）。
- 不支持并行读取；每个作业以单 split 读取。
- 无精确一次语义；重跑作业会重新读取数据。
- 所有列均为 STRING；嵌套对象输出为 JSON 字符串而非嵌套行。

## 变更日志

### next version

- 新增 Facebook Ads 源连接器，支持游标分页和多表读取
