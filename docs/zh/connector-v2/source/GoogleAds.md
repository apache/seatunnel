# GoogleAds

> Google Ads 源连接器

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

通过 Google Ads REST API（`googleAds:search`）和 GAQL 查询读取 Google Ads
资源数据（campaign、ad_group、keyword_view 等）。支持单资源模式、完整 GAQL
查询模式和多表（`tables_configs`）批量读取。结果通过 `nextPageToken` 逐页
流式读取，内存中同一时间只保留一页数据。

认证使用 OAuth 2.0 refresh-token 授权方式，并携带 Google Ads developer
token。access token 过期时自动刷新。

输出 schema 自动推导：连接器通过 `googleAdsFields:search` 元数据服务获取
每个所选字段的数据类型，并**按 SELECT 字段顺序**构建 schema，第 i 列始终
对应第 i 个所选字段。

## 支持的数据源信息

| 数据源     | 支持版本                                    |
|------------|---------------------------------------------|
| Google Ads | REST API v21（默认，可通过 `api_version` 配置） |

## 前置条件

1. Google Ads **developer token**（在管理者/MCC 账号的 API Center 获取）。
2. OAuth2 **client ID / client secret**（Google Cloud Console，OAuth 同意屏幕
   需包含 `https://www.googleapis.com/auth/adwords` scope）。
3. 已授权该 scope 的 **refresh token**（可通过 OAuth 2.0 Playground 或
   Google Ads API 官方 oauth 脚本获取）。
4. 要查询账号的 **customer ID**（纯数字，不含短横线）。如果该账号由 MCC
   管理，还需要将 `login_customer_id` 设置为 MCC 的 customer ID。

## 源选项

| 名称               | 类型    | 必填 | 默认值  | 描述                                                                                       |
|--------------------|---------|------|---------|--------------------------------------------------------------------------------------------|
| developer_token    | String  | 是   | -       | Google Ads API developer token。                                                             |
| client_id          | String  | 是   | -       | OAuth2 client ID。                                                                           |
| client_secret      | String  | 是   | -       | OAuth2 client secret。                                                                       |
| refresh_token      | String  | 是   | -       | 已授权 `adwords` scope 的 OAuth2 refresh token。                                             |
| customer_id        | String  | 是   | -       | 要查询的 customer ID，纯数字，例如 `1234567890`。                                            |
| login_customer_id  | String  | 否   | -       | 管理者（MCC）customer ID，纯数字。当 `customer_id` 由 MCC 管理时必须设置。                    |
| api_version        | String  | 否   | v21     | Google Ads REST API 版本。                                                                   |
| resource           | String  | 否*  | -       | 单表模式的资源名，例如 `campaign`，需配合 `fields`。与 `query`、`tables_configs` 互斥。       |
| fields             | List    | 否   | -       | 有序的 GAQL 字段路径列表，例如 `[campaign.id, metrics.clicks]`。输出 schema 按此顺序排列。    |
| filter             | String  | 否   | -       | 追加到自动构建的 `SELECT <fields> FROM <resource>` 查询后的 GAQL WHERE 子句。                 |
| query              | String  | 否*  | -       | 完整 GAQL 查询。与 `resource`/`fields`/`filter` 及 `tables_configs` 互斥。                    |
| tables_configs     | List    | 否*  | -       | 多表配置列表，每项必须包含 `table_path`。与 `resource`、`query` 互斥。                        |
| request_timeout_ms | Integer | 否   | 60000   | 单次 HTTP 请求超时时间（毫秒）。                                                             |
| max_retries        | Integer | 否   | 3       | 单个请求瞬时失败（429/5xx/网络错误）的最大重试次数。                                          |
| retry_backoff_ms   | Long    | 否   | 1000    | 重试基础退避时间（毫秒），每次翻倍。                                                          |
| page_size          | Integer | 否   | -       | 搜索请求分页大小，不设置时使用服务端默认值。注意：较新的 API 版本会忽略或拒绝显式分页大小。      |

\* `resource`、`query`、`tables_configs` 三者必须且只能提供一个。

GAQL 不支持 `SELECT *`，字段列表始终是显式的——通过 `fields` 或 `query`
指定。非法字段名会在读取数据前被拦截，错误信息中包含出错的字段名。非法
GAQL（HTTP 400）不会重试，API 错误信息原样透出。

### tables_configs 条目选项

| 名称        | 类型   | 必填 | 描述                                                                  |
|-------------|--------|------|-----------------------------------------------------------------------|
| table_path  | String | 是   | 格式：`database.resource`，例如 `google_ads.campaign`。                |
| fields      | List   | 否*  | 该表的有序 GAQL 字段路径列表。                                          |
| query       | String | 否*  | 该表的完整 GAQL 查询，其 `FROM` 资源必须与 `table_path` 一致。          |
| filter      | String | 否   | GAQL WHERE 子句（仅与 `fields` 搭配使用）。                             |
| customer_id | String | 否   | 该表的 customer ID 覆盖值，缺省时回退到全局 `customer_id`。             |

\* 每个条目的 `fields` 与 `query` 必须且只能提供一个。

## 数据类型映射

| Google Ads 数据类型         | SeaTunnel 数据类型 | 说明                                                                  |
|-----------------------------|--------------------|------------------------------------------------------------------------|
| INT64、UINT64               | BIGINT             | REST API 将 int64 序列化为 JSON 字符串，连接器解析为 long。             |
| INT32                       | INT                |                                                                        |
| DOUBLE、FLOAT               | DOUBLE             |                                                                        |
| BOOLEAN                     | BOOLEAN            |                                                                        |
| DATE                        | STRING             | 有意为之：日期类字段格式不统一（`2026-09-01`、`2026-09`、`2026-36`）。   |
| STRING、ENUM、RESOURCE_NAME | STRING             | 枚举为符号字符串，例如 `ENABLED`。                                      |
| MESSAGE                     | STRING             | 嵌套 message 以 JSON 文本输出。                                         |
| 其他 / 未知                 | STRING             | 向前兼容兜底。                                                          |

结果行中缺失的字段（API 会整体省略空字段）输出为 `null`。

## 示例

### 单资源

```hocon
source {
  GoogleAds {
    developer_token = "your_developer_token"
    client_id       = "your_client_id"
    client_secret   = "your_client_secret"
    refresh_token   = "your_refresh_token"
    customer_id     = "1234567890"

    resource = "campaign"
    fields   = ["campaign.id", "campaign.name", "campaign.status", "metrics.clicks", "metrics.impressions"]
    filter   = "segments.date DURING LAST_30_DAYS"
  }
}
```

### 完整 GAQL 查询

```hocon
source {
  GoogleAds {
    developer_token = "your_developer_token"
    client_id       = "your_client_id"
    client_secret   = "your_client_secret"
    refresh_token   = "your_refresh_token"
    customer_id     = "1234567890"

    query = "SELECT ad_group.id, ad_group.name, metrics.clicks FROM ad_group WHERE metrics.clicks > 0"
  }
}
```

### 多表（含表级 customer ID）

```hocon
source {
  GoogleAds {
    developer_token   = "your_developer_token"
    client_id         = "your_client_id"
    client_secret     = "your_client_secret"
    refresh_token     = "your_refresh_token"
    customer_id       = "1234567890"
    login_customer_id = "9876543210"

    tables_configs = [
      {
        table_path = "google_ads.campaign"
        fields     = ["campaign.id", "campaign.name", "metrics.cost_micros"]
        filter     = "segments.date DURING LAST_7_DAYS"
      },
      {
        table_path  = "google_ads.ad_group"
        query       = "SELECT ad_group.id, ad_group.name FROM ad_group"
        customer_id = "2345678901"
      }
    ]
  }
}
```

## 限制

- 仅支持批处理；不支持增量/CDC 读取（可用 `filter` 搭配日期 segment 做窗口抽取）。
- 不支持并行读取；每个作业以单 split 读取。
- 不支持精确一次语义；重跑作业会重新读取数据。
- 嵌套 MESSAGE 字段以 JSON 字符串输出，不展开为嵌套行。

## 变更日志

### 下一版本

- 新增 Google Ads 源连接器，支持 GAQL、schema 自动推导与多表读取
