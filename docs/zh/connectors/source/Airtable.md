import ChangeLog from '../changelog/connector-http-airtable.md';

# Airtable

> Airtable 源连接器

## 描述

用于从 Airtable 读取数据。

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [ ] [流](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行性](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)

## 选项

| 参数名 | 类型 | 必须 | 默认值 |
|--------|------|------|--------|
| token                       | String  | 是 | -             |
| base_id                     | String  | 是 | -             |
| table                       | String  | 是 | -             |
| api_base_url                | String  | 否 | https://api.airtable.com |
| view                        | String  | 否 | -             |
| fields                      | List    | 否 | -             |
| filter_by_formula           | String  | 否 | -             |
| max_records                 | int     | 否 | -             |
| page_size                   | int     | 否 | -             |
| sort                        | String  | 否 | -             |
| cell_format                 | String  | 否 | -             |
| return_fields_by_field_id   | boolean | 否 | -             |
| record_metadata             | List    | 否 | -             |
| time_zone                   | String  | 否 | -             |
| user_locale                 | String  | 否 | -             |
| request_interval_ms         | int     | 否 | 220           |
| rate_limit_backoff_ms       | int     | 否 | 30000         |
| rate_limit_max_retries      | int     | 否 | 3             |
| schema                      | Config  | 否 | -             |
| schema.fields               | Config  | 否 | -             |
| format                      | String  | 否 | text          |
| content_field               | String  | 否 | -             |
| json_field                  | Config  | 否 | -             |
| common-options              | config  | 否 | -             |

### token [String]

Airtable 个人访问令牌。可在 https://airtable.com/create/tokens 创建。

### base_id [String]

Airtable Base ID（以 `app` 开头）。

### table [String]

要读取的表名或表 ID。

### api_base_url [String]

Airtable API 基础 URL，默认 `https://api.airtable.com`。

### view [String]

视图名称或 ID，仅返回该视图中可见的记录。

### fields [List]

要包含在响应中的字段名列表。

### filter_by_formula [String]

Airtable 公式表达式，用于过滤记录。参考 [Airtable 公式文档](https://support.airtable.com/docs/formula-field-reference)。

### max_records [int]

返回的最大记录总数。

### page_size [int]

每页记录数（1-100）。

### sort [String]

排序定义 JSON 数组，例如 `[{"field":"Name","direction":"asc"}]`。

### cell_format [String]

单元格值格式，`json` 或 `string`。

### return_fields_by_field_id [boolean]

如果为 true，响应中的字段键将使用字段 ID 而非字段名。

### record_metadata [List]

要返回的额外记录元数据，例如 `["commentCount"]`。

### time_zone [String]

用于格式化日期/时间值的时区。

### user_locale [String]

用于格式化值的用户区域设置。

### request_interval_ms [int]

API 请求之间的最小间隔（毫秒），默认 220ms（以保持在 Airtable 每秒 5 次请求的限制内）。

### rate_limit_backoff_ms [int]

收到 429（限流）响应时的基础退避时间（毫秒），默认 30000ms。

### rate_limit_max_retries [int]

收到 429 响应后的最大重试次数，默认 3。

### schema [Config]

#### fields [Config]

上游数据的模式字段。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。

### format [String]

上游数据的格式，支持 `json` 和 `text`，默认 `text`。

### content_field [String]

用于从响应中提取数据的 JsonPath 表达式。对于 Airtable，通常使用 `$.records[*].fields` 来提取每条记录的字段。

### json_field [Config]

此参数帮助您配置模式，必须与 schema 一起使用。

### common options

源插件通用参数，请参考 [Source Common Options](../common-options/source-common-options.md)。

## 示例

读取 Airtable 表并输出原始文本：

```hocon
source {
  Airtable {
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Shipments"
    format = "text"
    max_records = 10
  }
}
```

指定 schema 并提取记录字段：

```hocon
source {
  Airtable {
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Shipments"
    content_field = "$.records[*].fields"
    filter_by_formula = "{Status} = 'Shipped'"
    schema = {
      fields {
        Name = string
        Status = string
        Weight = float
      }
    }
  }
}
```

## 变更日志

<ChangeLog />
