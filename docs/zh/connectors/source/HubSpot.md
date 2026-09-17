import ChangeLog from '../changelog/connector-http-hubspot.md';

# HubSpot

> HubSpot 源连接器

## 描述

HubSpot 源连接器用于读取 HubSpot CRM V3 REST API 数据。它基于共享的 HTTP 源连接器实现，并增加了 HubSpot 场景的默认行为：

- 自动把 `access_token` 组装成 `Authorization: Bearer <access_token>` 请求头
- 根据 `object_type` 自动拼接默认 URL
- 默认按 JSON 响应解析
- 默认使用 HubSpot `paging.next.after` 游标分页

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

:::tip

HubSpot 继承了共享 HTTP Source 的运行时。批任务只拉取一次后结束，流任务则会按照
`poll_interval_millis` 持续轮询。只有 `format = "binary"` 仍然只支持批模式，因为它沿用的是
共享 HTTP 的二进制读取约束。

:::

## 选项

| 参数名                         | 类型    | 必填 | 默认值 |
|--------------------------------|---------|------|--------|
| access_token                   | String  | 是   | -      |
| object_type                    | String  | 否   | contacts |
| url                            | String  | 否   | 根据 `object_type` 推导 |
| method                         | String  | 否   | GET    |
| headers                        | Map     | 否   | -      |
| params                         | Map     | 否   | -      |
| body                           | String  | 否   | -      |
| format                         | String  | 否   | JSON   |
| schema                         | Config  | 否   | -      |
| json_field                     | Config  | 否   | -      |
| content_field                  | String  | 否   | `$.results` |
| pageing                        | Config  | 否   | 默认游标分页 |
| binary_chunk_size              | long    | 否   | 10485760 |
| poll_interval_millis           | int     | 否   | -      |
| retry                          | int     | 否   | -      |
| retry_backoff_multiplier_ms    | int     | 否   | 100    |
| retry_backoff_max_ms           | int     | 否   | 10000  |
| enable_multi_lines             | boolean | 否   | false  |
| connect_timeout_ms             | int     | 否   | 12000  |
| socket_timeout_ms              | int     | 否   | 60000  |
| keep_params_as_form            | boolean | 否   | false  |
| keep_page_param_as_http_param  | boolean | 否   | true   |
| json_filed_missed_return_null  | boolean | 否   | false  |

### access_token [String]

HubSpot 私有应用访问令牌。连接器会把它作为 Bearer token 写入 HTTP `Authorization` 请求头。

### object_type [String]

要读取的 HubSpot CRM 对象类型。常见值包括 `contacts`、`companies`、`deals`、`products`、`tickets`、`quotes` 等。

### url [String]

可选的 HubSpot API URL 覆盖项。如果不配置，连接器会自动生成
`https://api.hubapi.com/crm/v3/objects/{object_type}`。

### method [String]

HTTP 请求方法。常见的 HubSpot 读取场景使用 `GET`。

### headers [Map]

额外的 HTTP 请求头。如果没有显式提供 `Authorization`，连接器会自动注入
`Authorization: Bearer <access_token>`。只有在你确实需要覆盖该值时，才需要配置
`headers.Authorization`。

### params [Map]

HTTP 查询参数。可用于补充筛选条件、分页大小或其他 HubSpot API 查询参数。

### body [String]

HTTP 请求体。只有 HubSpot 目标接口支持请求体时才需要配置。

### format [String]

响应数据格式，支持 `json`、`text` 和 `binary`。当未显式配置 `format` 时，
HubSpot 默认使用 `JSON`，因为 CRM API 的正常返回就是 JSON。

### schema [Config]

当 `format = "JSON"` 时，用于定义输出行结构。更多信息请参考
[Schema 特性](../../introduction/concepts/schema-feature.md)。

字段定义位于 `schema.fields` 这个嵌套配置下。

### json_field [Config]

把输出字段映射到 JSONPath 表达式。需要从 HubSpot 返回的嵌套 JSON 中提取字段时，可与 `schema` 一起使用。

### content_field [String]

用于在 schema 解析前先截取 JSON 片段的 JSONPath 表达式。HubSpot 默认值为 `$.results`。

### pageing [Config]

继承自 HTTP 连接器的分页配置。HubSpot 在 JSON / text 响应下默认使用基于 `after`
的游标分页，并从 `$.paging.next.after` 读取下一页游标。二进制模式不支持分页。
任务配置中请保持 `pageing` 这个拼写。

HubSpot 常用的 `pageing` 子项如下：

| 参数名 | 类型 | 必填 | 默认值 | 说明 |
|--------|------|------|--------|------|
| page_type | String | 否 | Cursor | 分页类型。HubSpot 默认使用游标分页。 |
| cursor_field | String | 否 | `after` | 请求参数中的游标字段名。 |
| cursor_response_field | String | 否 | `$.paging.next.after` | 用于从响应中提取下一页游标的 JSONPath。 |
| use_placeholder_replacement | boolean | 否 | false | 是否在请求头、参数和请求体中使用 `${field}` 占位符替换。 |
| total_page_size | long | 否 | 0 | 继承自共享 HTTP 分页的总页数限制。 |
| batch_size | int | 否 | 100 | 页码分页时每次请求的页大小。 |
| start_page_number | long | 否 | 1 | 页码分页时的起始页号。 |
| page_field | String | 否 | `page` | 页码分页时的请求参数名。 |

### binary_chunk_size [long]

当 `format = "binary"` 时，按该字节大小切分响应体。该选项只对批任务生效，行为与共享 HTTP
二进制读取器保持一致。

### poll_interval_millis [int]

流处理任务中的请求间隔，单位毫秒。批处理任务只读取一次后结束。

### retry [int]

HTTP 请求因 `IOException` 失败时的最大重试次数。

### retry_backoff_multiplier_ms [int]

重试退避时间乘数，单位毫秒。

### retry_backoff_max_ms [int]

最大重试退避时间，单位毫秒。

### enable_multi_lines [boolean]

设置为 `true` 时，共享 HTTP reader 会按行切分文本响应。

### connect_timeout_ms [int]

HTTP 连接超时时间，单位毫秒，默认值为 `12000`。

### socket_timeout_ms [int]

HTTP Socket 超时时间，单位毫秒，默认值为 `60000`。

### keep_params_as_form [boolean]

设置为 `true` 时，会把 `params` 以表单字段的方式提交，便于兼容需要 form 风格参数的接口。

### keep_page_param_as_http_param [boolean]

设置为 `true` 时，连接器会把生成的分页参数直接注入 HTTP 查询参数。HubSpot 默认开启该行为，
这样游标分页无需额外模板替换也能工作。

### json_filed_missed_return_null [boolean]

设置为 `true` 时，JSON 字段缺失会返回 `null`；否则字段缺失会报错。

### common options

源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md)。

## 示例

使用默认 CRM V3 地址读取 HubSpot Contacts：

```hocon
source {
  HubSpot {
    access_token = "pat-na1-..."
    object_type = "contacts"
    format = "JSON"
    schema = {
      fields {
        id = string
        properties = string
      }
    }
    json_field = {
      id = "$.id"
      properties = "$.properties"
    }
  }
}
```

使用自定义 URL 和显式分页参数读取 HubSpot 数据：

```hocon
source {
  HubSpot {
    url = "https://api.hubapi.com/crm/v3/objects/contacts"
    access_token = "pat-na1-..."
    params = {
      limit = "100"
    }
    pageing = {
      page_type = "Cursor"
      cursor_field = "after"
      cursor_response_field = "$.paging.next.after"
    }
    format = "JSON"
    schema = {
      fields {
        id = string
      }
    }
    json_field = {
      id = "$.id"
    }
  }
}
```

## 变更日志

<ChangeLog />
