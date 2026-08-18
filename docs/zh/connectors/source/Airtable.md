import ChangeLog from '../changelog/connector-http-airtable.md';

# Airtable

> Airtable 源连接器

## 描述

用于从 Airtable 读取数据。

## 主要特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [ ] [流](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 源选项

| 参数名 | 类型 | 必须 | 默认值 | 描述 |
|--------|------|------|--------|------|
| token                       | String  | 是 | -                      | Airtable 个人访问令牌。可在 https://airtable.com/create/tokens 创建。 |
| base_id                     | String  | 是 | -                      | Airtable Base ID（以 `app` 开头）。 |
| table                       | String  | 是 | -                      | 要读取的表名或表 ID。 |
| api_base_url                | String  | 否 | https://api.airtable.com | Airtable API 基础 URL。 |
| view                        | String  | 否 | -                      | 视图名称或 ID，仅返回该视图中可见的记录。 |
| fields                      | List    | 否 | -                      | 要包含在响应中的字段名列表。 |
| filter_by_formula           | String  | 否 | -                      | Airtable 公式表达式，用于过滤记录。参考 [Airtable 公式文档](https://support.airtable.com/docs/formula-field-reference)。 |
| max_records                 | int     | 否 | -                      | 返回的最大记录总数。 |
| page_size                   | int     | 否 | -                      | 每页记录数（1-100）。 |
| sort                        | String  | 否 | -                      | 排序定义 JSON 数组，例如 `[{"field":"Name","direction":"asc"}]`。 |
| cell_format                 | String  | 否 | -                      | 单元格值格式，`json` 或 `string`。 |
| return_fields_by_field_id   | boolean | 否 | -                      | 如果为 true，响应中的字段键将使用字段 ID 而非字段名。 |
| record_metadata             | List    | 否 | -                      | 要返回的额外记录元数据，例如 `["commentCount"]`。 |
| time_zone                   | String  | 否 | -                      | 用于格式化日期/时间值的时区。 |
| user_locale                 | String  | 否 | -                      | 用于格式化值的用户区域设置。 |
| offset                      | String  | 否 | -                      | Airtable 返回的分页偏移量。通常不需要手动配置，连接器会自动继续读取 Airtable 后续分页。 |
| headers                     | Map     | 否 | -                      | 额外的 HTTP 请求头。连接器会自动添加 Airtable 认证头和 JSON 内容类型请求头。 |
| body                        | String  | 否 | -                      | 高级请求体配置。不要把它和 `fields`、`filter_by_formula`、`page_size`、`sort` 等专用 Airtable 请求选项配置成同一个 Airtable API 字段。 |
| pageing                     | Config  | 否 | -                      | 继承自 HTTP 连接器的分页配置。普通 Airtable 读取建议优先使用连接器内置的 Airtable 分页处理。 |
| request_interval_ms         | int     | 否 | 220                    | API 请求之间的最小间隔（毫秒），默认 220ms（以保持在 Airtable 每秒 5 次请求的限制内）。 |
| rate_limit_backoff_ms       | int     | 否 | 30000                  | 收到 429（限流）响应时的基础退避时间（毫秒），默认 30000ms。 |
| rate_limit_max_retries      | int     | 否 | 3                      | 收到 429 响应后的最大重试次数，默认 3。 |
| schema                      | Config  | 否 | -                      | 输出数据结构，`format = "json"` 时必填。详见 [Schema 特性](../../introduction/concepts/schema-feature.md)。 |
| schema.fields               | Config  | 否 | -                      | 字段名与 SeaTunnel 数据类型，用于解析 JSON 响应。 |
| format                      | String  | 否 | text                   | 上游数据的格式，支持 `json` 和 `text`，默认 `text`。 |
| content_field               | String  | 否 | -                      | 用于从响应中提取数据的 JsonPath 表达式。对于 Airtable，通常使用 `$.records[*].fields` 来提取每条记录的字段。 |
| json_field                  | Config  | 否 | -                      | 字段级 JSONPath 映射，与 `schema` 配合使用。 |
| json_filed_missed_return_null | boolean | 否 | false                | 配置的 JSON 字段缺失时是否返回 `null`，否则报错。 |
| enable_multi_lines          | boolean | 否 | false                  | 是否启用多行模式，将响应体中按换行分隔的多个 JSON 对象视为独立记录。 |
| connect_timeout_ms          | int     | 否 | 12000                  | HTTP 连接超时时间（毫秒），默认 12000ms。 |
| socket_timeout_ms           | int     | 否 | 60000                  | HTTP 套接字超时时间（毫秒），默认 60000ms。 |
| common-options              | config  | 否 | -                      | 源插件通用参数，详见 [源通用选项](../common-options/source-common-options.md)。 |

## 使用提示

- `token` 是敏感信息，请避免在共享的任务文件中硬编码真实令牌。可使用 SeaTunnel 变量替换或部署平台的密钥管理机制。
- 连接器会自动按 Airtable `offset` 分页继续读取，通常不需要手动设置 `offset`。
- Airtable 对每个令牌限速为 5 次/秒。默认 `request_interval_ms = 220` 可保证单个连接器不超过该限制。可通过 `rate_limit_backoff_ms` 和 `rate_limit_max_retries` 控制遇到 HTTP 429 时的退避与重试行为。
- 若希望输出带字段名的数据行，请将 `format` 设置为 `json` 并配置 `schema`。
- 使用 `content_field = "$.records[*].fields"` 在解析前先抽取记录的字段部分。
- 只有当不同字段位于不同 JSON 路径时，才需要使用 `json_field`。

## 任务示例

### 读取记录并以文本形式输出

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Airtable {
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Shipments"
    format = "text"
    max_records = 10
  }
}

sink {
  Console {
  }
}
```

### 指定 Schema 并提取字段

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Airtable {
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Shipments"
    format = "json"
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

### 按页读取并控制吞吐

通过 `page_size` 与 `request_interval_ms` 控制分页读取的吞吐：

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Airtable {
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Shipments"
    format = "json"
    content_field = "$.records[*].fields"
    page_size = 2
    request_interval_ms = 220
    schema = {
      fields {
        Name = string
        Age = int
        Status = string
      }
    }
  }
}
```

### 通过 JSONPath 抽取字段

当不同字段位于不同 JSON 路径时，使用 `json_field`：

```hocon
source {
  Airtable {
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Shipments"
    format = "json"
    content_field = "$.records[*]"
    json_field = {
      Name = "$.fields.Name"
      Status = "$.fields.Status"
      CreatedAt = "$.createdTime"
    }
    schema = {
      fields {
        Name = string
        Status = string
        CreatedAt = string
      }
    }
  }
}
```

### 仅读取某个视图中的记录

使用 `view` 只读取指定视图中可见的记录，配合 `fields` 限制返回的列：

```hocon
source {
  Airtable {
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Shipments"
    view = "Pending shipments"
    fields = ["Name", "Status", "Weight"]
    format = "json"
    content_field = "$.records[*].fields"
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

### 按增量批次消费新增记录

Airtable source 只支持 `BATCH` 作业（其它模式在初始化时会被拒绝）。要在多次运行之间持续
消费新增行，可以固定 `filter_by_formula` 并配合 `sort` 按 `createdTime` 升序排列，同时把
上一次运行读到的最大 `CreatedAt` 写回公式：

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Airtable {
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Shipments"
    format = "json"
    content_field = "$.records[*].fields"
    filter_by_formula = "IS_AFTER({CreatedAt}, '2026-01-01T00:00:00.000Z')"
    sort = "[{\"field\":\"CreatedAt\",\"direction\":\"asc\"}]"
    page_size = 100
    schema = {
      fields {
        Name = string
        Status = string
        CreatedAt = string
      }
    }
  }
}
```

## 变更日志

<ChangeLog />