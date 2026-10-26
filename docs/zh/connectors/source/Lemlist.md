import ChangeLog from '../changelog/connector-http-lemlist.md';

# Lemlist

> Lemlist 源连接器

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

用于从 Lemlist API 读取数据。连接器会把 `password` 当作 Lemlist API Key，通过 Basic 认证生成请求头，然后复用 HTTP Source 的能力解析返回结果。

Lemlist 连接器与其他基于 HTTP 的源连接器共用同一套 HTTP 请求和重试能力。把 `password` 配置为 Lemlist API Key，再把 `url` 指向要调用的 Lemlist 接口。

## 支持的数据源信息

使用 Lemlist 连接器需要安装下面的依赖。可以通过 install-plugin.sh 安装，也可以从 Maven 中央仓库下载。

| 数据源     | 支持版本 | 依赖                                                                                            |
|------------|----------|-------------------------------------------------------------------------------------------------|
| Lemlist    | 通用     | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-http-lemlist)              |

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户定义分片](../../introduction/concepts/connector-v2-features.md)

:::tip

在流模式下，连接器会反复请求配置的 API。可以用 `poll_interval_millis` 控制请求间隔。

:::

## 选项

| 名称                        | 类型   | 是否必填 | 默认值 | 说明 |
|-----------------------------|--------|----------|--------|------|
| url                         | String | 是       | -      | Lemlist API 地址。 |
| password                    | String | 是       | -      | Lemlist API Key。 |
| method                      | String | 否       | GET    | HTTP 请求方法，支持 `GET` 和 `POST`。 |
| headers                     | Map    | 否       | -      | 额外的 HTTP 请求头。此处配置的 `Authorization` 会被连接器自动生成的 Basic 认证头覆盖。 |
| params                      | Map    | 否       | -      | HTTP 查询参数。 |
| body                        | String | 否       | -      | HTTP 请求体，通常和 `POST` 一起使用。 |
| format                      | String | 否       | TEXT   | 返回内容格式。如果要用 `schema`、`json_field` 或 `content_field` 解析 JSON，请设置为 `json`。 |
| schema                      | Config | 否       | -      | 输出字段结构。`format = "json"` 时必须配置。 |
| json_field                  | Config | 否       | -      | 用 JSONPath 把返回字段映射到输出列，必须和 `schema` 一起使用。 |
| content_field               | String | 否       | -      | 用 JSONPath 选出需要按行解析的数组或对象。 |
| poll_interval_millis        | int    | 否       | -      | 流模式下的请求间隔，单位毫秒。 |
| retry                       | int    | 否       | -      | 请求出现 `IOException` 时的最大重试次数。 |
| retry_backoff_multiplier_ms | int    | 否       | 100    | 重试退避时间倍数，单位毫秒。 |
| retry_backoff_max_ms        | int    | 否       | 10000  | 最大重试退避时间，单位毫秒。 |
| json_filed_missed_return_null | boolean | 否     | false  | `json_field` 中配置的字段缺失时，是否返回 `null`。 |
| common-options              | config | 否       | -      | 源连接器通用配置，见 [源通用选项](../common-options/source-common-options.md)。 |

### 认证

把 `password` 配置为 Lemlist API Key。连接器会用空用户名和该 API Key 生成 Basic 认证请求头。

### 返回结果解析

`format` 默认值是 `TEXT`，会把完整响应作为一个 `content` 字段输出。

如果需要结构化输出，请配置 `format = "json"` 和 `schema`：

```hocon
format = "json"
schema = {
  fields {
    _id = string
    name = string
    userIds = "array<string>"
    createdBy = string
    createdAt = string
    apiKey = string
    billing = {
      quantity = int
      ok = boolean
      plan = string
    }
  }
}
```

如果行数据在嵌套 JSON 节点里，用 `content_field` 选出对应节点。如果输出列需要从不同 JSONPath 中提取，用 `json_field`。

### 请求范围与轮询限制

每次配置的请求只读取一份 HTTP 响应。当前连接器不支持自动分页：`pageing` 不会推进页码，也不会跟随下一页链接或游标。需要多页数据时，请在 API 请求中指定某一页，或在连接器外编排多次请求。

流模式会重复发送配置的请求，可能再次输出相同的行。连接器不会把 API 游标保存到 checkpoint 来实现增量恢复，因此重复轮询和重启都可能产生重复数据。

## 示例

### 批量读取 Lemlist 团队信息

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Lemlist {
    plugin_output = "lemlist"
    url = "https://api.lemlist.com/api/team"
    password = "replace-with-api-key"
    method = "GET"
    format = "json"
    schema = {
      fields {
        _id = string
        name = string
        userIds = "array<string>"
        createdBy = string
        createdAt = string
        apiKey = string
        billing = {
          quantity = int
          ok = boolean
          plan = string
        }
      }
    }
  }
}

sink {
  Console {
    plugin_input = "lemlist"
  }
}
```

### 流模式下轮询读取

对于数据随时间增长的 Lemlist 接口，使用 `STREAMING` 模式运行连接器，
通过 `poll_interval_millis` 控制 SeaTunnel 重新发起请求的频率。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 60000
}

source {
  Lemlist {
    plugin_output = "lemlist_stream"
    url = "https://api.lemlist.com/api/activities"
    password = "replace-with-api-key"
    method = "GET"
    poll_interval_millis = 30000
    format = "json"
    schema = {
      fields {
        _id = string
        type = string
        createdAt = string
      }
    }
  }
}

sink {
  Console {
    plugin_input = "lemlist_stream"
  }
}
```

## 变更日志

<ChangeLog />
