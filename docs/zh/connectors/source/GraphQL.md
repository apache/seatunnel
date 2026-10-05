import ChangeLog from '../changelog/connector-graphql.md';

# GraphQL

> GraphQL 源连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

GraphQL 源连接器用于从 GraphQL 服务读取数据。它支持：

- 通过 HTTP 批量读取或轮询读取 GraphQL `query`。
- 通过 WebSocket 读取 GraphQL `subscription`。
- 使用 `content_field` 和 `schema.fields` 解析 JSON 响应。
- 配置请求头、请求参数、GraphQL 变量和超时时间。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)

## 支持的数据源信息

使用 GraphQL 连接器需要安装下面的依赖。可以通过 install-plugin.sh 安装，也可以从 Maven 中央仓库下载。

| 数据源  | 支持版本 | 依赖 |
|---------|----------|------|
| GraphQL | 通用     | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-graphql) |

## 源选项

| 名称                | 类型    | 是否必填 | 默认值 | 描述 |
|---------------------|---------|----------|--------|------|
| url                 | String  | 是       | -      | GraphQL 服务地址。查询模式使用 `http://` 或 `https://`；订阅模式使用 `ws://` 或 `wss://`。 |
| query               | String  | 是       | -      | GraphQL 语句。源连接器支持 `query`；只有设置 `enable_subscription = true` 时才支持 `subscription`。 |
| variables           | Map     | 否       | -      | 随请求体一起发送的 GraphQL 变量。 |
| enable_subscription | Boolean | 否       | false  | 是否使用 WebSocket 订阅模式。为 false 时使用 HTTP POST。 |
| timeout             | Long    | 否       | -      | 传给 HTTP 客户端参数的请求超时时间。 |
| headers             | Map     | 否       | -      | HTTP 请求头，例如鉴权请求头。 |
| params              | Map     | 否       | -      | HTTP 请求参数。 |
| format              | String  | 否       | TEXT   | 继承自 HTTP source 的响应格式。配置 `schema.fields` 读取结构化 JSON 时，通常设置为 `json`。 |
| content_field       | String  | 否       | -      | 从 GraphQL 响应中提取数据数组或对象的 JSONPath，例如 `$.data.source`。 |
| schema.fields       | Config  | 否       | -      | 输出字段和 SeaTunnel 数据类型。读取结构化 JSON 行时配置。 |
| poll_interval_millis | Int    | 否       | -      | 流式查询模式下，两次 HTTP 请求之间的间隔，单位毫秒。 |
| max_retries         | Int     | 否       | 5      | WebSocket 订阅模式下的最大重连次数。 |
| retry_delay_ms      | Int     | 否       | 5000   | WebSocket 订阅模式下两次重连之间的等待时间，单位毫秒。 |
| retry               | Int     | 否       | -      | HTTP 请求出现 IOException 时的最大重试次数。 |
| retry_backoff_multiplier_ms | Int | 否    | 100    | HTTP 请求失败后的重试退避倍率，单位毫秒。 |
| retry_backoff_max_ms | Int    | 否       | 10000  | HTTP 请求失败后的最大重试退避时间，单位毫秒。 |
| enable_multi_lines  | Boolean | 否       | false  | 是否按行拆分 HTTP 响应后再解析。 |
| connect_timeout_ms  | Int     | 否       | 12000  | HTTP 连接超时时间，单位毫秒。 |
| socket_timeout_ms   | Int     | 否       | 60000  | HTTP socket 超时时间，单位毫秒。 |
| common-options      | Config  | 否       | -      | Source 通用参数，详见 [Source Common Options](../common-options/source-common-options.md)。 |

## 注意事项

- 普通查询模式下，`url` 必须以 `http://` 或 `https://` 开头。
- 订阅模式下，需要设置 `enable_subscription = true`，并使用 `ws://` 或 `wss://` 地址。
- Source 不支持 GraphQL `mutation` 操作。如需发送 `mutation` 请求，请使用 GraphQL sink。
- GraphQL 响应通常包在 `data` 字段下面，所以一般需要配置 `content_field`。
- 配置 `schema.fields` 读取结构化数据时，通常需要设置 `format = "json"`，并让 `content_field` 指向要转换为 SeaTunnel 行的数组或对象。
- 使用 HTTP 流式轮询读取时，可以通过 `poll_interval_millis` 控制重复发送同一条查询的间隔。
- 使用 WebSocket 订阅读取时，`max_retries` 和 `retry_delay_ms` 只控制订阅连接失败后的重连行为。

## 任务示例

### 查询 GraphQL 数据

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  GraphQL {
    plugin_output = "graphql_source"
    url = "http://graphql:8080/v1/graphql"
    format = "json"
    content_field = "$.data.source"
    query = """
      query MyQuery($limit: Int) {
        source(limit: $limit) {
          id
          val_bool
          val_double
          val_float
        }
      }
    """
    variables = {
      limit = 2
    }
    schema = {
      fields {
        id = "int"
        val_bool = "boolean"
        val_double = "double"
        val_float = "float"
      }
    }
  }
}

sink {
  Console {
    plugin_input = "graphql_source"
  }
}
```

### 订阅 GraphQL 数据

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  GraphQL {
    plugin_output = "graphql_subscription"
    url = "ws://graphql:8080/v1/graphql"
    format = "json"
    content_field = "$.data.source"
    enable_subscription = true
    max_retries = 5
    retry_delay_ms = 5000
    query = """
      subscription MySubscription {
        source {
          id
          val_bool
          val_double
          val_float
        }
      }
    """
    schema = {
      fields {
        id = "int"
        val_bool = "boolean"
        val_double = "double"
        val_float = "float"
      }
    }
  }
}

sink {
  Console {
    plugin_input = "graphql_subscription"
  }
}
```

### 带鉴权头的 GraphQL 查询

GraphQL 服务需要鉴权时，可以把 bearer token 等放在 `headers` 里。底层 HTTP 客户端
支持的任何请求头都可以通过这个参数传递。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  GraphQL {
    plugin_output = "graphql_auth"
    url = "https://graphql.example.com/v1/graphql"
    format = "json"
    content_field = "$.data.source"
    headers = {
      Authorization = "Bearer ${secret}"
      X-Tenant = "acme"
    }
    query = """
      query MyQuery {
        source {
          id
          val_bool
        }
      }
    """
    schema = {
      fields {
        id = "int"
        val_bool = "boolean"
      }
    }
  }
}

sink {
  Console {
    plugin_input = "graphql_auth"
  }
}
```

### 流式轮询查询

对于只提供 HTTP 接口、但会持续发布新数据的服务，可以用流模式周期性地重复执行
同一条查询。SeaTunnel 按 `poll_interval_millis` 发送请求，并把新行转发给下游算子。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 60000
}

source {
  GraphQL {
    plugin_output = "graphql_streaming"
    url = "http://graphql:8080/v1/graphql"
    format = "json"
    content_field = "$.data.source"
    poll_interval_millis = 10000
    query = """
      query MyQuery {
        source {
          id
          val_bool
          val_double
        }
      }
    """
    schema = {
      fields {
        id = "int"
        val_bool = "boolean"
        val_double = "double"
      }
    }
  }
}

sink {
  Console {
    plugin_input = "graphql_streaming"
  }
}
```

## 变更日志

<ChangeLog />
