import ChangeLog from '../changelog/connector-graphql.md';

# GraphQL

> GraphQL Sink 连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

GraphQL Sink 连接器通过 HTTP POST 向 GraphQL 服务写入数据。它会对每一行输入数据发送一条
GraphQL `mutation`：输入行里的字段会按同名字段放入 GraphQL `variables`，然后随配置的
mutation 一起发送。

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [变更数据捕获](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 支持的数据源信息

使用 GraphQL 连接器需要安装下面的依赖。可以通过 install-plugin.sh 安装，也可以从 Maven 中央仓库下载。

| 数据源  | 支持版本 | 依赖 |
|---------|----------|------|
| GraphQL | 通用     | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-graphql) |

## Sink 选项

| 名称          | 类型    | 是否必填 | 默认值 | 描述 |
|---------------|---------|----------|--------|------|
| url           | String  | 是       | -      | GraphQL HTTP 服务地址。Sink 模式要求使用 `http://` 或 `https://`。 |
| query         | String  | 是       | -      | GraphQL mutation 语句。Sink 只支持 `mutation`。 |
| variables     | Map     | 否       | -      | 初始 GraphQL 变量。每次请求前，输入行字段会加入这个变量表。 |
| valueCover    | Boolean | 否       | false  | 为 false 时，同名输入行字段会覆盖配置里的变量；为 true 时，保留配置里的变量值。 |
| timeout       | Long    | 否       | -      | 传给 HTTP 客户端参数的请求超时时间。 |
| headers       | Map     | 否       | -      | HTTP 请求头，例如鉴权请求头。 |
| params        | Map     | 否       | -      | HTTP 请求参数。 |
| retry         | Int     | 否       | -      | HTTP 请求出现 IOException 时的最大重试次数。 |
| retry_backoff_multiplier_ms | Int | 否 | 100 | HTTP 请求失败后的重试退避倍率，单位毫秒。 |
| retry_backoff_max_ms | Int | 否 | 10000 | HTTP 请求失败后的最大重试退避时间，单位毫秒。 |
| connect_timeout_ms | Int | 否 | 12000 | HTTP 连接超时时间，单位毫秒。 |
| socket_timeout_ms | Int | 否 | 60000 | HTTP socket 超时时间，单位毫秒。 |
| multi_table_sink_replica | Int | 否 | - | Sink 通用参数，用于控制多表运行时的 sink 副本数；但写入到该 sink 的所有行仍使用同一条 GraphQL mutation。 |
| common-options | Config | 否 | - | Sink 通用参数，详见 [Sink Common Options](../common-options/sink-common-options.md)。 |

## 注意事项

- Sink 会校验 `query` 必须是 GraphQL `mutation`。
- 上游字段名应和 mutation 里使用的变量名保持一致。
- 如果 `variables` 已有某个 key，并且设置 `valueCover = true`，会保留配置里的变量值。
- 如果 `valueCover = false`，同名输入行字段会覆盖配置里的变量值。
- Sink 会为每一行输入数据发送一次 mutation 请求。如果上游包含多张表，需要确认同一条 mutation 和变量名可以处理所有写入到该 Sink 的表。
- `multi_table_sink_replica` 只影响多表 sink 运行时的副本数，不会按不同表自动选择不同的 GraphQL mutation。

### 鉴权

大多数 GraphQL 服务都要求鉴权请求头，可以把它写在 `headers` 里：

```hocon
sink {
  GraphQL {
    plugin_input = "fake"
    url = "https://graphql.example.com/v1/graphql"
    headers = {
      Authorization = "Bearer ${secret}"
    }
    query = """
      mutation MyMutation($id: Int!, $val_string: String!) {
        insert_event(objects: {id: $id, val_string: $val_string}) {
          affected_rows
        }
      }
    """
  }
}
```

`password`、`token` 等敏感配置建议通过运行时占位符（例如 `${secret}`）注入，
不要直接写在配置文件里。

## 任务示例

### 使用 GraphQL Mutation 写入数据

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    schema = {
      fields {
        id = int
        val_bool = boolean
        val_int8 = tinyint
        val_int16 = smallint
        val_int32 = int
        val_int64 = bigint
        val_float = float
        val_double = double
        val_decimal = "decimal(16, 1)"
        val_string = string
        val_unixtime_micros = timestamp
      }
    }
    rows = [
      {
        kind = INSERT
        fields = [1, true, 1, 2, 3, 4, 4.3, 5.3, 6.3, "NEW", "2020-02-02T02:02:02"]
      }
    ]
  }
}

sink {
  GraphQL {
    plugin_input = "fake"
    url = "http://graphql:8080/v1/graphql"
    query = """
      mutation MyMutation(
        $id: Int!
        $val_bool: Boolean!
        $val_int8: smallint!
        $val_int16: smallint!
        $val_int32: Int!
        $val_int64: bigint!
        $val_float: Float!
        $val_double: Float!
        $val_decimal: numeric!
        $val_string: String!
        $val_unixtime_micros: timestamp!
      ) {
        insert_sink(objects: {
          id: $id,
          val_bool: $val_bool,
          val_int8: $val_int8,
          val_int16: $val_int16,
          val_int32: $val_int32,
          val_int64: $val_int64,
          val_float: $val_float,
          val_double: $val_double,
          val_decimal: $val_decimal,
          val_string: $val_string,
          val_unixtime_micros: $val_unixtime_micros
        }) {
          affected_rows
        }
      }
    """
  }
}
```

### 保留配置里的变量值

```hocon
sink {
  GraphQL {
    plugin_input = "fake"
    url = "http://graphql:8080/v1/graphql"
    valueCover = true
    variables = {
      val_bool = true
    }
    query = """
      mutation MyMutation($id: Int!, $val_bool: Boolean!) {
        insert_sink(objects: {id: $id, val_bool: $val_bool}) {
          affected_rows
        }
      }
    """
  }
}
```

### 写入多张上游表

```hocon
source {
  FakeSource {
    plugin_output = "fake"
    tables_configs = [
      {
        schema = {
          table = "graphql_sink_1"
          fields {
            id = int
            val_bool = boolean
            val_string = string
          }
        }
        rows = [
          {
            kind = INSERT
            fields = [1, true, "NEW"]
          }
        ]
      },
      {
        schema = {
          table = "graphql_sink_2"
          fields {
            id = int
            val_bool = boolean
            val_string = string
          }
        }
        rows = [
          {
            kind = INSERT
            fields = [2, true, "READY"]
          }
        ]
      }
    ]
  }
}

sink {
  GraphQL {
    plugin_input = "fake"
    url = "http://graphql:8080/v1/graphql"
    query = """
      mutation MyMutation($id: Int!, $val_bool: Boolean!, $val_string: String!) {
        insert_sink(objects: {id: $id, val_bool: $val_bool, val_string: $val_string}) {
          affected_rows
        }
      }
    """
  }
}
```

### 在流模式下持续写入

流模式下 Sink 会持续接收上游行数据，并对每条行执行一次 mutation 请求。配合 `retry` 和
指数退避参数可以吸收偶发的 HTTP 抖动。

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  FakeSource {
    plugin_output = "events"
    schema = {
      fields {
        id = int
        val_string = string
      }
    }
    rows = [
      { kind = INSERT, fields = [1, "first"] }
      { kind = INSERT, fields = [2, "second"] }
    ]
  }
}

sink {
  GraphQL {
    plugin_input = "events"
    url = "http://graphql:8080/v1/graphql"
    headers = {
      Authorization = "Bearer ${secret}"
    }
    query = """
      mutation MyMutation($id: Int!, $val_string: String!) {
        insert_event(objects: {id: $id, val_string: $val_string}) {
          affected_rows
        }
      }
    """
    retry = 5
    retry_backoff_multiplier_ms = 200
    retry_backoff_max_ms = 5000
  }
}
```

## 变更日志

<ChangeLog />
