import ChangeLog from '../changelog/connector-http-feishu.md';

# 飞书

> 飞书 数据接收器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [变更数据捕获](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 描述

用于通过上游数据调用飞书 Webhook。

> 例如，如果来自上游的数据是 [`年龄: 12, 姓名: tyrantlucifer`]，则 body 内容如下：`{"年龄": 12, "姓名": "tyrantlucifer"}`

飞书 Sink 会发送 `POST` 请求。每一条上游数据都会被转换成 JSON 并作为请求体发送。当 `array_mode = true` 时，多条数据会先合并成一个 JSON 数组再发送。

:::tip 提示

飞书 Webhook URL 和自定义鉴权请求头通常包含敏感信息，请不要在日志或示例中暴露真实 Token。

:::

## 数据类型映射

|       SeaTunnel 数据类型        |   飞书数据类型   |
|-----------------------------|------------|
| ROW<br/>MAP                 | Json       |
| NULL                        | null       |
| BOOLEAN                     | boolean    |
| TINYINT                     | byte       |
| SMALLINT                    | short      |
| INT                         | int        |
| BIGINT                      | long       |
| FLOAT                       | float      |
| DOUBLE                      | double     |
| DECIMAL                     | BigDecimal |
| BYTES                       | byte[]     |
| STRING                      | String     |
| DATE                        | String     |
| TIME                        | String     |
| TIMESTAMP                   | String     |
| ARRAY                       | JsonArray  |

## 接收器选项

| 名称                          | 类型      | 是否必需 | 默认值   | 描述                                                                                       |
|-----------------------------|---------|------|-------|------------------------------------------------------------------------------------------|
| url                         | String  | 是    | -     | 飞书 Webhook URL。当前 Sink 写入器会向这个固定 URL 发送请求，不会替换表名相关占位符。                             |
| headers                     | Map     | 否    | -     | HTTP 请求头。Webhook 网关需要额外请求头时使用。                                                            |
| params                      | Map     | 否    | -     | 该参数会通过参数校验，但当前 Sink 写入器不会把它传入请求。如需查询参数，请把非敏感参数直接写在 `url` 中。                         |
| retry                       | Int     | 否    | -     | HTTP 请求发生 `IOException` 时的最大重试次数。                                                        |
| retry_backoff_multiplier_ms | Int     | 否    | 100   | 重试退避时间倍数，单位毫秒。                                                                         |
| retry_backoff_max_ms        | Int     | 否    | 10000 | 最大重试退避时间，单位毫秒。                                                                         |
| array_mode                  | Boolean | 否    | false | 为 true 时按 JSON 数组发送多条数据；为 false 时每次请求发送一个 JSON 对象。                                |
| batch_size                  | Int     | 否    | 1     | 单次请求最多发送的数据条数，仅在 `array_mode` 为 true 时生效。                                               |
| request_interval_ms         | Int     | 否    | 0     | 两次 HTTP 请求之间的间隔毫秒数，用于避免请求过于频繁。                                                        |
| multi_table_sink_replica    | Int     | 否    | 1     | 多表写入时的 Sink 副本数。详情请参考 [Sink 通用选项](../common-options/sink-common-options.md)。                 |
| common-options              |         | 否    | -     | Sink 插件通用参数，详情请参考 [Sink 通用选项](../common-options/sink-common-options.md)。                       |

## 使用说明

- Sink 固定发送 `POST` JSON 请求，不提供 `method` 配置。
- 如果 Webhook URL 需要查询参数，请把非敏感参数直接写在 `url` 中。鉴权类信息建议优先放在 `headers` 里，前提是网关支持这种方式，因为完整 URL（包括查询参数）可能出现在日志或作业元数据中。
- 多表作业可以使用 `multi_table_sink_replica`，但飞书 Sink 会把所有数据发送到配置的固定 `url`，不会替换 URL 中的 `${database_name}`、`${schema_name}` 或 `${table_name}`。
- 当接收端支持 JSON 数组，且希望减少 HTTP 请求次数时，可以启用 `array_mode`。
- 飞书 Webhook 投递不是精确一次。如果远端已经处理成功但本地收到异常并触发重试，接收端可能看到重复消息。

## 任务示例

### 简单示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 1
    schema = {
      fields {
        name = string
        age = int
      }
    }
    rows = [
      {
        fields = [tyrantlucifer, 12]
        kind = INSERT
      }
    ]
  }
}

sink {
  Feishu {
    url = "https://open.feishu.cn/open-apis/bot/v2/hook/<your-hook-token>"
  }
}
```

### 配置请求头和重试

```hocon
Feishu {
  url = "https://open.feishu.cn/open-apis/bot/v2/hook/<your-hook-token>"
  headers {
    Content-Type = "application/json"
  }
  retry = 3
  retry_backoff_multiplier_ms = 200
  retry_backoff_max_ms = 5000
}
```

### 将多条数据按 JSON 数组发送

```hocon
Feishu {
  url = "https://open.feishu.cn/open-apis/bot/v2/hook/<your-hook-token>"
  array_mode = true
  batch_size = 20
  request_interval_ms = 500
}
```

### 多表 Sink 副本

```hocon
Feishu {
  url = "https://open.feishu.cn/open-apis/bot/v2/hook/<your-hook-token>"
  multi_table_sink_replica = 2
}
```

### 流式告警推送到飞书机器人

如果需要持续推送告警，可以把 Sink 运行在流模式下，并配合 `request_interval_ms`
和 `array_mode = true`，把多条告警合并成一次 Webhook 调用。下面的示例从 Kafka
主题读取事件，并按批次以 JSON 数组的形式发送。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  Kafka {
    plugin_output = "alerts"
    bootstrap.servers = "kafka:9092"
    topic = "service_alerts"
    format = "json"
    schema = {
      fields {
        name = string
        age = int
      }
    }
  }
}

sink {
  Feishu {
    plugin_input = "alerts"
    url = "https://open.feishu.cn/open-apis/bot/v2/hook/<your-hook-token>"
    array_mode = true
    batch_size = 20
    request_interval_ms = 1000
    retry = 5
    retry_backoff_multiplier_ms = 200
    retry_backoff_max_ms = 10000
  }
}
```

### 发送飞书富文本消息

飞书机器人 Webhook 接收的是 `msg_type` 包裹的消息，例如 `text`、`post` 或
`interactive`。可以在上游加一个 Transform，把每一行包装成期望的格式后再交给
飞书 Sink 发送。下面的示例把每行数据封装成 `text` 消息体。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "raw"
    row.num = 1
    schema = {
      fields {
        name = string
        age = int
      }
    }
    rows = [
      {
        fields = [tyrantlucifer, 12]
        kind = INSERT
      }
    ]
  }
}

transform {
  Sql {
    plugin_input = "raw"
    query = "SELECT 'text' AS msg_type, named_struct('text', concat('User ', name, ' is ', cast(age as string), ' years old')) AS content FROM raw"
  }
}

sink {
  Feishu {
    plugin_input = "Sql"
    url = "https://open.feishu.cn/open-apis/bot/v2/hook/<your-hook-token>"
    headers {
      Content-Type = "application/json"
    }
  }
}
```

## 变更日志

<ChangeLog />
