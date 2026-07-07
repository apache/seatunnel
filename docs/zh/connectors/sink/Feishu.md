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
| TIME<br/>TIMESTAMP<br/>TIME | String     |
| ARRAY                       | JsonArray  |

## 接收器选项

| 名称                          | 类型      | 是否必需 | 默认值   | 描述                                                                 |
|-----------------------------|---------|------|-------|--------------------------------------------------------------------|
| url                         | String  | 是    | -     | 飞书 Webhook URL。多表作业中可使用 `${database_name}`、`${schema_name}`、`${table_name}` 占位符。 |
| headers                     | Map     | 否    | -     | HTTP 请求头。Webhook 网关需要额外请求头时使用。                                      |
| params                      | Map     | 否    | -     | 该参数被规则接受；当前 Sink 写入逻辑中，建议把查询参数直接写入 `url`，每条数据会作为请求体发送到最终 URL。      |
| retry                       | Int     | 否    | -     | HTTP 请求发生 `IOException` 时的最大重试次数。                                  |
| retry_backoff_multiplier_ms | Int     | 否    | 100   | 重试退避时间倍数，单位毫秒。                                                   |
| retry_backoff_max_ms        | Int     | 否    | 10000 | 最大重试退避时间，单位毫秒。                                                   |
| array_mode                  | Boolean | 否    | false | 为 true 时按 JSON 数组发送多条数据；为 false 时每次请求发送一个 JSON 对象。                  |
| batch_size                  | Int     | 否    | 1     | 单次请求最多发送的数据条数，仅在 `array_mode` 为 true 时生效。                         |
| request_interval_ms         | Int     | 否    | 0     | 两次 HTTP 请求之间的间隔毫秒数，用于避免请求过于频繁。                                  |
| multi_table_sink_replica    | Int     | 否    | 1     | 多表写入时的 Sink 副本数。详情请参考 [Sink 通用选项](../common-options/sink-common-options.md)。 |
| common-options              |         | 否    | -     | Sink 插件通用参数，详情请参考 [Sink 通用选项](../common-options/sink-common-options.md)。 |

## 使用说明

- Sink 固定发送 `POST` JSON 请求，不提供 `method` 配置。
- 如果 Webhook URL 需要查询参数，请直接写在 `url` 中。
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
    row_num = 1
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
    url = "https://open.feishu.cn/open-apis/bot/v2/hook/xxxx"
  }
}
```

### 配置请求头和重试

```hocon
Feishu {
  url = "https://open.feishu.cn/open-apis/bot/v2/hook/xxxx"
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
  url = "https://open.feishu.cn/open-apis/bot/v2/hook/xxxx"
  array_mode = true
  batch_size = 20
  request_interval_ms = 500
}
```

### 多表 Webhook 路径

```hocon
Feishu {
  url = "https://example.com/feishu/${database_name}/${table_name}"
  multi_table_sink_replica = 2
}
```

## 变更日志

<ChangeLog />
