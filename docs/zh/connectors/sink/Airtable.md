import ChangeLog from '../changelog/connector-http-airtable.md';

# Airtable

> Airtable Sink 连接器

## 描述

用于将数据写入 Airtable。

## 关键特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## Sink 选项

| 参数名                     | 类型    | 必须 | 默认值                    | 描述                                                                                          |
|--------------------------|---------|----|------------------------|---------------------------------------------------------------------------------------------|
| token                    | String  | 是  | -                      | Airtable 个人访问令牌。可在 https://airtable.com/create/tokens 创建。连接器会将其作为 `Authorization: Bearer <token>` 发送。 |
| base_id                  | String  | 是  | -                      | Airtable Base ID（以 `app` 开头）。                                                                      |
| table                    | String  | 是  | -                      | 要写入的表名或表 ID。                                                                                  |
| api_base_url             | String  | 否  | https://api.airtable.com | Airtable API 基础 URL，连接器会自动追加 `/v0/<base_id>/<table>`。                                            |
| typecast                 | boolean | 否  | false                  | 如果为 true，Airtable 会自动将值转换为匹配的字段类型，默认 false。                                                          |
| batch_size               | int     | 否  | 10                     | 每次 API 请求的记录数，受 Airtable API 限制最大为 10，默认 10。                                                       |
| request_interval_ms      | int     | 否  | 220                    | API 请求之间的最小间隔（毫秒），默认 220ms（以保持在 Airtable 每秒 5 次请求的限制内），必须 `>= 0`。                                       |
| rate_limit_backoff_ms    | int     | 否  | 30000                  | 收到 429（限流）响应时的基础退避时间（毫秒），默认 30000ms，必须 `>= 0`。                                                      |
| rate_limit_max_retries   | int     | 否  | 3                      | 收到 429 响应后的最大重试次数，默认 3，必须 `>= 0`。                                                                  |
| common-options           |         | 否  | -                      | Sink 插件通用参数，详见 [Sink Common Options](../common-options/sink-common-options.md)。                         |

## 使用提示

- `token` 是敏感信息，请避免在共享的任务文件中硬编码真实令牌。可使用 SeaTunnel 变量替换或部署平台的密钥管理机制。
- 连接器只会写入到固定的 `base_id` 和 `table`，不会按上游表名将记录路由到不同的 Airtable 表。多表写入场景需要配置多个 sink，或在上游提前路由。
- 每条输入记录会变成一条 Airtable 记录。上游字段名需要与 Airtable 列名一致，否则可以开启 `typecast = true` 让 Airtable 自动转换。
- Airtable 对每个令牌限速为 5 次/秒。默认 `request_interval_ms = 220` 可保证单个连接器不超过该限制。可通过 `rate_limit_backoff_ms` 和 `rate_limit_max_retries` 控制遇到 HTTP 429 时的退避与重试行为。
- 连接器不基于时间定时刷新，缓冲达到 `batch_size` 或写入器关闭时即会发送。

## 任务示例

### 写入 Airtable 表

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        Name = string
        Age = int
      }
    }
    rows = [
      {
        kind = INSERT
        fields = ["Alice", 30]
      },
      {
        kind = INSERT
        fields = ["Bob", 25]
      }
    ]
  }
}

sink {
  Airtable {
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Shipments"
    typecast = true
    batch_size = 10
    request_interval_ms = 220
  }
}
```

### 按列名映射字段写入

当上游字段名与 Airtable 列名一致时可直接写入：

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        Name = string
        Email = string
        Score = int
      }
    }
    rows = [
      {
        kind = INSERT
        fields = ["Alice", "alice@example.com", 95]
      },
      {
        kind = INSERT
        fields = ["Bob", "bob@example.com", 88]
      }
    ]
  }
}

sink {
  Airtable {
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Contacts"
    typecast = false
    batch_size = 10
  }
}
```

### 写入自托管 Airtable

可通过 `api_base_url` 指向自托管或代理的 Airtable 兼容接口：

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        Name = string
        Age = int
      }
    }
    rows = [
      {
        kind = INSERT
        fields = ["Alice", 30]
      }
    ]
  }
}

sink {
  Airtable {
    api_base_url = "https://airtable.internal.example.com"
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Shipments"
  }
}
```

### 从 Kafka 流式写入 Airtable

将 Kafka 源与 Airtable sink 组合，可以把订单等事件持续推送到运营跟踪表。
`batch_size` 保持在 10 以满足 Airtable 的请求上限；当 Topic 的突发流量超过每秒 5 条时，
适当上调 `request_interval_ms`。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 60000
}

source {
  Kafka {
    bootstrap.servers = "kafka:9092"
    topic = "orders.events"
    format = "json"
    schema = {
      fields {
        order_id = string
        customer = string
        amount = double
      }
    }
  }
}

sink {
  Airtable {
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Orders"
    typecast = true
    batch_size = 10
    request_interval_ms = 220
  }
}
```

## 变更日志

<ChangeLog />