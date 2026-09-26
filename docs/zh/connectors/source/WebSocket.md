import ChangeLog from '../changelog/connector-websocket.md';

# WebSocket

> WebSocket 源连接器

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [x] [流](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行性](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)

## 描述

用于读取 WebSocket 服务端通过 `ws://` 或 `wss://` 推送的数据。连接器建立单条连接，可在握手成功后按序发送
订阅或鉴权消息，并根据配置的 `schema` 与 `format` 把收到的每一帧转换为 SeaTunnel 行。

与轮询请求/响应接口的 HTTP 源不同，WebSocket 源由服务端驱动：只要服务端推送数据就会产生行。文本帧直接使用，
二进制帧按 UTF-8 解码。

连接器使用单个 split，因此源的并行度固定为 1。

## 数据类型映射

配置 `schema` 时，收到的消息按 `format` 解析，并把每个字段转换为声明的 SeaTunnel 类型。`json` 格式会把消息
解析为一个 JSON 对象或一个 JSON 对象数组；`text` 格式按 `field_delimiter` 切分消息。

未配置 `schema` 时，整条消息原样写入只有一列的行：

|  列   |  SeaTunnel 数据类型  |
|-------|---------------------|
| value | STRING              |

## 选项

|          名称          |   类型   | 是否必填 |  默认值  |                                                  描述                                                   |
|-----------------------|---------|--------|---------|---------------------------------------------------------------------------------------------------------|
| url                   | String  | 是      | -       | WebSocket 服务端地址，必须以 `ws://` 或 `wss://` 开头。                                                     |
| headers               | Map     | 否      | -       | 添加到握手请求中的请求头，例如 `Authorization`。                                                             |
| open_messages         | Array   | 否      | -       | 握手成功后按序发送给服务端的消息，通常是订阅或鉴权报文。每次重连成功后会重新发送。                                 |
| format                | String  | 否      | json    | 收到消息的数据格式，仅在配置了 `schema` 时生效，支持 `json` 与 `text`。                                        |
| field_delimiter       | String  | 否      | ,       | 自定义字段分隔符，仅在 `format` 为 `text` 时生效。                                                           |
| connect_timeout_ms    | Integer | 否      | 12000   | 建立连接的超时时间，单位毫秒。                                                                              |
| ping_interval_ms      | Integer | 否      | 0       | 用于保活的 WebSocket ping 帧发送间隔，单位毫秒，`0` 表示关闭。                                                |
| enable_reconnect      | Boolean | 否      | true    | 连接断开后是否自动重连。                                                                                   |
| max_reconnect_times   | Integer | 否      | 3       | 任务失败前允许的连续重连次数上限。                                                                          |
| reconnect_interval_ms | Integer | 否      | 3000    | 每次重连前的等待时间，单位毫秒。                                                                            |
| queue_capacity        | Integer | 否      | 1024    | 缓存收到消息的本地队列容量。队列写满后接收线程阻塞，从而对服务端形成背压。                                        |
| poll_timeout_ms       | Integer | 否      | 1000    | 本地队列为空时，单次读取在队列上等待的最长时间，单位毫秒。                                                      |
| max_records           | Long    | 否      | -1      | 已发出的行数达到该值后结束读取，仅在批模式下生效，`-1` 表示不限制。                                             |
| read_timeout_ms       | Integer | 否      | -1      | 连续该毫秒数没有收到任何消息后结束读取，仅在批模式下生效，`-1` 表示不限制。                                      |
| common-options        |         | 否      | -       | 源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md) 详见。                        |

:::tip

WebSocket 服务端会持续推送数据，因此 `job.mode = STREAMING` 时源不会自行结束。`job.mode = BATCH` 时
必须配置 `max_records` 或 `read_timeout_ms`，否则作业永远不会完成，连接器会直接拒绝这样的配置。

连接器不会为服务端位点做检查点，缓存在内存中的消息以及作业停止期间推送的消息在重启后会丢失。若需要可重放或
精确一次的读取，请不要使用该连接器。

:::

## 如何创建 WebSocket 数据同步作业

### 流模式

下面的示例读取一个行情流并在本地客户端打印。握手成功后立即发送订阅消息：

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  WebSocket {
    url = "wss://example.com/stream"
    headers {
      Authorization = "Bearer <your-token>"
    }
    open_messages = [
      """{"op":"subscribe","args":["trade.BTCUSDT"]}"""
    ]
    ping_interval_ms = 20000
    format = json
    schema = {
      fields {
        symbol = "STRING"
        price = "DOUBLE"
        ts = "BIGINT"
      }
    }
  }
}

sink {
  Console {
    parallelism = 1
  }
}
```

### 批模式

批模式必须配置结束条件。下面的示例采集 100 行后结束，若服务端 30 秒内没有推送任何数据则提前结束：

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  WebSocket {
    url = "ws://localhost:8080/events"
    max_records = 100
    read_timeout_ms = 30000
    schema = {
      fields {
        id = "INT"
        name = "STRING"
      }
    }
  }
}

sink {
  Console {
    parallelism = 1
  }
}
```

### 不配置 Schema

省略 `schema` 时，每一帧都会原样写入单列 `value`，便于先观察一个未知的数据流：

```hocon
source {
  WebSocket {
    url = "ws://localhost:8080/events"
    max_records = 10
  }
}
```

## 常见问题

### 为什么批作业启动时报配置错误？

WebSocket 流没有天然的终点，批作业需要显式的结束条件。请配置 `max_records`、`read_timeout_ms`，或两者都配置。

### 如何鉴权？

可以把凭证放入 `headers` 随握手请求发送；如果服务端要求应用层登录帧，则把凭证作为 `open_messages` 的第一条消息
发送。凭证不会被写入日志。

有些服务端要求把 token 放在 `url` 的查询串中，这同样支持：日志与报错信息中会保留参数名、隐藏参数值，即配置为
`wss://stream.example.com/ws?streams=btcusdt@trade&token=secret` 的 url 在日志中显示为
`wss://stream.example.com/ws?streams=***&token=***`。实际建立连接时仍使用配置的完整 `url`。

如果凭证位于 url 的 *路径* 而非查询串中，则无法以这种方式隐藏，因为路径标识了端点本身。这类凭证建议改用
`headers` 或 `open_messages` 传递。

### 连接断开后会发生什么？

默认情况下连接器最多重连 `max_reconnect_times` 次，每次间隔 `reconnect_interval_ms`，并在每次重连成功后重新发送
`open_messages`。重连次数用尽后任务失败。设置 `enable_reconnect = false` 可在首次断开时直接失败。

### 可以提高并行度吗？

不可以。源使用绑定到单条连接的单个 split，并行度固定为 1。如需扩展，请为每个流运行独立的作业。

### 一帧里包含一个 JSON 数组时会产生一行还是多行？

数组的每个元素产生一行。`format = json` 时，包含 JSON 数组的帧会被展开为多行，`max_records` 统计的是产生的行数，
而不是收到的帧数。

## Changelog

<ChangeLog />

