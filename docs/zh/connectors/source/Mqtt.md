import ChangeLog from '../changelog/connector-mqtt.md';

# MQTT

> MQTT 源连接器

## 描述

用于从 MQTT broker 读取消息。该连接器通过 Eclipse Paho 客户端库支持 MQTT 3.1.1 协议。

该连接器会订阅配置的 MQTT topic，将消息 payload 按 JSON 或 text 格式反序列化，并转换为 SeaTunnel Row。

## 支持引擎

> SeaTunnel Zeta<br/>
> Flink<br/>
> Spark<br/>

## 主要特性

- [ ] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

:::caution 交付语义

`qos` 选项只控制 MQTT broker 和 MQTT client 之间的交付语义。它没有与 SeaTunnel checkpoint 集成，因此该源连接器不提供端到端的精确一次或至少一次保证。

如需使用 MQTT 持久会话，请设置 `clean_session=false` 并配置稳定的 `client_id`。当 `clean_session=false` 时，源连接器在关闭时只断开连接，不会取消订阅，因此 broker 可以根据 MQTT 会话语义保留订阅。

源连接器使用 MQTT 自动重连。如果客户端断开连接的时间超过 `reconnect_timeout`，源任务会失败，以避免静默停止摄取。

:::

## 选项

| 参数名               | 类型      | 必须 | 默认值  |
|-------------------|---------|----|------|
| url               | string  | 是  | -    |
| topic             | string  | 是  | -    |
| schema            | config  | 是  | -    |
| username          | string  | 否  | -    |
| password          | string  | 否  | -    |
| qos               | int     | 否  | 1    |
| format            | string  | 否  | json |
| field_delimiter   | string  | 否  | ,    |
| client_id         | string  | 否  | -    |
| clean_session     | boolean | 否  | true |
| connection_timeout | int    | 否  | 30   |
| keep_alive_interval | int   | 否  | 60   |
| reconnect_timeout | int     | 否  | 120  |
| max_queue_size    | int     | 否  | 1000 |
| common-options    |         | 否  | -    |

### url [string]

MQTT broker 连接 URL。必须包含协议、主机和端口。

示例：`tcp://broker.example.com:1883`

### topic [string]

要订阅消息的 MQTT topic。

示例：`iot/sensors/temperature`

### schema [config]

上游数据的 schema 字段。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。

### username [string]

MQTT broker 认证用户名。匿名访问时可以不配置。

### password [string]

MQTT broker 认证密码。匿名访问时可以不配置。

### qos [int]

订阅 topic 时使用的 MQTT Quality of Service 等级。

该设置只控制 MQTT broker 和 MQTT client 之间的交付，不提供 SeaTunnel 端到端交付保证。

支持的值：

- `0` — MQTT QoS 0
- `1` — MQTT QoS 1

### format [string]

输入消息的反序列化格式。支持的值：

- `json` — 将每条消息反序列化为 JSON 对象（默认）
- `text` — 将每条消息按分隔符反序列化为纯文本（分隔符由 `field_delimiter` 控制）

### field_delimiter [string]

当 `format` 设置为 `text` 时使用的字段分隔符。默认值为 `,`。

示例：`,`, `|`, `\t`

### client_id [string]

MQTT client id。当 `clean_session=true` 且未配置该选项时，连接器会生成随机 client id。

当 `clean_session=false` 时必须配置该选项，因为 MQTT 持久会话需要稳定的 client id。

### clean_session [boolean]

是否使用 clean MQTT session。默认值为 `true`。

- `true` — broker 丢弃之前的会话状态，适合无状态运行。
- `false` — broker 可以保留会话状态，包括订阅信息。需要稳定的 `client_id`。

### connection_timeout [int]

MQTT 连接建立超时时间，单位为秒。

### keep_alive_interval [int]

MQTT keep alive 间隔，单位为秒。

### reconnect_timeout [int]

等待 MQTT 自动重连的最长时间，单位为秒。如果 MQTT 客户端断开连接的时间超过该超时时间，`pollNext()` 会使源任务失败，避免无限期静默等待。

### max_queue_size [int]

反序列化之前在内存中缓存的 MQTT 消息最大数量。

### common options

源插件通用参数，详情请参考 [源通用选项](../common-options/source-common-options.md)。

## 示例

### JSON 源

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  MQTT {
    url = "tcp://broker.example.com:1883"
    topic = "iot/sensors/readings"
    qos = 1
    format = "json"
    schema = {
      fields {
        id = bigint
        name = string
        temperature = double
      }
    }
    plugin_output = "sensor_data"
  }
}

sink {
  Console {
    plugin_input = "sensor_data"
  }
}
```

### 持久会话源

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  MQTT {
    url = "tcp://broker.example.com:1883"
    topic = "iot/sensors/readings"
    client_id = "seatunnel-mqtt-source"
    clean_session = false
    qos = 1
    format = "json"
    schema = {
      fields {
        id = bigint
        temperature = double
      }
    }
  }
}

sink {
  Console {}
}
```

## 变更日志

<ChangeLog />
