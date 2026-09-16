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

| 参数名 | 类型 | 是否必填 | 默认值 | 描述 |
|--------|------|----------|--------|------|
| url | String | 是 | - | MQTT broker 连接 URL，必须包含协议、主机和端口，例如 `tcp://broker.example.com:1883`。MQTT over TLS/SSL 请使用 `ssl://broker.example.com:8883`。 |
| topic | String | 是 | - | 要订阅消息的 MQTT topic，例如 `iot/sensors/temperature`。 |
| schema | Config | 是 | - | 上游数据的 schema 字段，详见 [Schema 特性](../../introduction/concepts/schema-feature.md)。 |
| username | String | 否 | - | MQTT broker 认证用户名，匿名访问时可不填。 |
| password | String | 否 | - | MQTT broker 认证密码，匿名访问时可不填。 |
| qos | Int | 否 | 1 | 订阅 topic 时使用的 MQTT QoS 等级。支持 `0`（QoS 0）或 `1`（QoS 1）。该设置只控制 MQTT broker 和 client 之间的交付，不提供 SeaTunnel 端到端保证。 |
| format | String | 否 | json | 输入消息的反序列化格式。支持 `json`（将每条消息反序列化为 JSON 对象）或 `text`（按 `field_delimiter` 切分为纯文本）。 |
| field_delimiter | String | 否 | `,` | 当 `format=text` 时使用的字段分隔符，例如 `,`、`\|`、`\t`。 |
| client_id | String | 否 | - | MQTT client id。当 `clean_session=true` 且未配置该选项时，连接器会生成随机 client id。`clean_session=false` 时必须配置稳定的 `client_id`。 |
| clean_session | Boolean | 否 | true | 是否使用 clean MQTT session。`true` 时 broker 丢弃之前的会话状态；`false` 时 broker 可以保留会话状态和订阅，需要稳定的 `client_id`。 |
| connection_timeout | Int | 否 | 30 | MQTT 连接建立超时时间，单位为秒。 |
| keep_alive_interval | Int | 否 | 60 | MQTT keep alive 间隔，单位为秒。 |
| reconnect_timeout | Int | 否 | 120 | 等待 MQTT 自动重连的最长时间，单位为秒。如果 MQTT 客户端断开连接的时间超过该超时时间，源任务会失败，避免无限期静默等待。 |
| max_queue_size | Int | 否 | 1000 | 反序列化之前在内存中缓存的 MQTT 消息最大数量。 |
| common-options | | 否 | - | 源插件通用参数，详情请参考 [源通用选项](../common-options/source-common-options.md)。 |

:::tip

该连接器当前只支持单个 topic 订阅，多 topic 拆分请使用多个 MQTT source。MQTT 5.0 特性（如共享订阅、消息属性）暂未启用。

:::

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

### TLS/SSL 接入

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  MQTT {
    url = "ssl://broker.example.com:8883"
    topic = "factory/line-1/status"
    username = "seatunnel"
    password = "broker-token"
    client_id = "seatunnel-mqtt-tls"
    qos = 1
    connection_timeout = 30
    keep_alive_interval = 60
    reconnect_timeout = 180
    format = "json"
    schema = {
      fields {
        device_id = string
        status = string
        ts = bigint
      }
    }
  }
}

sink {
  Console {}
}
```

### 文本分隔源

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  MQTT {
    url = "tcp://broker.example.com:1883"
    topic = "factory/line-2/log"
    format = "text"
    field_delimiter = "|"
    schema = {
      fields {
        device_id = string
        level = string
        message = string
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
