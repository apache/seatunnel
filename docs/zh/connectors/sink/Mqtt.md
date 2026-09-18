import ChangeLog from '../changelog/connector-mqtt.md';

# MQTT

> MQTT Sink 连接器

## 引擎支持

> SeaTunnel Zeta<br/>
> Flink<br/>
> Spark<br/>

## 描述

用于把数据写入 MQTT broker。该连接器基于 Eclipse Paho 客户端库，支持 MQTT 3.1.1 协议。

它适合把 SeaTunnel 作业中的数据发布到物联网设备、边缘网关或轻量消息 broker。消息可以按 JSON 或纯文本格式序列化，并写入指定的 MQTT topic。

## 关键特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [变更数据捕获](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

:::caution 投递语义

当 `qos = 0` 时，该连接器提供最多一次投递；当 `qos = 1` 时，提供尽力而为的至少一次投递。

默认 `clean_session = true`，连接器按无状态方式运行。客户端断开连接时，未确认的消息可能丢失。设置 `clean_session = false` 后，broker 可以在 writer 运行期间保留会话状态；但当前 Sink 会为每个 writer 自动生成唯一 client id，尚不提供 `client_id` 配置。作业重启后的恢复主要依赖上游重放能力和 MQTT QoS，而不是稳定的 Sink client id。

:::

## Sink 选项

| 名称                 | 类型      | 是否必须 | 默认值  | 描述                                                              |
|--------------------|---------|------|------|-----------------------------------------------------------------|
| url                | string  | 是    | -    | MQTT broker 连接地址，必须包含协议、主机和端口，例如 `tcp://broker.example.com:1883`。 |
| topic              | string  | 是    | -    | 要发布消息的 MQTT topic，例如 `iot/sensors/temperature`。                |
| username           | string  | 否    | -    | MQTT broker 认证用户名。匿名访问时可以不配置。                                   |
| password           | string  | 否    | -    | MQTT broker 认证密码。匿名访问时可以不配置。                                   |
| qos                | int     | 否    | 1    | 发布消息时使用的 MQTT QoS 等级。`0` 表示最多一次，`1` 表示至少一次。                  |
| format             | string  | 否    | json | 输出消息的序列化格式。`json` 把每一行序列化为 JSON 对象；`text` 按分隔符拼接为纯文本。          |
| field_delimiter    | string  | 否    | ,    | 当 `format = "text"` 时使用的字段分隔符，例如 `,`、`|`、`\t`。                  |
| batch_size         | int     | 否    | 1    | 发送到 broker 前缓存的消息数量；每个 checkpoint 和 writer 关闭时也会自动 flush。        |
| retry_timeout      | int     | 否    | 5000 | 发布消息遇到临时网络故障时，最多重试多久，单位为毫秒。                                  |
| connection_timeout | int     | 否    | 30   | 建立 MQTT 连接的超时时间，单位为秒。                                          |
| clean_session      | boolean | 否    | true | 是否使用 clean MQTT session。`true` 丢弃之前的会话状态，`false` 保留会话状态。       |
| common-options     | config  | 否    | -    | Sink 插件通用参数，详见 [Sink 通用选项](../common-options/sink-common-options.md)。 |

### url [string]

MQTT broker 连接地址，必须包含协议、主机和端口。

示例：`tcp://broker.example.com:1883`。

### topic [string]

要发布消息的 MQTT topic。

示例：`iot/sensors/temperature`。

### username [string]

MQTT broker 认证用户名。匿名访问时可以不配置。

### password [string]

MQTT broker 认证密码。匿名访问时可以不配置。

### qos [int]

发布消息时使用的 MQTT 服务质量等级。

- `0`：最多一次，发送后不等待确认。
- `1`：至少一次，broker 需要确认收到消息，默认值。

### format [string]

输出消息的序列化格式。支持以下值：

- `json`：把每一行序列化为一个 JSON 对象，默认值。
- `text`：把每一行序列化为按分隔符拼接的纯文本，分隔符由 `field_delimiter` 控制。

### field_delimiter [string]

当 `format = "text"` 时使用的字段分隔符。默认值为 `,`。

示例：`,`、`|`、`\t`。

### batch_size [int]

发送到 broker 前缓存的消息数量。默认值为 `1`，表示每条消息都会立即发送。
调大该值可以减少逐条发送的开销，提升吞吐。缓存中的消息会在 checkpoint 和 writer 关闭时自动 flush。

### retry_timeout [int]

发布消息遇到临时网络故障时，最多重试多久，单位为毫秒。writer 会在这个时间窗口内等待连接恢复并重试发送。

### connection_timeout [int]

建立 MQTT 连接的超时时间，单位为秒。

### clean_session [boolean]

是否使用 clean MQTT session。默认值为 `true`。

- `true`：broker 会丢弃之前的会话状态，适合大多数无状态写入场景。
- `false`：broker 可以在 writer 运行期间保留自动生成的 client id 对应的会话状态。它有助于处理短暂断连，但可能造成 broker 端状态堆积，也不提供跨作业重启的稳定 client id。

### 通用选项

Sink 插件通用参数，请参考 [Sink 通用选项](../common-options/sink-common-options.md)。

## 性能建议

MQTT Sink 会同步发送消息，以保持写入顺序。典型吞吐：

- QoS 0：~10,000 条消息/秒（局域网）。
- QoS 1：~5,000 条消息/秒（需要 broker ACK）。

可以通过下面方式提升吞吐：

- 适当调大 `batch_size`，例如设置为 `100`，减少逐条发送开销。
- 如果业务可以接受最多一次投递，把 `qos` 设置为 `0`。
- 提高 SeaTunnel 作业并行度，让多个 MQTT client 分担写入。
- 如果需要非常高的吞吐，可以考虑使用 Kafka Sink。

## 任务示例

### 写入 JSON 消息到 MQTT

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
  job.name = "SeaTunnel_MQTT_Sink"
}

source {
  FakeSource {
    row.num = 16
    schema = {
      fields {
        id = bigint
        name = string
        age = int
      }
    }
    plugin_output = "fake"
  }
}

sink {
  MQTT {
    plugin_input = "fake"
    url = "tcp://mqtt-broker:1883"
    topic = "test/seatunnel/sink"
    qos = 1
    format = "json"
  }
}
```

该作业会向 `test/seatunnel/sink` topic 写入 16 条消息。因为 `format` 设置为 `json`，每一行都会被序列化为一条 JSON 消息。

### 使用认证并写入文本格式

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 10
    schema = {
      fields {
        id = bigint
        content = string
      }
    }
  }
}

sink {
  MQTT {
    url = "tcp://secure-broker.example.com:1883"
    topic = "data/pipeline/output"
    username = "seatunnel_user"
    password = "secret"
    qos = 1
    format = "text"
    field_delimiter = "|"
    retry_timeout = 10000
    connection_timeout = 60
  }
}
```

当 `format = "text"` 时，每一行都会被序列化为一行分隔符文本。可以通过 `field_delimiter` 调整分隔符，以匹配下游消费端的解析方式。

## 变更日志

<ChangeLog />
