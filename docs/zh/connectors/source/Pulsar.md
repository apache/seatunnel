import ChangeLog from '../changelog/connector-pulsar.md';

# Apache Pulsar

> Apache Pulsar 源连接器

## 描述

Apache Pulsar 的源连接器。

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [x] [流](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行性](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)

## 选项

| 参数名                      | 类型      | 必须 | 默认值    | 描述                                                                                   |
|--------------------------|---------|----|--------|--------------------------------------------------------------------------------------|
| topic                    | String  | 否  | -      | 主题名称                                                                                 |
| topic-pattern            | String  | 否  | -      | 主题名称的正则表达式模式                                                                         |
| topic-discovery.interval | Long    | 否  | -1     | 发现新主题分区的间隔（毫秒）                                                                       |
| subscription.name        | String  | 是  | -      | 订阅名称                                                                                 |
| client.service-url       | String  | 是  | -      | Pulsar 服务 URL                                                                        |
| admin.service-url        | String  | 是  | -      | Pulsar 管理端点的 HTTP URL                                                                |
| auth.plugin-class        | String  | 否  | -      | 认证插件的名称                                                                              |
| auth.params              | String  | 否  | -      | 认证插件的参数                                                                              |
| poll.timeout             | Integer | 否  | 100    | 获取记录时的最大等待时间（毫秒）                                                                     |
| poll.interval            | Long    | 否  | 50     | 获取记录时的间隔时间（毫秒）                                                                       |
| poll.batch.size          | Integer | 否  | 500    | 轮询时要获取的最大记录数                                                                         |
| cursor.startup.mode      | Enum    | 否  | LATEST | 启动模式                                                                                 |
| cursor.startup.timestamp | Long    | 否  | -      | 启动时间戳（毫秒）                                                                            |
| cursor.reset.mode        | Enum    | 否  | LATEST | 游标重置策略                                                                               |
| cursor.stop.mode         | Enum    | 否  | NEVER  | 停止模式                                                                                 |
| cursor.stop.timestamp    | Long    | 否  | -      | 停止时间戳（毫秒）                                                                            |
| schema                   | config  | 否  | -      | 数据结构，包括字段名称和字段类型。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。 |
| common-options           |         | 否  | -      | 源插件通用参数                                                                              |
| format                   | String  | 否  | json   | 数据格式                                                                                 |

### topic [String]

当表用作源时要读取数据的主题名称。它也支持通过分号分隔的主题列表，如 'topic-1;topic-2'。

**注意，只能为源指定 "topic-pattern" 和 "topic" 中的一个。**

### topic-pattern [String]

主题名称模式的正则表达式。当作业开始运行时，所有名称与指定正则表达式匹配的主题都将被消费者订阅。

**注意，只能为源指定 "topic-pattern" 和 "topic" 中的一个。**

### topic-discovery.interval [Long]

Pulsar 源发现新主题分区的间隔（毫秒）。非正值禁用主题分区发现。

**注意，此选项仅在使用 'topic-pattern' 选项时有效。**

### subscription.name [String]

为此消费者指定订阅名称。构造消费者时需要此参数。

### client.service-url [String]

Pulsar 服务的服务 URL 提供程序。要使用客户端库连接到 Pulsar，需要指定 Pulsar 协议 URL。

例如，`localhost`: `pulsar://localhost:6650,localhost:6651`。

### admin.service-url [String]

Pulsar 服务管理端点的 HTTP URL。

例如，`http://my-broker.example.com:8080`，或 `https://my-broker.example.com:8443`（用于 TLS）。

### auth.plugin-class [String]

认证插件的名称。

### auth.params [String]

认证插件的参数。

例如，`key1:val1,key2:val2`

### poll.timeout [Integer]

获取记录时的最大等待时间（毫秒）。更长的时间会增加吞吐量但也会增加延迟。

### poll.interval [Long]

获取记录时的间隔时间（毫秒）。更短的时间会增加吞吐量，但也会增加 CPU 负载。

### poll.batch.size [Integer]

轮询时要获取的最大记录数。更长的时间会增加吞吐量但也会增加延迟。

### cursor.startup.mode [Enum]

Pulsar 消费者的启动模式，有效值为 `'EARLIEST'`、`'LATEST'`、`'SUBSCRIPTION'`、`'TIMESTAMP'`。

### cursor.startup.timestamp [Long]

从指定的纪元时间戳（毫秒）开始。

**注意，当 "cursor.startup.mode" 选项使用 `'TIMESTAMP'` 时，此选项是必需的。**

### cursor.reset.mode [Enum]

Pulsar 消费者的游标重置策略，有效值为 `'EARLIEST'`、`'LATEST'`。

**注意，此选项仅在 "cursor.startup.mode" 选项使用 `'SUBSCRIPTION'` 时有效。**

### cursor.stop.mode [String]

Pulsar 消费者的停止模式，有效值为 `'NEVER'`、`'LATEST'` 和 `'TIMESTAMP'`。

**注意，当指定 `'NEVER'` 时，这是一个实时作业，其他模式是离线作业。**

### cursor.stop.timestamp [Long]

从指定的纪元时间戳（毫秒）停止。

**注意，当 "cursor.stop.mode" 选项使用 `'TIMESTAMP'` 时，此选项是必需的。**

### schema [Config]

数据的结构，包括字段名称和字段类型。参考 [Schema-Feature](../../introduction/concepts/schema-feature.md)

## format [String]

数据格式。默认格式是 json，参考 [formats](../formats)。

### 通用选项

源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md) 详见。

## 示例

```
source {
  Pulsar {
  	topic = "example"
  	subscription.name = "seatunnel"
    client.service-url = "pulsar://localhost:6650"
    admin.service-url = "http://my-broker.example.com:8080"
    plugin_output = "test"
  }
}
```

## 变更日志

<ChangeLog />

