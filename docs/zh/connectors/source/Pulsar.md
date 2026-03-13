import ChangeLog from '../changelog/connector-pulsar.md';

# Apache Pulsar

> Apache Pulsar Source 连接器

## 描述

Apache Pulsar 的 Source 连接器。

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行读取](../../introduction/concepts/connector-v2-features.md)
- [ ] [用户自定义 split](../../introduction/concepts/connector-v2-features.md)

## 参数

| name                     | type    | required | default value |
|--------------------------|---------|----------|---------------|
| topic                    | String  | No       | -             |
| topic-pattern            | String  | No       | -             |
| table_path               | String  | No       | -             |
| tables_configs           | Array   | No       | -             |
| topic-discovery.interval | Long    | No       | -1            |
| subscription.name        | String  | No       | -             |
| client.service-url       | String  | Yes      | -             |
| admin.service-url        | String  | Yes      | -             |
| auth.plugin-class        | String  | No       | -             |
| auth.params              | String  | No       | -             |
| poll.timeout             | Integer | No       | 100           |
| poll.interval            | Long    | No       | 50            |
| poll.batch.size          | Integer | No       | 500           |
| cursor.startup.mode      | Enum    | No       | LATEST        |
| cursor.startup.timestamp | Long    | No       | -             |
| cursor.reset.mode        | Enum    | No       | LATEST        |
| cursor.stop.mode         | Enum    | No       | NEVER         |
| cursor.stop.timestamp    | Long    | No       | -             |
| schema                   | config  | No       | -             |
| common-options           |         | No       | -             |
| format                   | String  | No       | json          |

### topic [String]

单表读取的 topic 名称。也支持用逗号分隔多个 topic，例如 `'topic-1,topic-2'`。

**注意，只能在 `topic`、`topic-pattern` 和 `tables_configs` 中选择一种配置方式。**

### topic-pattern [String]

用于匹配 topic 名称的正则表达式。作业启动时，所有匹配该正则的 topic 都会被订阅。

**注意，只能在 `topic`、`topic-pattern` 和 `tables_configs` 中选择一种配置方式。**

### table_path [String]

单个 `tables_configs` 配置项对应的逻辑表标识。该参数主要用于多表模式。

### tables_configs [Array]

多表读取配置。每个 item 都可以覆盖全局默认值，例如 `format`、cursor 相关参数和 `subscription.name`。

每个 item 必须且只能配置以下其中一个参数：

- `topic`
- `topic-pattern`

额外约束：

- 当使用 `topic-pattern` 时，必须显式配置 `table_path`。
- `subscription.name` 必须在全局或 item 内存在。
- 多表模式当前只支持 `JSON` 和 `CANAL_JSON`。
- 在 batch 模式下，所有表都必须是 bounded。只要有任意一个表使用 `cursor.stop.mode = NEVER`，整个 source 就会被视为 unbounded，并拒绝在 batch 作业中运行。

### topic-discovery.interval [Long]

Pulsar Source 发现新 topic 分区的时间间隔，单位为毫秒。小于等于 0 表示关闭动态发现。

**注意，该参数只在使用 `topic-pattern` 时生效。**

### subscription.name [String]

消费者订阅名。对每个最终生效的表配置都是必需的；在多表模式下，可以定义在全局，也可以在 `tables_configs` 的 item 中单独覆盖。

### client.service-url [String]

Pulsar 服务地址。

例如：`pulsar://localhost:6650,localhost:6651`。

### admin.service-url [String]

Pulsar Admin HTTP 地址。

例如：`http://my-broker.example.com:8080`，或开启 TLS 时使用 `https://my-broker.example.com:8443`。

### auth.plugin-class [String]

认证插件类名。

### auth.params [String]

认证插件参数。

例如：`key1:val1,key2:val2`

### poll.timeout [Integer]

单次拉取数据的最长等待时间，单位毫秒。值越大，吞吐通常越高，但延迟也会增加。

### poll.interval [Long]

轮询间隔时间，单位毫秒。值越小，吞吐通常越高，但 CPU 开销也会增加。

### poll.batch.size [Integer]

单次 `pollNext` 最多处理的消息数。

### cursor.startup.mode [Enum]

启动模式，可选值为 `'EARLIEST'`、`'LATEST'`、`'SUBSCRIPTION'`、`'TIMESTAMP'`。

### cursor.startup.timestamp [Long]

当 `cursor.startup.mode = TIMESTAMP` 时使用的起始时间戳，单位毫秒。

### cursor.reset.mode [Enum]

当 `cursor.startup.mode = SUBSCRIPTION` 时使用的 cursor reset 策略，可选值为 `'EARLIEST'`、`'LATEST'`。

### cursor.stop.mode [Enum]

停止模式，可选值为 `'NEVER'`、`'LATEST'`、`'TIMESTAMP'`。

当值为 `'NEVER'` 时，作业是实时流式读取；其他模式表示有界读取。

### cursor.stop.timestamp [Long]

当 `cursor.stop.mode = TIMESTAMP` 时使用的停止时间戳，单位毫秒。

### schema [Config]

数据结构定义，包括字段名和字段类型。参考 [Schema Feature](../../introduction/concepts/schema-feature.md)。

## format [String]

数据格式。默认值为 `json`。更多格式说明参考 [formats](../formats)。

### 通用参数

Source 插件通用参数请参考 [Source Common Options](../common-options/source-common-options.md)。

## 示例

```hocon
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

## 多表示例

```hocon
source {
  Pulsar {
    subscription.name = "seatunnel-sub"
    client.service-url = "pulsar://localhost:6650"
    admin.service-url = "http://localhost:8080"
    cursor.startup.mode = "EARLIEST"
    cursor.stop.mode = "NEVER"
    format = "json"

    tables_configs = [
      {
        table_path = "default.orders"
        topic = "persistent://public/default/orders"
        schema = {
          fields {
            order_id = "bigint"
            user_id = "int"
          }
        }
      },
      {
        table_path = "default.users"
        topic-pattern = "persistent://public/default/users-.*"
        subscription.name = "users-sub"
        format = "canal_json"
        schema = {
          fields {
            user_id = "int"
            name = "string"
          }
        }
      }
    ]
  }
}
```

如果用于 batch 作业，请将 `cursor.stop.mode = "NEVER"` 改为有界模式，例如 `LATEST` 或 `TIMESTAMP`。

## 变更日志

<ChangeLog />
