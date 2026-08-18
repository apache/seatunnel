import ChangeLog from '../changelog/connector-pulsar.md';

# Pulsar

> Pulsar Sink 连接器

## 引擎支持

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 描述

Pulsar Sink 用于将 SeaTunnel 数据写入 Apache Pulsar topic。它既可以写入一个固定 topic，也可以在多表写入时根据每行数据携带的表标识路由到不同 topic。

## 支持的数据源信息

| 数据源 | 支持的版本 |
|--------|------------|
| Pulsar | Universal  |

## 输出选项

| 名称                     | 类型   | 是否必须 | 默认值              | 描述                                                                 |
|--------------------------|--------|----------|---------------------|----------------------------------------------------------------------|
| topic                    | String | 否       | -                   | 目标 Pulsar topic。普通单表写入必须配置；多表数据带有表标识时可以省略。 |
| client.service-url       | String | 是       | -                   | Pulsar 客户端服务地址，例如 `pulsar://localhost:6650`。               |
| admin.service-url        | String | 是       | -                   | Pulsar 管理端 HTTP 地址，例如 `http://localhost:8080`。               |
| auth.plugin-class        | String | 否       | -                   | Pulsar 认证插件类名。                                                |
| auth.params              | String | 否       | -                   | 认证插件参数。需要和 `auth.plugin-class` 一起配置。                   |
| format                   | String | 否       | json                | 数据格式。默认格式为 json。可选 text 和 avro 格式。                                    |
| field_delimiter          | String | 否       | ,                   | 当 `format = "text"` 时使用的字段分隔符。                             |
| semantics                | Enum   | 否       | AT_LEAST_ONCE       | 写入一致性语义。可选值：`NON`、`AT_LEAST_ONCE`、`EXACTLY_ONCE`。       |
| transaction_timeout      | Int    | 否       | 600                 | Pulsar 事务超时时间，单位为秒。用于 `EXACTLY_ONCE`。                  |
| pulsar.config            | Map    | 否       | -                   | 传给 Pulsar 生产者客户端的额外参数。                                  |
| message.routing.mode     | Enum   | 否       | RoundRobinPartition | 分区 topic 的消息路由模式。可选值：`SinglePartition`、`RoundRobinPartition`。 |
| partition_key_fields     | Array  | 否       | -                   | 用于生成 Pulsar 消息 key 的字段。                                     |
| multi_table_sink_replica | Int    | 否       | 1                   | 多表写入时的写入器副本数。                                           |
| common-options           | Config | 否       | -                   | Sink 插件通用参数，详见 [Sink 通用选项](../common-options/sink-common-options.md)。 |

## 参数解释

### topic [String]

Sink 默认写入的 Pulsar topic。

普通单表任务需要配置 `topic`。多表任务中，如果每行数据带有表标识，Sink 会把这个表标识作为目标 topic；只有当数据没有表标识时才会回退使用 `topic`。

如果数据没有表标识，并且也没有配置 `topic`，任务会直接报配置错误。

### client.service-url [String]

Pulsar 客户端服务地址。请使用 Pulsar 协议，例如 `pulsar://localhost:6650`。

### admin.service-url [String]

Pulsar 管理端 HTTP 地址。

例如：`http://my-broker.example.com:8080`；如果启用了 TLS，可以使用 `https://my-broker.example.com:8443`。

### auth.plugin-class [String]

认证插件类名。

### auth.params [String]

认证插件参数。

例如：`key1:val1,key2:val2`。

### format [String]

数据格式。默认格式为 json。可选 text 和 avro 格式。默认字段分隔符为","。如果自定义分隔符，请添加"field_delimiter"选项。使用 avro 格式时，Avro schema 会从上游数据的 row type 自动推导，无需在 sink 侧单独配置 `schema`。

### field_delimiter [String]

`text` 格式使用的字段分隔符。默认值为 `,`。

### semantics [Enum]

写入一致性语义。

- `AT_LEAST_ONCE`：默认值。作业重启或重试后，消息可能重复。
- `EXACTLY_ONCE`：通过 Pulsar 事务写入。Pulsar 集群必须开启事务能力，并且 `transaction_timeout` 应大于 checkpoint 间隔。
- `NON`：不和 checkpoint 协调，直接发送消息。作业重启、重试或网络异常后，数据可能重复或丢失。

### transaction_timeout [Int]

Pulsar 事务超时时间，单位为秒。默认值为 `600`。

该参数只在 `semantics = "EXACTLY_ONCE"` 时生效。如果事务没有在超时时间内提交，Pulsar 会自动中止该事务。

### pulsar.config [Map]

Pulsar 生产者客户端的额外参数。这些参数会传给 Pulsar producer。

### message.routing.mode [Enum]

分区 topic 的消息路由模式。

- `SinglePartition`：没有消息 key 时，选择一个分区并把所有消息写入该分区；有 key 时，Pulsar 会根据 key 哈希选择分区。
- `RoundRobinPartition`：没有消息 key 时，按轮询方式写入不同分区。Pulsar 的轮询发生在批处理延迟边界上，不是逐条消息轮询。

### partition_key_fields [Array]

用于生成 Pulsar 消息 key 的字段。

例如上游数据包含 `name` 和 `age`，配置 `partition_key_fields = ["name"]` 后，会生成类似 `{"name":"Jack"}` 的 JSON key。所选字段必须存在于上游数据结构中。

### multi_table_sink_replica [Int]

多表写入时的写入器副本数。该配置会对多表 Sink 中的所有目标 topic 生效。

### common options

Sink 插件通用参数，请参考 [Sink 通用选项](../common-options/sink-common-options.md)。

## 任务示例

### 写入 FakeSource 数据到 Pulsar

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 10
    schema = {
      fields {
        c_string = string
        c_int = int
        c_bigint = bigint
        c_double = double
        c_timestamp = timestamp
      }
    }
  }
}

sink {
  Pulsar {
    topic = "topic_test"
    client.service-url = "pulsar://localhost:6650"
    admin.service-url = "http://localhost:8080"
    format = json
    pulsar.config = {
      sendTimeoutMs = 30000
    }
  }
}
```

### 使用消息 Key 写入

```hocon
sink {
  Pulsar {
    topic = "orders"
    client.service-url = "pulsar://localhost:6650"
    admin.service-url = "http://localhost:8080"
    partition_key_fields = ["order_id"]
    message.routing.mode = "SinglePartition"
    format = json
  }
}
```

### 精确一次写入

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

sink {
  Pulsar {
    topic = "orders"
    client.service-url = "pulsar://localhost:6650"
    admin.service-url = "http://localhost:8080"
    semantics = "EXACTLY_ONCE"
    transaction_timeout = 600
    format = json
  }
}
```

### 多表写入

当上游数据带有表标识时，Sink 可以把每行数据路由到同名 topic。此时可以不配置 `topic`。

```hocon
sink {
  Pulsar {
    client.service-url = "pulsar://localhost:6650"
    admin.service-url = "http://localhost:8080"
    multi_table_sink_replica = 2
    format = json
  }
}
```

### 使用自定义分隔符写入文本消息

将 `format` 设置为 `text`，并通过 `field_delimiter` 指定分隔符，把每行序列化成定界文本。下游消费者需要简单扁平格式时可以使用这种方式。

```hocon
sink {
  Pulsar {
    topic = "text_events"
    client.service-url = "pulsar://localhost:6650"
    admin.service-url = "http://localhost:8080"
    format = text
    field_delimiter = "|"
  }
}
```

### 写入 Avro 消息

将 `format` 设置为 `avro` 即可。Avro schema 由上游行类型推导生成，不需要在 Sink 端额外配置 `schema`。

```hocon
sink {
  Pulsar {
    topic = "test_avro_topic_fake_source"
    client.service-url = "pulsar://localhost:6650"
    admin.service-url = "http://localhost:8080"
    format = avro
  }
}
```

### 自定义 Pulsar Producer 属性

通过 `pulsar.config` 传入额外的 producer 属性，这些配置会透传给 Pulsar producer 客户端，可以用来调整超时、批大小、压缩等参数。

```hocon
sink {
  Pulsar {
    topic = "topic_test"
    client.service-url = "pulsar://localhost:6650"
    admin.service-url = "http://localhost:8080"
    format = json
    pulsar.config = {
      sendTimeoutMs = 30000
      batchingMaxMessages = 1000
    }
  }
}
```

## 变更日志

<ChangeLog />
