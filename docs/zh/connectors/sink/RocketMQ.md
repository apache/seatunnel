import ChangeLog from '../changelog/connector-rocketmq.md';

# RocketMQ

> RocketMQ Sink 连接器

## 支持的 Apache RocketMQ 版本

- 4.9.0 或更新版本

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [变更数据捕获](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 描述

将 SeaTunnel 数据行写入 Apache RocketMQ topic。该 Sink 支持 JSON 和文本消息体、消息 tag、同步发送、按字段选择分区，以及在 `exactly.once = true` 时使用事务消息保证精确一次写入。

## Sink 参数

| 参数名 | 类型 | 是否必填 | 默认值 | 描述 |
|--------|------|----------|--------|------|
| topic | String | 是 | - | RocketMQ topic 名称。 |
| name.srv.addr | String | 是 | - | RocketMQ NameServer 地址，例如 `localhost:9876`。 |
| acl.enabled | Boolean | 否 | false | 是否启用 RocketMQ ACL 鉴权。 |
| access.key | String | 否 | - | 访问密钥。`acl.enabled = true` 时必填。 |
| secret.key | String | 否 | - | 秘密密钥。`acl.enabled = true` 时必填。 |
| producer.group | String | 否 | SeaTunnel-Producer-Group | RocketMQ 生产者组 ID。 |
| tag | String | 否 | - | 写入每条消息时使用的 RocketMQ tag。 |
| partition.key.fields | List | 否 | - | 会被序列化为 RocketMQ 消息 key 的字段名。配置的字段必须存在于上游 schema 中。 |
| format | String | 否 | json | 消息格式。支持 `json` 和 `text`。 |
| field.delimiter | String | 否 | `,` | `format = text` 时使用的字段分隔符。 |
| producer.send.sync | Boolean | 否 | false | 是否同步发送消息。为 `false` 时异步发送。 |
| exactly.once | Boolean | 否 | false | 是否使用事务消息实现精确一次写入。 |
| max.message.size | int | 否 | 4194304 | 最大消息体大小，单位字节。 |
| send.message.timeout | int | 否 | 3000 | 发送消息超时时间，单位毫秒。 |
| common-options | config | 否 | - | Sink 连接器通用参数，详情请参考 [Sink 通用参数](../common-options/sink-common-options.md)。 |

## 参数说明

### partition.key.fields

`partition.key.fields` 控制 RocketMQ 消息 key。SeaTunnel 会把这些字段的值序列化成
JSON，并写入 `Message.keys`。在非事务发送时，同一个 key 还会交给 RocketMQ
的哈希队列选择器，因此相同 key 的数据会进入同一个队列。如果不配置该参数，则由
RocketMQ 自行选择队列。

例如，上游字段中有 `c_int` 时，可以这样把 `c_int` 用作消息 key：

```hocon
partition.key.fields = ["c_int"]
```

### exactly.once

Sink 支持通过 RocketMQ 事务消息实现精确一次写入。该能力默认关闭。确认 RocketMQ 集群和作业 checkpoint 配置满足事务写入要求后，可设置 `exactly.once = true`。

当 `format = text` 时，SeaTunnel 会按上游 schema 的字段顺序序列化，并使用 `field.delimiter` 拼接字段。当 `format = json` 时，每行数据会写成一个 JSON 对象。

### producer.send.sync

`producer.send.sync = true` 表示生产者会等待 RocketMQ 确认每次发送请求。默认值为 `false` 时，消息会异步发送。

## 任务示例

### 写入 JSON 消息

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
        c_string = string
        c_int = int
        c_timestamp = timestamp
      }
    }
  }
}

sink {
  Rocketmq {
    name.srv.addr = "rocketmq-e2e:9876"
    topic = "test_topic"
    partition.key.fields = ["c_int"]
    producer.send.sync = true
  }
}
```

### 写入文本消息

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
  Rocketmq {
    name.srv.addr = "rocketmq-e2e:9876"
    topic = "test_text_topic"
    format = text
    field.delimiter = ","
    producer.send.sync = true
  }
}
```

### 写入带 tag 的消息

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
        c_string = string
        c_int = int
      }
    }
  }
}

sink {
  Rocketmq {
    name.srv.addr = "rocketmq-e2e:9876"
    topic = "test_topic_message_tag"
    tag = "test_tag"
    partition.key.fields = ["c_string"]
    producer.send.sync = true
  }
}
```

### RocketMQ 读写 RocketMQ

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Rocketmq {
    name.srv.addr = "rocketmq-e2e:9876"
    topics = "test_topic_source"
    plugin_output = "rocketmq_table"
    format = json
    start.mode = "CONSUME_FROM_FIRST_OFFSET"
    consumer.group = "rocketmq_to_rocketmq_group"
    schema = {
      fields {
        id = bigint
        c_string = string
      }
    }
  }
}

sink {
  Rocketmq {
    plugin_input = "rocketmq_table"
    name.srv.addr = "rocketmq-e2e:9876"
    topic = "test_topic_sink"
    partition.key.fields = ["id"]
    exactly.once = true
  }
}
```

## 变更日志

<ChangeLog />
