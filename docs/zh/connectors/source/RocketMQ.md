import ChangeLog from '../changelog/connector-rocketmq.md';

# RocketMQ

> RocketMQ 源连接器

## 支持的 Apache RocketMQ 版本

- 4.9.0 或更新版本

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列裁剪](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表读取](../../introduction/concepts/connector-v2-features.md)

## 描述

从 Apache RocketMQ topic 读取消息。连接器既可以用一套 schema 读取一个或多个 topic，也可以通过 `tables_configs` 读取多张不同结构的表。

## 源参数

| 参数名 | 类型 | 是否必填 | 默认值 | 描述 |
|--------|------|----------|--------|------|
| name.srv.addr | String | 是 | - | RocketMQ NameServer 地址，例如 `localhost:9876`。 |
| topics | String | 否 | - | topic 名称，多个 topic 使用逗号分隔，例如 `"topic_a,topic_b"`。`topics`、`tables_configs` 和 `table_list` 只能配置其中一个。 |
| tables_configs | List | 否 | - | 多表读取配置。每一项必须包含 `topics`，并可配置 `format`、`schema`、`tags`、`start.mode`、`start.mode.timestamp`、`start.mode.offsets` 和 `ignore_parse_errors`。 |
| table_list | List | 否 | - | 已废弃，请使用 `tables_configs`。 |
| tags | String | 否 | - | tag 名称，多个 tag 使用逗号分隔。只消费匹配这些 tag 的消息。 |
| acl.enabled | Boolean | 否 | false | 是否启用 RocketMQ ACL 鉴权。 |
| access.key | String | 否 | - | 访问密钥。`acl.enabled = true` 时必填。 |
| secret.key | String | 否 | - | 秘密密钥。`acl.enabled = true` 时必填。 |
| batch.size | int | 否 | 100 | 每次最多拉取的消息数。 |
| consumer.group | String | 否 | SeaTunnel-Consumer-Group | RocketMQ 消费者组 ID。 |
| commit.on.checkpoint | Boolean | 否 | true | 是否在 SeaTunnel checkpoint 完成后提交消费位点。 |
| schema | config | 否 | - | 消息结构。详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。不配置时，连接器按文本读取消息体。 |
| format | String | 否 | json | 消息格式。支持 `json` 和 `text`。 |
| field.delimiter | String | 否 | `,` | `format = text` 时使用的字段分隔符。 |
| start.mode | String | 否 | CONSUME_FROM_GROUP_OFFSETS | 启动消费位置。支持：`CONSUME_FROM_LAST_OFFSET`、`CONSUME_FROM_FIRST_OFFSET`、`CONSUME_FROM_GROUP_OFFSETS`、`CONSUME_FROM_TIMESTAMP`、`CONSUME_FROM_SPECIFIC_OFFSETS`。 |
| start.mode.offsets | Map | 否 | - | `start.mode = CONSUME_FROM_SPECIFIC_OFFSETS` 时必填。key 格式为 `topic-queueId`，例如 `test_topic-0`。 |
| start.mode.timestamp | Long | 否 | - | `start.mode = CONSUME_FROM_TIMESTAMP` 时必填，单位是毫秒时间戳。 |
| partition.discovery.interval.millis | long | 否 | -1 | 动态发现 topic 和分区的间隔，单位毫秒。`-1` 表示不启用动态发现。 |
| ignore_parse_errors | Boolean | 否 | false | 是否跳过解析失败的 JSON 消息。 |
| consumer.poll.timeout.millis | long | 否 | 5000 | 拉取消息的超时时间，单位毫秒。 |
| common-options | config | 否 | - | 源连接器通用参数，详情请参考 [源通用参数](../common-options/source-common-options.md)。 |

## 参数说明

### 启动消费位置

`start.mode` 用来控制从哪里开始读：

- `CONSUME_FROM_GROUP_OFFSETS`：从消费者组已提交的位点开始读。
- `CONSUME_FROM_FIRST_OFFSET`：从最早可用位点开始读。
- `CONSUME_FROM_LAST_OFFSET`：从最新位点开始读。
- `CONSUME_FROM_TIMESTAMP`：从 `start.mode.timestamp` 对应时间之后的第一条消息开始读。
- `CONSUME_FROM_SPECIFIC_OFFSETS`：从 `start.mode.offsets` 指定的位点开始读。

当 `start.mode = CONSUME_FROM_TIMESTAMP` 时，`start.mode.timestamp` 必须是非负的毫秒时间戳，并且不能晚于任务运行时的当前时间。

```hocon
start.mode = "CONSUME_FROM_SPECIFIC_OFFSETS"
start.mode.offsets = {
  test_topic-0 = 50
}
```

```hocon
start.mode = "CONSUME_FROM_TIMESTAMP"
start.mode.timestamp = 1667179890315
```

### 消息格式

当 `format = json` 时，请配置 `schema`，SeaTunnel 会按 schema 将 JSON 消息体解析成有类型的字段。配置 `ignore_parse_errors = true` 后，遇到无法解析的 JSON 消息会跳过，而不是让任务失败。

当 `format = text` 时，SeaTunnel 会按 `field.delimiter` 拆分消息体，并按照 schema 字段顺序映射数据。如果不配置 `schema`，消息体会作为单个文本值读取。

### 多表读取

当不同 topic 的字段结构不一样时，使用 `tables_configs`。每一项都必须包含 `topics`，并且可以单独配置 `schema`、`format`、`tags` 和启动消费位置。如果没有配置 `schema.table`，输出表名默认使用 topic 名称。

`topics`、`tables_configs` 和已废弃的 `table_list` 互斥，只能配置其中一个。在 `tables_configs` 中，单个条目未配置的参数会沿用顶层默认值，因此每个条目只需要覆盖该 topic 特有的 schema、tag 或启动位置。

如果某个 `tables_configs` 条目使用 `start.mode = CONSUME_FROM_TIMESTAMP`，必须同时配置 `start.mode.timestamp`。如果使用 `start.mode = CONSUME_FROM_SPECIFIC_OFFSETS`，必须同时配置非空的 `start.mode.offsets`。

## 任务示例

### 读取 JSON 消息

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Rocketmq {
    name.srv.addr = "rocketmq-e2e:9876"
    topics = "test_topic_json"
    plugin_output = "rocketmq_table"
    format = json
    schema = {
      fields {
        id = bigint
        c_string = string
        c_int = int
        c_timestamp = timestamp
      }
    }
  }
}

sink {
  Console {
    plugin_input = "rocketmq_table"
  }
}
```

### 按 tag 读取文本消息

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Rocketmq {
    name.srv.addr = "rocketmq-e2e:9876"
    topics = "test_topic_text"
    plugin_output = "rocketmq_table"
    format = text
    field.delimiter = ","
    tags = "tag_a,tag_b"
    schema = {
      fields {
        id = bigint
        content = string
      }
    }
  }
}

sink {
  Console {
    plugin_input = "rocketmq_table"
  }
}
```

### 从指定 offset 读取

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Rocketmq {
    name.srv.addr = "rocketmq-e2e:9876"
    topics = "test_topic_source"
    plugin_output = "rocketmq_table"
    format = json
    start.mode = "CONSUME_FROM_SPECIFIC_OFFSETS"
    start.mode.offsets = {
      test_topic_source-0 = 50
    }
    schema = {
      fields {
        id = bigint
      }
    }
  }
}

sink {
  Console {
    plugin_input = "rocketmq_table"
  }
}
```

### 读取多个不同结构的 topic

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Rocketmq {
    name.srv.addr = "rocketmq-e2e:9876"
    start.mode = "CONSUME_FROM_LAST_OFFSET"
    tables_configs = [
      {
        topics = "test_topic_multi_a"
        start.mode = "CONSUME_FROM_FIRST_OFFSET"
        format = json
        schema = {
          fields {
            id = bigint
            c_string = string
          }
        }
      },
      {
        topics = "test_topic_multi_b"
        start.mode = "CONSUME_FROM_FIRST_OFFSET"
        tags = "tag_b"
        format = json
        schema = {
          table = "rocketmq_multi_custom"
          fields {
            id = bigint
            description = string
          }
        }
      }
    ]
  }
}

sink {
  Console {}
}
```

### 从指定时间戳读取

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Rocketmq {
    name.srv.addr = "rocketmq-e2e:9876"
    topics = "test_topic_source"
    plugin_output = "rocketmq_table"
    format = json
    start.mode = "CONSUME_FROM_TIMESTAMP"
    start.mode.timestamp = 1667179890315
    schema = {
      fields {
        id = bigint
      }
    }
  }
}

sink {
  Console {
    plugin_input = "rocketmq_table"
  }
}
```

## 变更日志

<ChangeLog />
