import ChangeLog from '../changelog/connector-rocketmq.md';

# RocketMQ

> RocketMQ 源连接器

## 支持的 Apache RocketMQ 版本

- 4.9.0（或更新版本，供参考）

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [x] [流](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行性](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)

## 描述

Apache RocketMQ 的源连接器。

## 源选项

| 参数名                                 | 类型      | 必须 | 默认值                        | 描述                                                                                                                                                            |
|-------------------------------------|---------|----|----------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topics                              | String  | 是  | -                          | RocketMQ 主题名称。如果有多个主题，使用 `,` 分隔，例如：`"tpc1,tpc2"`。                                                                                                             |
| name.srv.addr                       | String  | 是  | -                          | RocketMQ 名称服务器集群地址。                                                                                                                                           |
| tags                                | String  | 否  | -                          | RocketMQ 标签名称。如果有多个标签，使用 `,` 分隔，例如：`"tag1,tag2"`。                                                                                                             |
| acl.enabled                         | Boolean | 否  | false                      | 如果为 true，启用访问控制，需要配置访问密钥和秘密密钥。                                                                                                                                |
| access.key                          | String  | 否  |                            | 访问密钥                                                                                                                                                          |
| secret.key                          | String  | 否  |                            | 当 ACL_ENABLED 为 true 时，秘密密钥不能为空。                                                                                                                              |
| batch.size                          | int     | 否  | 100                        | RocketMQ 消费者拉取批大小                                                                                                                                             |
| consumer.group                      | String  | 否  | SeaTunnel-Consumer-Group   | RocketMQ 消费者组 ID，用于区分不同的消费者组。                                                                                                                                 |
| commit.on.checkpoint                | Boolean | 否  | true                       | 如果为 true，消费者的偏移量将在后台定期提交。                                                                                                                                     |
| schema                              |         | 否  | -                          | 数据的结构，包括字段名称和字段类型。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。                                                                         |
| format                              | String  | 否  | json                       | 数据格式。默认格式是 json。可选 text 格式。默认字段分隔符是 ","。如果自定义分隔符，添加 "field.delimiter" 选项。                                                                                     |
| field.delimiter                     | String  | 否  | ,                          | 自定义数据格式的字段分隔符                                                                                                                                                 |
| start.mode                          | String  | 否  | CONSUME_FROM_GROUP_OFFSETS | 消费者的初始消费模式，有几种类型：[CONSUME_FROM_LAST_OFFSET],[CONSUME_FROM_FIRST_OFFSET],[CONSUME_FROM_GROUP_OFFSETS],[CONSUME_FROM_TIMESTAMP],[CONSUME_FROM_SPECIFIC_OFFSETS] |
| start.mode.offsets                  |         | 否  |                            | 消费模式为 "CONSUME_FROM_SPECIFIC_OFFSETS" 所需的偏移量                                                                                                                  |
| start.mode.timestamp                | Long    | 否  |                            | 消费模式为 "CONSUME_FROM_TIMESTAMP" 所需的时间。                                                                                                                         |
| partition.discovery.interval.millis | long    | 否  | -1                         | 动态发现主题和分区的间隔。                                                                                                                                                 |
| ignore_parse_errors                 | Boolean | 否  | false                      | 可选标志，跳过解析错误而不是失败。                                                                                                                                             |
| common-options                      | config  | 否  | -                          | 源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md) 详见。                                                                                           |

### start.mode.offsets

消费模式为 "CONSUME_FROM_SPECIFIC_OFFSETS" 所需的偏移量。

例如：

```hocon
start.mode.offsets = {
  topic1-0 = 70
  topic1-1 = 10
  topic1-2 = 10
}
```

## 任务示例

### 简单

> 消费者读取 Rocketmq 数据并将其打印到控制台

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
    schema = {
      fields {
        id = bigint
        c_map = "map<string, smallint>"
        c_array = "array<tinyint>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(2, 1)"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
  }
}

transform {
  # 如果您想了解有关如何配置 seatunnel 的更多信息并查看完整的转换插件列表，
  # 请访问 https://seatunnel.apache.org/docs/category/transform
}

sink {
  Console {
  }
}
```

### 指定格式消费简单

> 当我以 json 格式消费主题数据并解析，每次拉取的条数是 400，消费从原始位置开始

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Rocketmq {
    name.srv.addr = "localhost:9876"
    topics = "test_topic"
    plugin_output = "rocketmq_table"
    start.mode = "CONSUME_FROM_FIRST_OFFSET"
    batch.size = "400"
    consumer.group = "test_topic_group"
    format = json
    schema = {
      fields {
        c_map = "map<string, string>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(30, 8)"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
  }
}

transform {
  # 如果您想了解有关如何配置 seatunnel 的更多信息并查看完整的转换插件列表，
  # 请访问 https://seatunnel.apache.org/docs/category/transform
}

sink {
  Console {
  }
}
```

## 变更日志

<ChangeLog />

