# RocketMQ

> RocketMQ sink 连接器

## 支持Apache RocketMQ版本

- 4.9.0 (或更新版本，供参考)

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [精确一次](../../concept/connector-v2-features.md)

默认情况下，我们将使用2pc来保证消息精确一次到RocketMQ。

## 描述

将数据行写入Apache RocketMQ主题

## Sink 参数

|         名称         |  类型   | 是否必填 |         默认值          |                                                                             描述                                                                             |
|----------------------|---------|----------|--------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic                | string  | 是      | -                        | `RocketMQ topic` 名称.                                                                                                                                              |
| name.srv.addr        | string  | 是      | -                        | `RocketMQ`名称服务器集群地址。                                                                                                                             |
| acl.enabled          | Boolean | 否       | false                    | false                                                                                                                                                               |
| access.key           | String  | 否       |                          | 当ACL_ENABLED为true时，access key不能为空。                                                                                                                |
| secret.key           | String  | 否       |                          |  当ACL_ENABLED为true时, secret key 不能为空。                                                                                                                |
| producer.group       | String  | 否       | SeaTunnel-producer-Group | SeaTunnel-producer-Group                                                                                                                                            |
| tag                  | String  | 否       | -                        | `RocketMQ`消息标签。                                                                                                                                             |
| partition.key.fields | array   | 否       | -                        | -                                                                                                                                                                   |
| format               | String  | 否       | json                     | 数据格式。默认格式为json。可选text格式。默认字段分隔符为“，”。如果自定义分隔符，请添加“field_delimiter”选项。 |
| field.delimiter      | String  | 否       | ,                        | 自定义数据格式的字段分隔符。                                                                                                                      |
| producer.send.sync   | Boolean | 否       | false                    | 如果为 true, 则消息将同步发送。                                                                                                                             |
| common-options       | config  | 否       | -                        | Sink插件常用参数，请参考[sink common options]（../Sink-common-Options.md）了解详细信息。                                                        |

### partition.key.fields [array]

配置哪些字段用作RocketMQ消息的键。
例如，如果要使用上游数据中的字段值作为键，可以为此属性指定字段名。
上游数据如下：

| name | age |     data      |
|------|-----|---------------|
| Jack | 16  | data-example1 |
| Mary | 23  | data-example2 |

如果name被设置为主键，那么name列的哈希值将决定消息被发送到哪个分区。

## 任务示例

### Fake 到 RocketMQ 简单示例

>数据是随机生成的，并异步发送到测试主题

```hocon
env {
  parallelism = 1
}

source {
  FakeSource {
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
	#如果你想了解更多关于如何配置seatunnel的信息，并查看转换插件的完整列表，
	#请前往https://seatunnel.apache.org/docs/category/transform
}

sink {
  Rocketmq {
    name.srv.addr = "localhost:9876"
    topic = "test_topic"
  }
}

```

### Rocketmq 到 Rocketmq 简单示例

> 使用RocketMQ时，会向c_int字段写入哈希数，该哈希数表示写入不同分区的分区数量。这是默认的异步写入方式

```hocon
env {
  parallelism = 1
}

source {
  Rocketmq {
    name.srv.addr = "localhost:9876"
    topics = "test_topic"
    plugin_output = "rocketmq_table"
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

sink {
  Rocketmq {
    name.srv.addr = "localhost:9876"
    topic = "test_topic_sink"
    partition.key.fields = ["c_int"]
  }
}
```

### 时间戳消费写入示例

>这是流消费中特定的时间戳消费，当添加新分区时，程序将定期刷新感知和消费，并写入另一个主题类型

```hocon

env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  Rocketmq {
    name.srv.addr = "localhost:9876"
    topics = "test_topic"
    plugin_output = "rocketmq_table"
    start.mode = "CONSUME_FROM_FIRST_OFFSET"
    batch.size = "400"
    consumer.group = "test_topic_group"
    format = "json"
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
	#如果你想了解更多关于如何配置seatunnel的信息，并查看转换插件的完整列表，
	#请前往https://seatunnel.apache.org/docs/category/transform
}
sink {
  Rocketmq {
    name.srv.addr = "localhost:9876"
    topic = "test_topic"
    partition.key.fields = ["c_int"]
    producer.send.sync = true
  }
}
```

