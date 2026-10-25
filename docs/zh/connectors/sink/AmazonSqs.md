import ChangeLog from '../changelog/connector-amazonsqs.md';

# AmazonSqs

> Amazon SQS 写入连接器

## 描述

Amazon SQS 写入连接器用于把每条输入的 SeaTunnel 行数据写入一个 Amazon SQS 队列 URL。
连接器会按照 `format` 序列化数据，并把序列化后的内容作为 SQS 消息体发送。

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [变更数据捕获](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 写入选项

| 名称                | 类型     | 是否必填 | 默认值  | 描述                                                                                                                   |
|-------------------|--------|------|------|----------------------------------------------------------------------------------------------------------------------|
| url               | String | 是    | -    | 要写入的完整 SQS 队列 URL，例如 `https://sqs.us-east-1.amazonaws.com/123456789012/sink_queue`。                             |
| region            | String | 是    | -    | SQS 队列所在的 AWS 区域，例如 `us-east-1`。                                                                                     |
| access_key_id     | String | 否    | -    | AWS access key ID。和 `secret_access_key` 一起配置时使用静态凭证；两者都不配置时使用 AWS 默认凭证链。                                    |
| secret_access_key | String | 否    | -    | AWS secret access key。和 `access_key_id` 一起配置时使用静态凭证。                                                            |
| format            | String | 否    | json | 消息体格式。支持 `json`、`text`、`canal_json`、`debezium_json`。                                                             |
| field_delimiter   | String | 否    | ,    | 当 `format = text` 时使用的字段分隔符。                                                                                       |
| common-options    |        | 否    | -    | Sink 插件通用参数，详见 [Sink Common Options](../common-options/sink-common-options.md)。                                      |

`url` 可以指向 AWS SQS，也可以指向兼容 SQS 的本地服务，例如 `http://sqs-host:4566/000000000000/sink_queue`。

## 格式说明

- `json`：把每行数据写成 JSON 对象。
- `text`：用 `field_delimiter` 拼接每行中的字段。
- `canal_json`：写出 Canal JSON 消息，详见 [Canal JSON](../formats/canal-json.md)。
- `debezium_json`：写出 Debezium JSON 消息，详见 [Debezium JSON](../formats/debezium-json.md)。
- 当前写入连接器只发送消息体，不提供 SQS message attributes、delay seconds、deduplication ID 或 message group ID 等配置。
- `access_key_id` 和 `secret_access_key` 是可选项；如果使用静态 AWS 凭证，需要两个一起配置。
- 该写入连接器会把每条 SeaTunnel 行数据发送成一条 SQS 消息，不会把多行数据合并到一次 SQS 请求里。

## 任务示例

### 在本地兼容队列之间复制消息

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  AmazonSqs {
    url = "http://sqs-host:4566/000000000000/source_queue"
    access_key_id = "1234"
    secret_access_key = "abcd"
    region = "us-east-1"
    schema = {
      fields {
        name = "string"
      }
    }
  }
}

sink {
  AmazonSqs {
    url = "http://sqs-host:4566/000000000000/sink_queue"
    access_key_id = "1234"
    secret_access_key = "abcd"
    region = "us-east-1"
  }
}
```

### 写入 JSON 消息

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 1
    schema = {
      fields {
        name = string
      }
    }
    rows = [
      {
        kind = INSERT
        fields = ["test_name"]
      }
    ]
  }
}

sink {
  AmazonSqs {
    url = "https://sqs.us-east-1.amazonaws.com/123456789012/sink_queue"
    region = "us-east-1"
    access_key_id = "AKIA..."
    secret_access_key = "SECRET..."
  }
}
```

### 使用自定义分隔符写入文本消息

```hocon
source {
  FakeSource {
    schema = {
      fields {
        artist = string
        album = string
        release_year = int
      }
    }
  }
}

sink {
  AmazonSqs {
    url = "https://sqs.us-east-1.amazonaws.com/123456789012/sink_queue"
    region = "us-east-1"
    format = text
    field_delimiter = "|"
  }
}
```

### 写入 Canal JSON 消息

将 `format` 设为 `canal_json`，每行 SeaTunnel 数据会被序列化为 Canal JSON 变更事件。下游是 Canal JSON 消费者（如 Canal → Kafka 桥接或兼容 Canal 的 BigQuery 加载器）时非常有用。

```hocon
sink {
  AmazonSqs {
    url = "https://sqs.us-east-1.amazonaws.com/123456789012/sink_queue"
    region = "us-east-1"
    format = canal_json
  }
}
```

## 变更日志

<ChangeLog />
