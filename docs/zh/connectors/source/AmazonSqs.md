import ChangeLog from '../changelog/connector-amazonsqs.md';

# AmazonSqs

> Amazon SQS 源连接器

## 描述

Amazon SQS 源连接器用于从一个 Amazon SQS 队列 URL 读取消息。连接器会按照 `format` 和
`schema` 解析每条消息的消息体，然后输出为 SeaTunnel 行数据。

该连接器使用单个 reader，处理完本次接收到的消息后任务会结束，适合从队列中做有界读取。

每次 receive 请求最多从 SQS 拉取 10 条消息。如果队列里还有更多消息，需要再次运行任务，或者使用上游调度方式重复触发这种有界读取。

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 源选项

| 名称                             | 类型      | 是否必填 | 默认值   | 描述                                                                                                                     |
|--------------------------------|---------|------|-------|------------------------------------------------------------------------------------------------------------------------|
| url                            | String  | 是    | -     | 要读取的完整 SQS 队列 URL，例如 `https://sqs.us-east-1.amazonaws.com/123456789012/source_queue`。                              |
| region                         | String  | 是    | -     | SQS 队列所在的 AWS 区域，例如 `us-east-1`。                                                                                       |
| schema                         | Config  | 是    | -     | 消息体结构，包含字段名和字段类型。更多说明请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。                                  |
| access_key_id                  | String  | 否    | -     | AWS access key ID。和 `secret_access_key` 一起配置时使用静态凭证；两者都不配置时使用 AWS 默认凭证链。                                      |
| secret_access_key              | String  | 否    | -     | AWS secret access key。和 `access_key_id` 一起配置时使用静态凭证。                                                              |
| format                         | String  | 否    | json  | 消息体格式。支持 `json`、`text`、`canal_json`、`debezium_json`。                                                               |
| field_delimiter                | String  | 否    | ,     | 当 `format = text` 时使用的字段分隔符。                                                                                         |
| ignore_parse_errors            | Boolean | 否    | false | 是否跳过无法解析的消息并继续处理，而不是让本次轮询失败。                                                                                         |
| delete_message                 | Boolean | 否    | false | 读取并成功解析消息后，是否从队列中删除该消息。                                                                                              |
| message_group_id               | String  | 否    | -     | 为兼容保留的消息分组 ID 选项，普通 SQS 读取不需要配置。                                                                                       |
| debezium_record_include_schema | Boolean | 否    | true  | Debezium JSON 消息是否包含 schema。仅在 `format = debezium_json` 时使用。                                                        |
| common-options                 |         | 否    | -     | 源插件通用参数，详见 [Source Common Options](../common-options/source-common-options.md)。                                          |

`url` 可以指向 AWS SQS，也可以指向兼容 SQS 的本地服务，例如 `http://sqs-host:4566/000000000000/source_queue`。

## 格式说明

- `json`：把每条消息体按 JSON 对象解析，并要求字段能对应到 `schema`。
- `text`：按 `field_delimiter` 切分消息体，并按 `schema` 中字段顺序映射。
- `canal_json`：读取 Canal JSON 消息，详见 [Canal JSON](../formats/canal-json.md)。
- `debezium_json`：读取 Debezium JSON 消息，详见 [Debezium JSON](../formats/debezium-json.md)。
- 一条 `canal_json` 或 `debezium_json` 消息可能产生多行。例如，更新事件会产生更新前和更新后的行。
- `ignore_parse_errors = false` 会让本次轮询失败并保留无法解析的消息。设置为 `true` 时，源连接器会跳过该消息并继续处理本批次中的其他消息。
- 当 `ignore_parse_errors` 和 `delete_message` 都为 `true` 时，跳过的消息会从 SQS 中删除。如果需要保留这些消息以便重新投递，请保持 `delete_message = false`。
- 对于产生多行的消息，只有在所有行都成功收集后才会删除消息。收集失败时，SQS 消息会保留以便重新投递。
- `delete_message = true` 会删除已经消费的 SQS 消息。如果只是检查或复制消息，建议保留默认值 `false`。
- `access_key_id` 和 `secret_access_key` 是可选项；如果使用静态 AWS 凭证，需要两个一起配置。
- 该源连接器只执行一次 receive 请求，最多读取 10 条消息，然后结束这个有界任务。

## 认证

连接器按以下顺序解析 AWS 凭证：

1. 如果同时配置了 `access_key_id` 和 `secret_access_key`，则使用这对静态凭证。
2. 否则，回退到 AWS 默认凭证链（环境变量、实例角色等）。

针对 LocalStack、ElasticMQ 等 SQS 兼容本地服务进行测试时，把 `url` 指向本地端点（例如 `http://sqs-host:4566/...`），并提供任意非空的 `access_key_id` / `secret_access_key` 即可。SQS 兼容的测试服务通常不会校验请求里的 SigV4 签名，因此任意一对静态凭证都会被接受。

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

### 读取 JSON 消息

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  AmazonSqs {
    url = "https://sqs.us-east-1.amazonaws.com/123456789012/source_queue"
    region = "us-east-1"
    access_key_id = "AKIA..."
    secret_access_key = "SECRET..."
    schema = {
      fields {
        name = string
      }
    }
  }
}

sink {
  Console {}
}
```

### 使用自定义分隔符读取文本消息

```hocon
source {
  AmazonSqs {
    url = "https://sqs.us-east-1.amazonaws.com/123456789012/source_queue"
    region = "us-east-1"
    format = text
    field_delimiter = "#"
    delete_message = true
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
  Console {}
}
```

### 读取 Debezium JSON 消息

当上游系统（例如 Debezium 或其他 CDC 源）以 Debezium 信封形式发布变更事件时，把 `format` 设为 `debezium_json`，并通过 `debezium_record_include_schema` 控制消息中是否包含 schema 字段。

```hocon
source {
  AmazonSqs {
    url = "https://sqs.us-east-1.amazonaws.com/123456789012/cdc_events"
    region = "us-east-1"
    format = debezium_json
    debezium_record_include_schema = true
    schema = {
      fields {
        id = bigint
        name = string
        score = double
      }
    }
  }
}
```

## 变更日志

<ChangeLog />
