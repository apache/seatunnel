import ChangeLog from '../changelog/connector-google-pubsub.md';

# GooglePubSub

> Google Pub/Sub Sink 连接器

## 描述

将每条 SeaTunnel 输入行作为一条消息发布到 Google Pub/Sub 主题。

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 参数

| 参数名 | 类型 | 是否必填 | 默认值 |
| --- | --- | --- | --- |
| project_id | string | 是 | - |
| topic | string | 是 | - |
| credentials_path | string | 否 | - |
| emulator_host | string | 否 | - |
| format | enum | 否 | json |
| field_delimiter | string | 否 | , |
| common-options | | 否 | - |

### project_id [string]

目标主题所属的 Google Cloud 项目 ID。

### topic [string]

目标 Pub/Sub 主题 ID。启动作业前必须先创建该主题。

### credentials_path [string]

Google Cloud 服务账号 JSON 密钥文件的路径。未配置时，连接器使用 [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)。

### emulator_host [string]

Pub/Sub 模拟器的主机和端口，例如 `pubsub-emulator:8085`。配置后，连接器使用无凭证的明文连接。生产环境中不要使用该选项。

### format [enum]

消息负载格式。支持以下值：

- `json`：将行写为 JSON 对象。
- `text`：使用 `field_delimiter` 拼接行字段。

### field_delimiter [string]

`format = text` 时使用的字段分隔符。默认值为 `,`。

### common options

Sink 插件通用参数，请参考 [Sink 通用选项](../common-options/sink-common-options.md)。

## 交付语义

连接器使用 Google Pub/Sub Publisher 的批处理、流量控制和重试机制。在检查点和关闭期间，连接器会等待所有已接收的发布操作完成；异步发布失败会使任务失败。

从 SeaTunnel 作业角度看，Pub/Sub 发布语义为至少一次。任务重试可能再次发布消息，因此下游消费者应根据业务需要处理重复消息。

当前版本只发布序列化后的行负载，不支持消息属性、排序键和按行选择主题。

## 任务示例

### Application Default Credentials

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
        event_id = string
        event_type = string
      }
    }
  }
}

sink {
  GooglePubSub {
    project_id = "my-gcp-project"
    topic = "events"
    format = json
  }
}
```

### 服务账号密钥文件

```hocon
sink {
  GooglePubSub {
    project_id = "my-gcp-project"
    topic = "events"
    credentials_path = "/secrets/service-account.json"
    format = text
    field_delimiter = "|"
  }
}
```

### Pub/Sub 模拟器

```hocon
sink {
  GooglePubSub {
    project_id = "local-project"
    topic = "events"
    emulator_host = "pubsub-emulator:8085"
  }
}
```

## Changelog

<ChangeLog />
