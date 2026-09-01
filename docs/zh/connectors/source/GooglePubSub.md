import ChangeLog from '../changelog/connector-google-pubsub.md';

# GooglePubSub

> Google Pub/Sub Source 连接器

## 描述

从已有的 Google Pub/Sub 订阅读取消息，并将每条消息负载转换为 SeaTunnel 行。

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义 Split](../../introduction/concepts/connector-v2-features.md)

## 参数

| 参数名 | 类型 | 是否必填 | 默认值 |
| --- | --- | --- | --- |
| project_id | string | 是 | - |
| subscription | string | 是 | - |
| credentials_path | string | 否 | - |
| emulator_host | string | 否 | - |
| format | enum | 否 | json |
| field_delimiter | string | 否 | , |
| max_outstanding_messages | long | 否 | Google 客户端默认值 |
| max_outstanding_bytes | long | 否 | Google 客户端默认值 |
| parallel_pull_count | int | 否 | Google 客户端默认值 |
| schema | config | 是 | - |
| common-options | | 否 | - |

### project_id [string]

订阅所属的 Google Cloud 项目 ID。

### subscription [string]

Pub/Sub 订阅 ID。启动作业前必须先创建该订阅及其关联主题。

### credentials_path [string]

Google Cloud 服务账号 JSON 密钥文件的路径。未配置时，连接器使用 [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)。

### emulator_host [string]

Pub/Sub 模拟器的主机和端口，例如 `pubsub-emulator:8085`。配置后，连接器使用无凭证的明文连接。生产环境中不要使用该选项。

### format [enum]

消息负载格式。支持以下值：

- `json`：按照配置的 Schema 将 JSON 对象转换为行。
- `text`：使用 `field_delimiter` 将负载拆分为字段。

### field_delimiter [string]

`format = text` 时使用的字段分隔符。默认值为 `,`。

### max_outstanding_messages [long]

订阅客户端在触发流量控制前最多保留的消息数。该值必须大于 `0`。未配置时使用 Google 客户端默认值。

### max_outstanding_bytes [long]

订阅客户端在触发流量控制前最多保留的消息总字节数。该值必须大于 `0`。未配置时使用 Google 客户端默认值。

### parallel_pull_count [int]

每个 Source Reader 建立的流式拉取连接数。该值必须大于 `0`。未配置时使用 Google 客户端默认值。

### schema [config]

反序列化消息负载使用的 Schema。详情请参阅 [Schema 特性](../../introduction/concepts/schema-feature.md)。

### common options

Source 插件通用参数，请参考 [Source 通用选项](../common-options/source-common-options.md)。

## 交付语义

连接器使用一个逻辑 Pub/Sub 订阅 Split。只有当包含消息对应行的 SeaTunnel 检查点完成后，连接器才确认这些消息。如果任务在检查点完成前失败，Pub/Sub 可以重新投递未确认的消息。

该机制提供至少一次交付语义。恢复后可能出现重复行，使用方需要具备去重能力。必须启用周期性 SeaTunnel 检查点，连接器才能确认已处理的消息。当前版本不将 Pub/Sub 消息属性、排序键或发布时间公开为元数据字段。

如果消息无法反序列化，连接器会对该消息进行否定确认并使 Source 任务失败。Pub/Sub 可以在任务恢复后再次投递同一条消息，因此永久无效的消息可能导致作业反复重启。如不能接受该行为，请配置 Pub/Sub 死信主题或移除无效消息。

## 任务示例

### Application Default Credentials

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  GooglePubSub {
    project_id = "my-gcp-project"
    subscription = "events-subscription"
    format = json
    schema = {
      fields {
        event_id = string
        event_type = string
      }
    }
  }
}

sink {
  Console {}
}
```

### 服务账号密钥文件

```hocon
source {
  GooglePubSub {
    project_id = "my-gcp-project"
    subscription = "events-subscription"
    credentials_path = "/secrets/service-account.json"
    format = text
    field_delimiter = "|"
    schema = {
      fields {
        event_id = string
        event_type = string
      }
    }
  }
}
```

### Pub/Sub 模拟器

```hocon
source {
  GooglePubSub {
    project_id = "local-project"
    subscription = "events-subscription"
    emulator_host = "pubsub-emulator:8085"
    schema = {
      fields {
        event_id = string
      }
    }
  }
}
```

## Changelog

<ChangeLog />
