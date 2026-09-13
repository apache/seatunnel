import ChangeLog from '../changelog/connector-azure-event-hubs.md';

# AzureEventHubs

> Azure Event Hubs 源连接器

## 描述

通过 Azure 原生 AMQP 客户端从一个 Azure Event Hub 读取事件，并将事件体转换为 SeaTunnel 行。

当作业需要 Event Hubs 分区发现和由 SeaTunnel 管理的序列号恢复时，使用此原生连接器。Azure Event Hubs 也提供 Kafka 兼容端点；如果现有部署已经统一使用 Kafka 协议配置和语义，可改用 SeaTunnel Kafka 连接器。

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [CDC](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持多表读取](../../introduction/concepts/connector-v2-features.md)

## 选项

| 名称 | 类型 | 是否必填 | 默认值 |
| --- | --- | --- | --- |
| connection_string | string | 是 | - |
| event_hub_name | string | 是 | - |
| consumer_group | string | 否 | $Default |
| start_mode | enum | 否 | earliest |
| format | enum | 否 | json |
| field_delimiter | string | 否 | , |
| max_batch_size | int | 否 | 100 |
| poll_timeout_ms | long | 否 | 1000 |
| prefetch_count | int | 否 | 300 |
| schema | config | 是 | - |
| common-options | | 否 | - |

### connection_string [string]

Azure Event Hubs 命名空间连接字符串。必须单独配置 `event_hub_name`；包含 `EntityPath` 段的连接字符串会被拒绝，从而保证只有一种明确的 Event Hub 选择方式。该选项会按敏感配置进行遮蔽，也不会写入连接器日志。

首个版本仅支持命名空间连接字符串认证，暂不支持 Microsoft Entra ID、托管身份和自定义端点认证。

### event_hub_name [string]

要消费的 Event Hub 名称。

### consumer_group [string]

源使用的消费者组。每个独立检查点作业应使用专用消费者组。

### start_mode [enum]

仅在作业没有恢复的源状态时使用的起始位置：

- `earliest`：从每个分区当前的起始序列号开始。
- `latest`：从每个分区最后入队序列号之后开始。

枚举器会将此模式一次性解析为每个分区的具体序列号。恢复的作业始终使用 SeaTunnel 检查点中保存的序列号，不会再次计算 `start_mode`。

### format [enum]

事件体格式：

- `json`：将事件体读取为 JSON 对象。
- `text`：使用 `field_delimiter` 拆分事件体字段。

### field_delimiter [string]

`format = text` 时使用的字段分隔符。

### max_batch_size [int]

每次从一个分区轮询的最大事件数，必须大于零。

### poll_timeout_ms [long]

每次分区轮询等待事件的最长时间，取值范围为 1 到 5000 毫秒。有限超时可使源关闭和分片变更及时中断空闲轮询。

### prefetch_count [int]

Azure SDK 为分配给源读取器的每个分区预取的最大事件数。该值必须大于零且不小于 `max_batch_size`。一个读取器可以负责多个分区，因此其客户端缓冲总量由该值乘以已分配的分区数进行限制。

### schema [config]

用于反序列化每个事件体的 schema。

### 通用选项

源插件通用参数请参考 [Source 通用选项](../common-options/source-common-options.md)。

## 分区与恢复语义

该源仅支持流处理。启动时，每个 Event Hubs 分区创建一个 SeaTunnel 源分片，并通过 SeaTunnel 常规的分片所有者计算进行分配。源并行度可并发处理不同分区；并行度高于分区数时，部分读取器会空闲。

首个版本仅在初始枚举时发现分区。作业启动后新增的分区不会动态发现，需要重启作业。

SeaTunnel 检查点状态是唯一的恢复依据。连接器不使用 Azure Blob Storage 检查点或 `EventProcessorClient`。分片检查点保存下一条待读取的序列号。已取入读取器队列但尚未发出的事件会在恢复后重放，已发出的事件会推进分片状态。启用检查点时提供至少一次投递语义。

连接器可以在未启用检查点时运行，但任务或作业重启时会重新应用 `start_mode`，因为不存在恢复状态。启用检查点时，下游处理应能容忍重复。

如果 Event Hubs 保留策略在恢复前删除了检查点对应的序列位置，源会失败，而不会静默重置为 `earliest` 或 `latest`。无效的 JSON 或文本负载也会使源任务失败；最后完成的检查点决定重放位置。

## 重试与失败行为

Azure SDK 会应用其内置的 AMQP 重试策略。如果 SDK 耗尽重试次数，源任务将失败，SeaTunnel 作业恢复会从最后完成的检查点继续读取每个分区。当前连接器版本不提供 Azure SDK 重试或退避配置。

## 作业示例

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  AzureEventHubs {
    connection_string = "Endpoint=sb://my-namespace.servicebus.windows.net/;SharedAccessKeyName=listen;SharedAccessKey=..."
    event_hub_name = "events"
    consumer_group = "$Default"
    start_mode = earliest
    format = json
    max_batch_size = 100
    poll_timeout_ms = 1000
    prefetch_count = 300
    schema = {
      fields {
        event_id = string
        event_type = string
      }
    }
  }
}
```

## 变更日志

<ChangeLog />
