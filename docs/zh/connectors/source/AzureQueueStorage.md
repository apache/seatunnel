import ChangeLog from '../changelog/connector-azure-queue-storage.md';

# AzureQueueStorage

> Azure Queue Storage source connector

## 描述

从一个 Azure Storage 队列读取消息，并将每个消息负载转换为 SeaTunnel 行。

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
| queue_name | string | 是 | - |
| authentication_type | enum | 是 | - |
| connection_string | string | 条件必填 | - |
| endpoint | string | 条件必填 | - |
| account_name | string | 条件必填 | - |
| account_key | string | 条件必填 | - |
| sas_token | string | 条件必填 | - |
| format | enum | 否 | json |
| field_delimiter | string | 否 | , |
| message_encoding | enum | 否 | none |
| batch_size | int | 否 | 32 |
| visibility_timeout_seconds | int | 否 | 300 |
| poll_interval_ms | long | 否 | 1000 |
| max_in_flight_messages | int | 否 | 1000 |
| operation_timeout_ms | long | 否 | 60000 |
| schema | config | 是 | - |
| common-options | | 否 | - |

### queue_name [string]

源 Azure Storage 队列。作业启动前队列必须已经存在。队列名称必须包含 3-63 个小写字母、数字或单个连字符。

### authentication_type [enum]

选择一种认证方式：

- `connection_string`：需要 `connection_string`。
- `shared_key`：需要 `endpoint`、`account_name` 和 `account_key`。
- `sas_token`：需要 `endpoint` 和 `sas_token`。

不能混用不同认证方式的凭据。连接器不会记录凭据值。

### connection_string [string]

Azure Storage 连接字符串。该方式也支持包含自定义 `QueueEndpoint` 的 Azurite 连接字符串。

### endpoint [string]

Azure Queue 服务端点，例如 `https://myaccount.queue.core.windows.net`。

### account_name [string]

共享密钥认证使用的 Azure Storage 账户名。

### account_key [string]

共享密钥认证使用的 Azure Storage 账户密钥。

### sas_token [string]

Azure Storage SAS 令牌。创建客户端前会移除开头的 `?`。

### format [enum]

消息负载格式：

- `json`：将负载读取为 JSON 对象。
- `text`：使用 `field_delimiter` 拆分负载字段。

### field_delimiter [string]

`format = text` 时使用的字段分隔符。

### message_encoding [enum]

控制 Azure SDK 的消息解码：

- `none`：直接读取 UTF-8 负载。
- `base64`：反序列化前对队列消息执行 Base64 解码。

该值应与消息生产者发布时使用的编码一致。

### batch_size [int]

每次 Azure Queue 接收请求的最大消息数。Azure 接受 1 到 32 的值。

### visibility_timeout_seconds [int]

已接收消息对其他消费者保持隐藏的时间。消息等待检查点完成时，连接器会续期该可见性期限。Azure 接受 1 到 604800 秒的值。

### poll_interval_ms [long]

队列为空或达到 `max_in_flight_messages` 后再次轮询的等待时间。

### max_in_flight_messages [int]

源在检查点完成前保留的最大消息数。该值必须不小于 `batch_size`，并在检查点较慢时限制源的内存使用。

### operation_timeout_ms [long]

每次接收、可见性更新或删除请求的最长时间。该值必须小于 `visibility_timeout_seconds` 的一半，以便续期请求能在当前租约到期前完成。

### schema [config]

用于反序列化每个消息负载的 schema。

### 通用选项

源插件通用参数请参考 [Source 通用选项](../common-options/source-common-options.md)。

## 投递语义

源支持启用检查点的流作业。只有包含该行的检查点完成后，源才删除对应消息。中止的检查点会让消息保留到后续检查点，可见性续期可防止等待期间消息再次提供给其他消费者。

投递语义为至少一次。任务失败、可见性租约丢失或部分删除失败可能使 Azure Queue Storage 再次投递消息，因此下游处理应能容忍重复。无法反序列化的负载会被重新释放到队列，并使源任务失败。

一个队列对应一个 SeaTunnel 源分片。提高作业并行度不会为此源创建更多 Azure Queue 消费者。

## 作业示例

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  AzureQueueStorage {
    queue_name = "events"
    authentication_type = connection_string
    connection_string = "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=...;EndpointSuffix=core.windows.net"
    format = json
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
