import ChangeLog from '../changelog/connector-azure-queue-storage.md';

# AzureQueueStorage

> Azure Queue Storage Sink 连接器

## 描述

将每一条 SeaTunnel Row 作为一条消息发送到 Azure Storage Queue。

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [CDC](../../introduction/concepts/connector-v2-features.md)
- [ ] [多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 配置项

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
| max_in_flight | int | 否 | 100 |
| operation_timeout_ms | long | 否 | 60000 |
| common-options | | 否 | - |

### queue_name [string]

目标 Azure Storage Queue。任务启动前队列必须已经存在。队列名称长度为 3-63，只能包含小写字母、数字或单个连字符。

### authentication_type [enum]

选择一种明确的认证方式：

- `connection_string`：需要配置 `connection_string`。
- `shared_key`：需要配置 `endpoint`、`account_name` 和 `account_key`。
- `sas_token`：需要配置 `endpoint` 和 `sas_token`。

不同认证方式的凭证不能混用，连接器不会在日志中输出凭证值。

### connection_string [string]

Azure Storage 连接字符串。该方式也支持包含自定义 `QueueEndpoint` 的 Azurite 连接字符串。

### endpoint [string]

Azure Queue 服务地址，例如 `https://myaccount.queue.core.windows.net`。

### account_name [string]

共享密钥认证使用的 Azure Storage 账户名。

### account_key [string]

共享密钥认证使用的 Azure Storage 账户密钥。

### sas_token [string]

Azure Storage SAS Token。允许以 `?` 开头，创建客户端前会移除该字符。

### format [enum]

消息格式：

- `json`：将 Row 写为 JSON 对象。
- `text`：使用 `field_delimiter` 连接 Row 字段。

### field_delimiter [string]

`format = text` 时使用的字段分隔符。

### message_encoding [enum]

控制 Azure SDK 的消息编码：

- `none`：直接发送 UTF-8 内容。
- `base64`：发送前对 UTF-8 内容进行 Base64 编码。

Azure Queue Storage 对编码后的单条消息限制为 64 KiB，连接器会在发送前校验。使用 `base64` 时，由于编码会扩大消息，原始序列化内容最大为 48 KiB。

### max_in_flight [int]

每个 Sink Task 允许的最大异步发送数量，达到上限后会施加背压。

### operation_timeout_ms [long]

等待发送槽位，以及在 Checkpoint 或关闭时等待未完成发送的最大时间。

### common options

Sink 插件通用参数请参考 [Sink Common Options](../common-options/sink-common-options.md)。

## 交付语义

连接器会在 Checkpoint 和关闭时等待所有已接受的发送，并将异步失败报告给任务。未完成发送数量由 `max_in_flight` 限制。

从 SeaTunnel 任务角度看，该连接器提供至少一次语义。客户端重试或任务恢复可能重复发送消息，下游消费者应能处理重复消息。连接器不会创建队列、按 Row 路由到不同队列，也不提供精确一次语义。

## 任务示例

### 连接字符串

```hocon
sink {
  AzureQueueStorage {
    queue_name = "events"
    authentication_type = connection_string
    connection_string = "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=...;EndpointSuffix=core.windows.net"
    format = json
  }
}
```

### 共享密钥

```hocon
sink {
  AzureQueueStorage {
    queue_name = "events"
    authentication_type = shared_key
    endpoint = "https://myaccount.queue.core.windows.net"
    account_name = "myaccount"
    account_key = "..."
    format = text
    field_delimiter = "|"
  }
}
```

### SAS Token

```hocon
sink {
  AzureQueueStorage {
    queue_name = "events"
    authentication_type = sas_token
    endpoint = "https://myaccount.queue.core.windows.net"
    sas_token = "sv=...&sig=..."
    message_encoding = base64
  }
}
```

## 变更日志

<ChangeLog />
