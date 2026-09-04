import ChangeLog from '../changelog/connector-azure-queue-storage.md';

# AzureQueueStorage

> Azure Queue Storage sink connector

## Description

Publishes each incoming SeaTunnel row as one message to an Azure Storage queue.

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Options

| name | type | required | default value |
| --- | --- | --- | --- |
| queue_name | string | yes | - |
| authentication_type | enum | yes | - |
| connection_string | string | conditional | - |
| endpoint | string | conditional | - |
| account_name | string | conditional | - |
| account_key | string | conditional | - |
| sas_token | string | conditional | - |
| format | enum | no | json |
| field_delimiter | string | no | , |
| message_encoding | enum | no | none |
| max_in_flight | int | no | 100 |
| operation_timeout_ms | long | no | 60000 |
| common-options | | no | - |

### queue_name [string]

Target Azure Storage queue. The queue must exist before the job starts. Queue names must contain 3-63 lowercase letters, numbers or single hyphens.

### authentication_type [enum]

Selects one explicit authentication path:

- `connection_string`: requires `connection_string`.
- `shared_key`: requires `endpoint`, `account_name` and `account_key`.
- `sas_token`: requires `endpoint` and `sas_token`.

Credentials from different authentication modes cannot be mixed. The connector does not log credential values.

### connection_string [string]

Azure Storage connection string. This mode also supports Azurite connection strings with a custom `QueueEndpoint`.

### endpoint [string]

Azure Queue service endpoint, for example `https://myaccount.queue.core.windows.net`.

### account_name [string]

Azure Storage account name used by shared-key authentication.

### account_key [string]

Azure Storage account key used by shared-key authentication.

### sas_token [string]

Azure Storage SAS token. A leading `?` is accepted and removed before the client is created.

### format [enum]

Message payload format:

- `json`: writes the row as a JSON object.
- `text`: joins row fields with `field_delimiter`.

### field_delimiter [string]

Field delimiter used when `format = text`.

### message_encoding [enum]

Controls the Azure SDK message encoding:

- `none`: sends the UTF-8 payload verbatim.
- `base64`: Base64-encodes the UTF-8 payload before sending.

Azure Queue Storage limits each encoded message to 64 KiB. The connector validates the size before sending. With `base64`, the raw serialized payload can be at most 48 KiB because Base64 expands the message.

### max_in_flight [int]

Maximum number of asynchronous sends accepted by each sink task before applying backpressure.

### operation_timeout_ms [long]

Maximum time to wait for an available send slot or for outstanding sends during checkpoint and shutdown.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

## Delivery Semantics

The connector waits for all accepted sends during checkpoints and shutdown and reports asynchronous failures to the task. The number of outstanding sends is bounded by `max_in_flight`.

Azure Queue publishing is at-least-once from the SeaTunnel job's perspective. A client retry or task recovery can publish a message again, so consumers should tolerate duplicates. The connector does not create queues, route rows to different queues, or provide exactly-once delivery.

## Task Example

### Connection String

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

### Shared Key

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

## Changelog

<ChangeLog />
