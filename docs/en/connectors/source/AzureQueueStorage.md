import ChangeLog from '../changelog/connector-azure-queue-storage.md';

# AzureQueueStorage

> Azure Queue Storage source connector

## Description

Reads messages from one Azure Storage queue and converts each payload to a SeaTunnel row.

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [support multiple table read](../../introduction/concepts/connector-v2-features.md)

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
| batch_size | int | no | 32 |
| visibility_timeout_seconds | int | no | 300 |
| poll_interval_ms | long | no | 1000 |
| max_in_flight_messages | int | no | 1000 |
| operation_timeout_ms | long | no | 60000 |
| schema | config | yes | - |
| common-options | | no | - |

### queue_name [string]

Source Azure Storage queue. The queue must exist before the job starts. Queue names must contain 3-63 lowercase letters, numbers or single hyphens.

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

- `json`: reads the payload as a JSON object.
- `text`: splits the payload fields with `field_delimiter`.

### field_delimiter [string]

Field delimiter used when `format = text`.

### message_encoding [enum]

Controls Azure SDK message decoding:

- `none`: reads the UTF-8 payload verbatim.
- `base64`: Base64-decodes the queue message before deserialization.

Use the same value that the queue producer uses when publishing messages.

### batch_size [int]

Maximum messages requested in one Azure Queue receive call. Azure accepts values from 1 to 32.

### visibility_timeout_seconds [int]

How long a received message is hidden from other consumers. The connector renews this visibility period while the message waits for a completed checkpoint. Azure accepts values from 1 to 604800 seconds.

### poll_interval_ms [long]

Delay before polling again when the queue is empty or `max_in_flight_messages` has been reached.

### max_in_flight_messages [int]

Maximum messages retained by the source until checkpoint completion. It must be at least `batch_size` and bounds source memory usage when checkpoints are slow.

### operation_timeout_ms [long]

Maximum duration of each receive, visibility update or delete request. It must be less than half of `visibility_timeout_seconds` so a renewal request has time to finish before the current lease expires.

### schema [config]

Schema used to deserialize each message payload.

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

## Delivery Semantics

The source supports streaming jobs with checkpointing. It does not delete a received message until a checkpoint containing that row completes. An aborted checkpoint keeps the message pending for a later checkpoint, and visibility renewal prevents it from becoming available to another consumer while it is pending.

Delivery is at-least-once. A task failure, visibility lease loss or partial delete failure can cause Azure Queue Storage to deliver a message again, so downstream processing should tolerate duplicates. A payload that cannot be deserialized is released back to the queue and fails the source task.

One queue is represented by one SeaTunnel source split. Increasing job parallelism does not create additional Azure Queue consumers for this source.

## Task Example

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

## Changelog

<ChangeLog />
