import ChangeLog from '../changelog/connector-azure-event-hubs.md';

# AzureEventHubs

> Azure Event Hubs source connector

## Description

Reads events from one Azure Event Hub through the native Azure AMQP client and converts each event body to a SeaTunnel row.

Use the native connector when the job needs Event Hubs partition discovery and SeaTunnel-managed sequence-number recovery. Azure Event Hubs also exposes a Kafka-compatible endpoint; use the SeaTunnel Kafka connector instead when an existing deployment already standardizes on Kafka protocol configuration and semantics.

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
| connection_string | string | yes | - |
| event_hub_name | string | yes | - |
| consumer_group | string | no | $Default |
| start_mode | enum | no | earliest |
| format | enum | no | json |
| field_delimiter | string | no | , |
| max_batch_size | int | no | 100 |
| poll_timeout_ms | long | no | 1000 |
| prefetch_count | int | no | 300 |
| schema | config | yes | - |
| common-options | | no | - |

### connection_string [string]

Azure Event Hubs namespace connection string. Configure `event_hub_name` separately; connection strings containing an `EntityPath` segment are rejected so there is one unambiguous event hub selection path. The option is masked as sensitive configuration and is not written to connector logs.

This first version supports namespace connection-string authentication. Microsoft Entra ID, managed identity and custom endpoint authentication are not yet supported.

### event_hub_name [string]

Name of the Event Hub to consume.

### consumer_group [string]

Consumer group used by the source. Each independently checkpointed job should use a dedicated consumer group.

### start_mode [enum]

Position used only when a job starts without restored source state:

- `earliest`: start at each partition's current beginning sequence number.
- `latest`: start immediately after each partition's last enqueued sequence number.

The enumerator resolves this mode once into a concrete sequence number per partition. A restored job always uses the sequence number stored in its SeaTunnel checkpoint and does not evaluate `start_mode` again.

### format [enum]

Event body format:

- `json`: reads the body as a JSON object.
- `text`: splits the body fields with `field_delimiter`.

### field_delimiter [string]

Field delimiter used when `format = text`.

### max_batch_size [int]

Maximum events requested from one partition in one poll. The value must be greater than zero.

### poll_timeout_ms [long]

Maximum time one partition poll waits for events. The value must be between 1 and 5000 milliseconds. A bounded timeout lets source shutdown and split changes interrupt idle polling promptly.

### prefetch_count [int]

Maximum events the Azure SDK prefetches for each partition assigned to a source reader. It must be greater than zero and at least `max_batch_size`. A reader can own multiple partitions, so its total client-side buffer is bounded by this value multiplied by its assigned partition count.

### schema [config]

Schema used to deserialize each event body.

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

## Partition And Recovery Semantics

The source is streaming-only. At startup, one SeaTunnel source split is created for each Event Hubs partition and assigned with the regular SeaTunnel split owner calculation. Source parallelism can process different partitions concurrently; parallelism greater than the partition count leaves some readers idle.

This first version discovers partitions only during the initial enumeration. Partitions added after the job starts require a job restart and are not picked up dynamically.

SeaTunnel checkpoint state is the only recovery authority. The connector does not use Azure Blob Storage checkpointing or `EventProcessorClient`. A split checkpoint stores the next sequence number to read. Events fetched into the reader queue but not emitted before a checkpoint are replayed after recovery, while emitted events advance the split state. This provides at-least-once delivery when checkpointing is enabled.

The connector can run without checkpointing, but a task or job restart then applies `start_mode` again because no recovery state exists. Downstream processing should tolerate duplicates when checkpointing is enabled.

If Event Hubs retention removes a checkpointed sequence before restore, the source fails instead of silently resetting to `earliest` or `latest`. Invalid JSON or text payloads also fail the source task; the last completed checkpoint determines the replay position.

## Retry And Failure Behavior

The Azure SDK applies its built-in AMQP retry policy. If the SDK exhausts those retries, the source task fails and SeaTunnel job recovery resumes each partition from its last completed checkpoint. This connector version does not expose Azure SDK retry or backoff settings.

## Task Example

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

## Changelog

<ChangeLog />
