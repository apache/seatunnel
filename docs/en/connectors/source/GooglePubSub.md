import ChangeLog from '../changelog/connector-google-pubsub.md';

# GooglePubSub

> Google Pub/Sub source connector

## Description

Reads messages from an existing Google Pub/Sub subscription and converts each message payload to a SeaTunnel row.

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Options

| name | type | required | default value |
| --- | --- | --- | --- |
| project_id | string | yes | - |
| subscription | string | yes | - |
| credentials_path | string | no | - |
| emulator_host | string | no | - |
| format | enum | no | json |
| field_delimiter | string | no | , |
| max_outstanding_messages | long | no | Google client default |
| max_outstanding_bytes | long | no | Google client default |
| parallel_pull_count | int | no | Google client default |
| schema | config | yes | - |
| common-options | | no | - |

### project_id [string]

Google Cloud project ID that owns the subscription.

### subscription [string]

Pub/Sub subscription ID. The subscription and its topic must exist before the job starts.

### credentials_path [string]

Path to a Google Cloud service account JSON key file. If this option is not set, the connector uses [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials).

### emulator_host [string]

Pub/Sub emulator host and port, for example `pubsub-emulator:8085`. When set, the connector uses a plaintext connection without credentials. Do not use this option for a production Pub/Sub endpoint.

### format [enum]

Message payload format. Supported values:

- `json`: converts a JSON object to a row using the configured schema.
- `text`: splits the payload into fields using `field_delimiter`.

### field_delimiter [string]

Field delimiter used when `format = text`. The default is `,`.

### max_outstanding_messages [long]

Maximum number of messages the subscriber can hold before applying flow control. The value must be greater than `0`. When omitted, the Google client default is used.

### max_outstanding_bytes [long]

Maximum total message bytes the subscriber can hold before applying flow control. The value must be greater than `0`. When omitted, the Google client default is used.

### parallel_pull_count [int]

Number of streaming pull connections opened by each source reader. The value must be greater than `0`. When omitted, the Google client default is used.

### schema [config]

Schema used to deserialize message payloads. See [Schema Feature](../../introduction/concepts/schema-feature.md) for details.

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

## Delivery Semantics

The connector uses one logical Pub/Sub subscription split. Messages are acknowledged only after the SeaTunnel checkpoint containing their rows completes. If the task fails before that checkpoint completes, Pub/Sub can redeliver the unacknowledged messages.

This provides at-least-once delivery. Consumers must tolerate duplicate rows after recovery. Periodic SeaTunnel checkpoints must be enabled so the connector can acknowledge processed messages. The source currently does not expose Pub/Sub message attributes, ordering keys, or publish timestamps as metadata fields.

If a message cannot be deserialized, the connector negatively acknowledges it and fails the source task. Pub/Sub can redeliver the same message after recovery, so a permanently invalid message can repeatedly restart the job. Configure a Pub/Sub dead-letter topic or remove the invalid message when this behavior is not acceptable.

## Task Example

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

### Service Account Key File

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

### Pub/Sub Emulator

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
