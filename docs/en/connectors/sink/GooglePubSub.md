import ChangeLog from '../changelog/connector-google-pubsub.md';

# GooglePubSub

> Google Pub/Sub sink connector

## Description

Publishes each incoming SeaTunnel row as one message to a Google Pub/Sub topic.

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
| project_id | string | yes | - |
| topic | string | yes | - |
| credentials_path | string | no | - |
| emulator_host | string | no | - |
| format | enum | no | json |
| field_delimiter | string | no | , |
| common-options | | no | - |

### project_id [string]

Google Cloud project ID that owns the target topic.

### topic [string]

Target Pub/Sub topic ID. The topic must exist before the job starts.

### credentials_path [string]

Path to a Google Cloud service account JSON key file. If this option is not set, the connector uses [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials).

### emulator_host [string]

Pub/Sub emulator host and port, for example `pubsub-emulator:8085`. When set, the connector uses a plaintext connection without credentials. Do not use this option for a production Pub/Sub endpoint.

### format [enum]

Message payload format. Supported values:

- `json`: writes the row as a JSON object.
- `text`: joins row fields with `field_delimiter`.

### field_delimiter [string]

Field delimiter used when `format = text`. The default is `,`.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

## Delivery Semantics

The connector uses the Google Pub/Sub publisher's batching, flow-control, and retry behavior. It waits for all accepted publish operations during checkpoints and shutdown and fails the task when an asynchronous publish fails.

Pub/Sub publishing is at-least-once from the SeaTunnel job's perspective. A task retry can publish a message again, so downstream consumers should tolerate duplicates when required by the use case.

This first sink implementation publishes only the serialized row payload. Message attributes, ordering keys, and per-row topic routing are not supported.

## Task Example

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

### Service Account Key File

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

### Pub/Sub Emulator

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
