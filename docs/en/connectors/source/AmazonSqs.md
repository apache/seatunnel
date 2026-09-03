import ChangeLog from '../changelog/connector-amazonsqs.md';

# AmazonSqs

> Amazon SQS source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

The Amazon SQS source connector reads messages from one Amazon SQS queue URL. Each message body is
deserialized by the configured `format` and `schema`, then emitted as SeaTunnel rows.

The connector uses a single reader and finishes the job after the current receive request is
processed. It is suitable for bounded reads from a queue.

Each receive request asks SQS for up to 10 messages. If more messages are waiting in the queue, run another job or use a streaming-style upstream design that repeatedly starts bounded reads.

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Source Options

| Name                           | Type    | Required | Default | Description                                                                                                                                                 |
|--------------------------------|---------|----------|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                            | String  | Yes      | -       | Full SQS queue URL to read from, for example `https://sqs.us-east-1.amazonaws.com/123456789012/source_queue`.                                                |
| region                         | String  | Yes      | -       | AWS region of the SQS queue, for example `us-east-1`.                                                                                                       |
| schema                         | Config  | Yes      | -       | Message body schema. For more details, see [Schema Feature](../../introduction/concepts/schema-feature.md).                                                  |
| access_key_id                  | String  | No       | -       | AWS access key ID. Set it together with `secret_access_key` to use static credentials. Leave both unset to use the AWS default credential provider chain.     |
| secret_access_key              | String  | No       | -       | AWS secret access key. Set it together with `access_key_id` to use static credentials.                                                                       |
| format                         | String  | No       | json    | Message body format. Supported values are `json`, `text`, `canal_json`, and `debezium_json`.                                                                |
| field_delimiter                | String  | No       | ,       | Field delimiter used when `format = text`.                                                                                                                  |
| ignore_parse_errors            | Boolean | No       | false   | Whether to skip messages that cannot be deserialized instead of failing the poll.                                                                            |
| delete_message                 | Boolean | No       | false   | Whether to delete a message from the queue after it is read and deserialized successfully.                                                                   |
| message_group_id               | String  | No       | -       | Message group ID option kept for compatibility. It is not required for normal SQS reads.                                                                     |
| debezium_record_include_schema | Boolean | No       | true    | Whether Debezium JSON messages include a schema. This option is used only when `format = debezium_json`.                                                     |
| common-options                 |         | No       | -       | Source plugin common parameters. For details, see [Source Common Options](../common-options/source-common-options.md).                                      |

`url` can point to AWS SQS or to an SQS-compatible local service, for example `http://sqs-host:4566/000000000000/source_queue`.

## Format Notes

- `json` reads each message body as a JSON object that matches `schema`.
- `text` splits each message body by `field_delimiter` and maps the values to fields in `schema` order.
- `canal_json` reads Canal JSON messages. For details, see [Canal JSON](../formats/canal-json.md).
- `debezium_json` reads Debezium JSON messages. For details, see [Debezium JSON](../formats/debezium-json.md).
- `ignore_parse_errors = false` fails the poll and retains an unreadable message. When set to `true`, the source skips the message and continues processing the batch.
- When both `ignore_parse_errors` and `delete_message` are `true`, skipped messages are deleted from SQS. Keep `delete_message = false` if skipped messages should remain available for redelivery.
- `delete_message = true` removes consumed messages from SQS. Keep the default `false` when you only want to inspect or copy messages without deleting them.
- `access_key_id` and `secret_access_key` are optional, but they must be configured together when static AWS credentials are used.
- The source performs one receive request, with up to 10 messages, and then finishes the bounded job.

## Authentication

The connector resolves AWS credentials in the following order:

1. `access_key_id` and `secret_access_key` if both are configured.
2. Otherwise, the AWS default credential provider chain (environment variables, instance profile, etc.).

For local testing against an SQS-compatible service such as LocalStack or ElasticMQ,
point `url` at the local endpoint (for example `http://sqs-host:4566/...`) and provide any
non-empty `access_key_id` / `secret_access_key`. SQS-compatible test services typically
do not validate the SigV4 signature on incoming requests, so any static credential pair is
accepted by them.

## Task Examples

### Copy Messages Between Local-Compatible Queues

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  AmazonSqs {
    url = "http://sqs-host:4566/000000000000/source_queue"
    access_key_id = "1234"
    secret_access_key = "abcd"
    region = "us-east-1"
    schema = {
      fields {
        name = "string"
      }
    }
  }
}

sink {
  AmazonSqs {
    url = "http://sqs-host:4566/000000000000/sink_queue"
    access_key_id = "1234"
    secret_access_key = "abcd"
    region = "us-east-1"
  }
}
```

### Read JSON Messages

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  AmazonSqs {
    url = "https://sqs.us-east-1.amazonaws.com/123456789012/source_queue"
    region = "us-east-1"
    access_key_id = "AKIA..."
    secret_access_key = "SECRET..."
    schema = {
      fields {
        name = string
      }
    }
  }
}

sink {
  Console {}
}
```

### Read Text Messages With a Custom Delimiter

```hocon
source {
  AmazonSqs {
    url = "https://sqs.us-east-1.amazonaws.com/123456789012/source_queue"
    region = "us-east-1"
    format = text
    field_delimiter = "#"
    delete_message = true
    schema = {
      fields {
        artist = string
        album = string
        release_year = int
      }
    }
  }
}

sink {
  Console {}
}
```

### Read Debezium JSON Messages

When the upstream system (such as Debezium or a CDC source) publishes change
events as Debezium envelopes, set `format = debezium_json` and use
`debezium_record_include_schema` to control whether the schema field is expected.

```hocon
source {
  AmazonSqs {
    url = "https://sqs.us-east-1.amazonaws.com/123456789012/cdc_events"
    region = "us-east-1"
    format = debezium_json
    debezium_record_include_schema = true
    schema = {
      fields {
        id = bigint
        name = string
        score = double
      }
    }
  }
}
```

## Changelog

<ChangeLog />
