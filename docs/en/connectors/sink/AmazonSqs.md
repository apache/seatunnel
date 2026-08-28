import ChangeLog from '../changelog/connector-amazonsqs.md';

# AmazonSqs

> Amazon SQS sink connector

## Description

The Amazon SQS sink connector writes each incoming SeaTunnel row to one Amazon SQS queue URL. The row
is serialized by the configured `format`, and the serialized value is sent as the SQS message body.

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

## Sink Options

| Name              | Type   | Required | Default | Description                                                                                                                                                 |
|-------------------|--------|----------|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url               | String | Yes      | -       | Full SQS queue URL to write to, for example `https://sqs.us-east-1.amazonaws.com/123456789012/sink_queue`.                                                   |
| region            | String | Yes      | -       | AWS region of the SQS queue, for example `us-east-1`.                                                                                                       |
| access_key_id     | String | No       | -       | AWS access key ID. Set it together with `secret_access_key` to use static credentials. Leave both unset to use the AWS default credential provider chain.     |
| secret_access_key | String | No       | -       | AWS secret access key. Set it together with `access_key_id` to use static credentials.                                                                       |
| format            | String | No       | json    | Message body format. Supported values are `json`, `text`, `canal_json`, and `debezium_json`.                                                                |
| field_delimiter   | String | No       | ,       | Field delimiter used when `format = text`.                                                                                                                  |
| common-options    |        | No       | -       | Sink plugin common parameters. For details, see [Sink Common Options](../common-options/sink-common-options.md).                                            |

`url` can point to AWS SQS or to an SQS-compatible local service, for example `http://sqs-host:4566/000000000000/sink_queue`.

## Format Notes

- `json` writes each row as a JSON object.
- `text` joins row fields by `field_delimiter`.
- `canal_json` writes Canal JSON messages. For details, see [Canal JSON](../formats/canal-json.md).
- `debezium_json` writes Debezium JSON messages. For details, see [Debezium JSON](../formats/debezium-json.md).
- The sink sends only the message body. It does not expose SQS message attributes, delay seconds, deduplication ID, or message group ID options.
- `access_key_id` and `secret_access_key` are optional, but they must be configured together when static AWS credentials are used.
- The sink sends each SeaTunnel row as one SQS message. It does not batch multiple rows into one SQS request.

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

### Write JSON Messages

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 1
    schema = {
      fields {
        name = string
      }
    }
    rows = [
      {
        kind = INSERT
        fields = ["test_name"]
      }
    ]
  }
}

sink {
  AmazonSqs {
    url = "https://sqs.us-east-1.amazonaws.com/123456789012/sink_queue"
    region = "us-east-1"
    access_key_id = "AKIA..."
    secret_access_key = "SECRET..."
  }
}
```

### Write Text Messages With a Custom Delimiter

```hocon
source {
  FakeSource {
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
  AmazonSqs {
    url = "https://sqs.us-east-1.amazonaws.com/123456789012/sink_queue"
    region = "us-east-1"
    format = text
    field_delimiter = "|"
  }
}
```

### Write Canal JSON Messages

Set `format = canal_json` so each SeaTunnel row is serialized as a Canal JSON
change event. Useful when the downstream consumer is a Canal JSON consumer (such
as a Canal → Kafka bridge or a Canal-compatible BigQuery loader).

```hocon
sink {
  AmazonSqs {
    url = "https://sqs.us-east-1.amazonaws.com/123456789012/sink_queue"
    region = "us-east-1"
    format = canal_json
  }
}
```

## Changelog

<ChangeLog />
