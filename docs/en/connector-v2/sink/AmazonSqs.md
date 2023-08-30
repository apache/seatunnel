# AmazonSqs

> Amazon SQS sink connector

## Description

Write data to Amazon SQS

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## Options

| name              |  type  | required | default value |
|-------------------|--------|----------|---------------|
| url               | string | yes      | -             |
| region            | string | yes      | -             |
| queue             | string | no       | -             |
| common-options    |        | no       | -             |

### url [string]

The URL to write to Amazon SQS.

### region [string]

The region of Amazon SQS.

### queue [string]

The queue of Amazon SQS.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details.

## Example

```bash
amazonsqs {
    url = "http://127.0.0.1:8000"
    region = "us-east-1"
    queue = "queueName"
  }
```

## Changelog

### next version

- Add Amazon SQS Sink Connector

