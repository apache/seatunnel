import ChangeLog from '../changelog/connector-amazondynamodb.md';

# AmazonDynamoDB

> Amazon DynamoDB sink connector

## Description

Write data to Amazon DynamoDB

## Key Features

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## Options

|       Name        |  Type  | Required | Default value |
|-------------------|--------|----------|---------------|
| url               | string | yes      | -             |
| region            | string | yes      | -             |
| access_key_id     | string | yes      | -             |
| secret_access_key | string | yes      | -             |
| table             | string | yes      | -             |
| batch_size        | string | no       | 25            |
| common-options    |        | no       | -             |

### url [string]

The URL to write to Amazon DynamoDB.

### region [string]

The region of Amazon DynamoDB.

### access_key_id [string]

The access id of Amazon DynamoDB.

### secret_access_key [string]

The access secret of Amazon DynamoDB.

### table [string]

The table of Amazon DynamoDB.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details.

## Example

```bash
Amazondynamodb {
    url = "http://127.0.0.1:8000"
    region = "us-east-1"
    access_key_id = "dummy-key"
    secret_access_key = "dummy-secret"
    table = "TableName"
  }
```

## Changelog

<ChangeLog />


