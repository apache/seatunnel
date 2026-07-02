import ChangeLog from '../changelog/connector-amazondynamodb.md';

# AmazonDynamoDB

> Amazon DynamoDB sink connector

## Description

Write data to Amazon DynamoDB

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Options

|       Name        |  Type  | Required | Default value |
|-------------------|--------|----------|---------------|
| url               | string | yes      | -             |
| region            | string | yes      | -             |
| access_key_id     | string | yes      | -             |
| secret_access_key | string | yes      | -             |
| table             | string | yes      | -             |
| batch_size          | int    | no       | 25            |
| max_retries         | int    | no       | 10            |
| retry_base_delay_ms | long   | no       | 100           |
| retry_max_delay_ms  | long   | no       | 5000          |
| common-options      |        | no       | -             |

### url [string]

The URL to write to Amazon DynamoDB.

### region [string]

The region of Amazon DynamoDB.

### access_key_id [string]

The access id of Amazon DynamoDB.

### secret_access_key [string]

The access secret of Amazon DynamoDB.

### table [string]

The table of Amazon DynamoDB. Supports `${table_name}` placeholder for multi-table sink scenarios.

### batch_size [int]

The number of records to batch before writing to Amazon DynamoDB.

### max_retries [int]

Maximum number of retries when DynamoDB returns unprocessed items in a batch write.

### retry_base_delay_ms [long]

Base delay in milliseconds for exponential backoff between retries.

### retry_max_delay_ms [long]

Maximum delay in milliseconds between retries regardless of retry count.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

## Example

### Single table
```bash
AmazonDynamoDB {
    url = "http://127.0.0.1:8000"
    region = "us-east-1"
    access_key_id = "dummy-key"
    secret_access_key = "dummy-secret"
    table = "TableName"
}
```

### Multiple table
```bash
AmazonDynamoDB {
    url = "http://127.0.0.1:8000"
    region = "us-east-1"
    access_key_id = "dummy-key"
    secret_access_key = "dummy-secret"
    table = "${table_name}"
}
```

## Changelog

<ChangeLog />


