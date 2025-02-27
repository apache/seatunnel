# AmazonDynamoDB

> Amazon DynamoDB sink connector

## Description

Write data to Amazon DynamoDB

## Key Features

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## Options

|       Name        |  Type  | Required | Default value |
|-------------------|--------|----------|---------------|
| url               | string | no       | -             |
| region            | string | no       | -             |
| access_key_id     | string | no       | -             |
| secret_access_key | string | no       | -             |
| table             | string | yes      | -             |
| batch_size        | string | no       | 25            |
| common-options    |        | no       | -             |

### url [string]

The URL to write to Amazon DynamoDB. It will override the `endpoint` of AWS DynamoDB.

### region [string]

The region of Amazon DynamoDB. The DynamoDB client will use the region to determine the service endpoint.

### access_key_id [string]

The access id of Amazon DynamoDB. If you don't set it, the plugin will use the container credential provider chain to get the access id.

### secret_access_key [string]

The access secret of Amazon DynamoDB. If you don't set it, the plugin will use the container credential provider chain to get the access secret.

### table [string]

The table of Amazon DynamoDB.

### batch_size [string]

The number of records to write to Amazon DynamoDB in a batch. The default value is 25, and the maximum value is 25 because the Amazon DynamoDB batch write item limit is 25.

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

### next version

- Add Amazon DynamoDB Sink Connector

