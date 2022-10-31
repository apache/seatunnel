````
# Amazondynamodb

> Amazondynamodb sink connector

## Description

Write data to `Amazondynamodb`

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name             | type   | required | default value |
|----------------- | ------ |----------| ------------- |
| url              | string | yes      | -             |
| region           | string | yes      | -             |
| access_key_id    | string | yes      | -             |
| secret_access_key| string | yes      | -             |
| table            | string | yes      | -             |
| batch_size       | string | no       | 25            |
| batch_interval_ms| string | no       | 1000          |
| common-options   |        | no       | -             |

### url [string]

url to write to Amazondynamodb.

### region [string]

The region of Amazondynamodb.

### accessKeyId [string]

The access id of Amazondynamodb.

### secretAccessKey [string]

The access secret of Amazondynamodb.

### table [string]

The table of Amazondynamodb.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details.

## Example

```bash
Amazondynamodb {
    url = "http://127.0.0.1:8000"
    region = "us-east-1"
    accessKeyId = "dummy-key"
    secretAccessKey = "dummy-secret"
    table = "TableName"
  }
```

## Changelog

### next version

- Add Amazondynamodb Sink Connector

````
