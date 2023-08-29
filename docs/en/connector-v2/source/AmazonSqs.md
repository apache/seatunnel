# AmazonSqs

> AmazonSqs source connector

## Description

Read and write data from/to Amazon DynamoDB.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name              |  type  | required | default value |
|-------------------|--------|----------|---------------|
| url               | string | yes      | -             |
| region            | string | yes      | -             |
| queue             | string | false    | -             |
| schema            | config | yes      | -             |
| common-options    |        | yes      | -             |

### url [string]

The URL to read from Amazon SQS.


### queue [string]

the queue name to read from Amazon SQS

### region [string]

The region of Amazon SQS.

### schema [Config]

#### fields [config]

Amazon SQS is a managed message queuing service
such as:

```
schema {
  fields {
    id = int
    key_aa = string
    key_bb = string
  }
}
```

### common options

Source Plugin common parameters, refer to [Source Plugin](common-options.md) for details

## Example

```bash
AmazonSqs {
  url = "http://127.0.0.1:8000"
  region = "us-east-1"
  queue = "QueueName"
  schema = {
    fields {
      artist = string
      c_map = "map<string, array<int>>"
      c_array = "array<int>"
      c_string = string
      c_boolean = boolean
      c_tinyint = tinyint
      c_smallint = smallint
      c_int = int
      c_bigint = bigint
      c_float = float
      c_double = double
      c_decimal = "decimal(30, 8)"
      c_null = "null"
      c_bytes = bytes
      c_date = date
      c_timestamp = timestamp
    }
  }
}
```

## Changelog

### next version

- Add Amazon SQS Source Connector

