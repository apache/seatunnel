# AmazonDynamoDB

> AmazonDynamoDB source connector

## Description

Read data from Amazon DynamoDB.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

|         name          |  type  | required | default value |
|-----------------------|--------|----------|---------------|
| url                   | string | yes      | -             |
| region                | string | yes      | -             |
| access_key_id         | string | yes      | -             |
| secret_access_key     | string | yes      | -             |
| table                 | string | yes      | -             |
| schema                | config | yes      | -             |
| common-options        |        | yes      | -             |
| scan_item_limit       |        | false    | -             |
| parallel_scan_threads |        | false    | -             |

### url [string]

The URL to read to Amazon Dynamodb.

### region [string]

The region of Amazon Dynamodb.

### accessKeyId [string]

The access id of Amazon DynamoDB.

### secretAccessKey [string]

The access secret of Amazon DynamoDB.

### table [string]

The table of Amazon DynamoDB.

### schema [Config]

#### fields [config]

Amazon Dynamodb is a NOSQL database service of support keys-value storage and document data structure,there is no way to get the data type.Therefore, we must configure schema.

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

Source Plugin common parameters, refer to [Source Plugin](../source-common-options.md) for details

### scan_item_limit

number of item each scan request should return

### parallel_scan_threads

number of logical segments for parallel scan

## Example

```bash
Amazondynamodb {
  url = "http://127.0.0.1:8000"
  region = "us-east-1"
  accessKeyId = "dummy-key"
  secretAccessKey = "dummy-secret"
  table = "TableName"
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

- Add Amazon DynamoDB Source Connector
- Add source  split to Amazondynamodb Connectors

