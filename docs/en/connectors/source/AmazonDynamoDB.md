import ChangeLog from '../changelog/connector-amazondynamodb.md';

# AmazonDynamoDB

> Amazon DynamoDB source connector

## Description

The Amazon DynamoDB source connector reads existing items from an Amazon DynamoDB table by using DynamoDB scan requests.

The connector is a batch source. DynamoDB does not expose field types in the same way as a relational database, so the SeaTunnel schema must be configured explicitly.

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Options

| name                  | type   | required | default value |
|-----------------------|--------|----------|---------------|
| url                   | string | yes      | -             |
| region                | string | yes      | -             |
| access_key_id         | string | yes      | -             |
| secret_access_key     | string | yes      | -             |
| table                 | string | yes      | -             |
| schema                | config | yes      | -             |
| scan_item_limit       | int    | no       | 1             |
| parallel_scan_threads | int    | no       | 2             |
| common-options        |        | no       | -             |

### url [string]

The DynamoDB endpoint URL, for example `https://dynamodb.us-east-1.amazonaws.com`.

When testing with DynamoDB Local, use the local endpoint, for example `http://127.0.0.1:8000`.

### region [string]

The AWS region of the DynamoDB service, such as `us-east-1`.

### access_key_id [string]

The AWS access key ID used to connect to DynamoDB.

### secret_access_key [string]

The AWS secret access key used to connect to DynamoDB.

### table [string]

The DynamoDB table name to scan.

### schema [config]

Defines the SeaTunnel fields to read from DynamoDB items.

DynamoDB is a key-value and document database. The source connector cannot infer a complete SeaTunnel schema from DynamoDB, so every field that should be read must be listed here.

```hocon
schema = {
  fields {
    id = string
    c_map = "map<string, smallint>"
    c_array = "array<tinyint>"
    c_string = string
    c_boolean = boolean
    c_int = int
    c_bigint = bigint
    c_float = float
    c_double = double
    c_decimal = "decimal(2, 1)"
    c_bytes = bytes
    c_date = date
    c_timestamp = timestamp
  }
}
```

For more schema syntax, see [Schema Feature](../../introduction/concepts/schema-feature.md).

### scan_item_limit [int]

The maximum number of items returned by each DynamoDB scan request.

### parallel_scan_threads [int]

The number of logical scan segments used for DynamoDB parallel scan.

This value controls how the source splits the table scan. It should usually be aligned with job parallelism and table size.

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

## Data Type Mapping

| SeaTunnel Data Type | DynamoDB Attribute Type |
|---------------------|-------------------------|
| BOOLEAN             | BOOL                    |
| TINYINT             | N                       |
| SMALLINT            | N                       |
| INT                 | N                       |
| BIGINT              | N                       |
| FLOAT               | N                       |
| DOUBLE              | N                       |
| DECIMAL             | N                       |
| STRING              | S                       |
| TIME                | S                       |
| DATE                | S                       |
| TIMESTAMP           | S                       |
| BYTES               | B                       |
| MAP                 | M                       |
| ARRAY               | L                       |
| NULL                | NULL                    |

## Task Example

The following example reads rows from `source_table` and writes them to `sink_table`.

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  AmazonDynamoDB {
    url = "http://127.0.0.1:8000"
    region = "us-east-1"
    access_key_id = "dummy-key"
    secret_access_key = "dummy-secret"
    table = "source_table"
    parallelism = 2
    scan_item_limit = 2
    parallel_scan_threads = 4
    schema = {
      fields {
        id = string
        c_map = "map<string, smallint>"
        c_array = "array<tinyint>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(2, 1)"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
  }
}

sink {
  AmazonDynamoDB {
    url = "http://127.0.0.1:8000"
    region = "us-east-1"
    access_key_id = "dummy-key"
    secret_access_key = "dummy-secret"
    table = "sink_table"
    batch_size = 25
  }
}
```

## Changelog

<ChangeLog />
