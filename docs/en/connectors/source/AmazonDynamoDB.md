import ChangeLog from '../changelog/connector-amazondynamodb.md';

# AmazonDynamoDB Source Connector

`Source: AmazonDynamoDB`

Read existing items from an Amazon DynamoDB table by issuing DynamoDB scan requests. The connector is a batch source; DynamoDB does not expose field types the way a relational database does, so the SeaTunnel schema must be configured explicitly. This source reads the current table data with scan requests. It does **not** subscribe to DynamoDB Streams or CDC change events.

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Source Options

| Name                  | Type   | Required | Default Value | Description                                      |
|-----------------------|--------|----------|---------------|--------------------------------------------------|
| url                   | String | Yes      | -             | DynamoDB endpoint URL. For local testing, use `http://127.0.0.1:8000`. |
| region                | String | Yes      | -             | AWS region of the DynamoDB service, for example `us-east-1`. |
| access_key_id         | String | Yes      | -             | AWS access key ID.                               |
| secret_access_key     | String | Yes      | -             | AWS secret access key.                           |
| table                 | String | Yes      | -             | DynamoDB table name to scan.                     |
| schema                | config | Yes      | -             | SeaTunnel fields to read from DynamoDB items.    |
| scan_item_limit       | Int    | No       | 1             | Maximum items returned by each scan request.     |
| parallel_scan_threads | Int    | No       | 2             | Number of logical segments for parallel scan.    |
| common-options        | object | No       | -             | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md). |

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

Larger values reduce the number of requests but may increase the memory used by each read batch.

### parallel_scan_threads [int]

The number of logical scan segments used for DynamoDB parallel scan.

This value controls how the source splits the table scan. It should usually be aligned with job parallelism and table size.

For small tables, keep the default value. For large tables, increase it together with `env.parallelism` and the source `parallelism` option so that multiple readers can scan different segments.

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

## Usage Notes

- The source uses DynamoDB scan requests, so it reads the current table snapshot rather than change events.
- `access_key_id` and `secret_access_key` are required by this connector. For DynamoDB Local, use dummy values accepted by the local service.
- `parallel_scan_threads` controls the number of DynamoDB scan segments. Increase it together with job parallelism for larger tables.
- `scan_item_limit` is the page limit used on each scan request, not the total number of rows in the job.

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
