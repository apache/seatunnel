import ChangeLog from '../changelog/connector-amazondynamodb.md';

# AmazonDynamoDB

> Amazon DynamoDB sink connector

## Description

The Amazon DynamoDB sink connector writes SeaTunnel rows to a DynamoDB table.

The target table must already exist. The connector writes each row as a DynamoDB item and uses batch write requests. It supports single-table writes and multi-table writes when the upstream row carries a table id.

## Supported Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Options

| name                | type   | required | default value | description                                      |
|---------------------|--------|----------|---------------|--------------------------------------------------|
| url                 | string | yes      | -             | DynamoDB endpoint URL.                           |
| region              | string | yes      | -             | AWS region of the DynamoDB service.              |
| access_key_id       | string | yes      | -             | AWS access key ID.                               |
| secret_access_key   | string | yes      | -             | AWS secret access key.                           |
| table               | string | yes      | -             | DynamoDB table name to write to.                 |
| batch_size          | int    | no       | 25            | Records buffered for one batch write request.    |
| multi_table_sink_replica | int | no       | -             | Sink writer replicas for each table.             |
| max_retries         | int    | no       | 10            | Retries for unprocessed items. Must be at least `0`. |
| retry_base_delay_ms | long   | no       | 100           | Initial retry backoff delay in milliseconds.     |
| retry_max_delay_ms  | long   | no       | 5000          | Maximum retry backoff delay in milliseconds.     |
| common-options      | object | no       | -             | Sink plugin common parameters.                   |

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

The DynamoDB table name to write to.

For a normal single-table job, set this to the target table name. In a multi-table pipeline, the writer uses the table id carried by each row as the target table name and falls back to this configured table when the row has no table id.

### batch_size [int]

The number of records buffered for one DynamoDB batch write request.

DynamoDB batch write supports up to 25 write requests per call, so the default is `25`.
Do not set this higher than `25`; larger values do not match the DynamoDB batch write API limit.

### multi_table_sink_replica [int]

Optional common sink option used by multi-table sink jobs. For details, see [Sink Common Options](../common-options/sink-common-options.md).

### max_retries [int]

The maximum number of retries when DynamoDB returns unprocessed items from a batch write request.
A value of `0` disables retries. The value must not be negative.

### retry_base_delay_ms [long]

The base delay, in milliseconds, used by exponential backoff between retries.

### retry_max_delay_ms [long]

The maximum delay, in milliseconds, between retries.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

## Usage Notes

- Create the target DynamoDB table before starting the SeaTunnel job. The sink does not create tables or key schemas.
- `access_key_id` and `secret_access_key` are required by this connector. For DynamoDB Local, use dummy values accepted by the local service.
- DynamoDB accepts at most 25 write requests in one batch write call, so keep `batch_size` at or below `25`.
- The sink retries unprocessed items with exponential backoff. It does not provide exactly-once guarantees.

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
    max_retries = 10
    retry_base_delay_ms = 100
    retry_max_delay_ms = 5000
  }
}
```

## Changelog

<ChangeLog />
