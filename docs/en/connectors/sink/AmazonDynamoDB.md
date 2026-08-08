import ChangeLog from '../changelog/connector-amazondynamodb.md';

# AmazonDynamoDB Sink Connector

`Sink: AmazonDynamoDB`

Write SeaTunnel rows to a DynamoDB table. The target table must already exist; the connector does not create tables or key schemas. Each row is written as a DynamoDB item through a batch write request, so the connector supports single-table writes (by configuring `table`) and multi-table writes (when the upstream row carries a table id, the writer routes each row to that target).

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Sink Options

| Name                | Type   | Required | Default Value | Description                                      |
|---------------------|--------|----------|---------------|--------------------------------------------------|
| url                 | String | Yes      | -             | DynamoDB endpoint URL. For local testing, use `http://127.0.0.1:8000`. |
| region              | String | Yes      | -             | AWS region of the DynamoDB service, for example `us-east-1`. |
| access_key_id       | String | Yes      | -             | AWS access key ID.                               |
| secret_access_key   | String | Yes      | -             | AWS secret access key.                           |
| table               | String | Yes      | -             | DynamoDB table name to write to. Used as the per-row target only when the upstream row does not carry a table id; otherwise the row's table id is preferred. |
| batch_size          | Int    | No       | 25            | Records buffered for one DynamoDB batch write request. DynamoDB accepts at most 25 write requests in one batch write call, so do not set this above `25`. |
| multi_table_sink_replica | Int | No       | -             | Optional common sink option used by multi-table sink jobs. For details, see [Sink Common Options](../common-options/sink-common-options.md). |
| max_retries         | Int    | No       | 10            | Retries for unprocessed items returned by DynamoDB. |
| retry_base_delay_ms | Long   | No       | 100           | Initial retry backoff delay in milliseconds.     |
| retry_max_delay_ms  | Long   | No       | 5000          | Maximum retry backoff delay in milliseconds (exponential).    |
| common-options      | object | No       | -             | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md). |

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
