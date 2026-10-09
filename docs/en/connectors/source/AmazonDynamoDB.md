import ChangeLog from '../changelog/connector-amazondynamodb.md';

# AmazonDynamoDB

> Amazon DynamoDB source connector

## Description

The Amazon DynamoDB source connector reads existing items from an Amazon DynamoDB table by using DynamoDB scan requests.

The connector is a batch source. DynamoDB does not expose field types in the same way as a relational database, so the SeaTunnel schema must be configured explicitly.

This source reads the current table data with scan requests. It does not read DynamoDB Streams or CDC change events.

Use `tables_configs` to read several DynamoDB tables with different schemas through one source.

## Supported Engines

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
- [x] [multi-table](../../introduction/concepts/connector-v2-features.md)

## Options

| name                  | type   | required | default value | description                                      |
|-----------------------|--------|----------|---------------|--------------------------------------------------|
| url                   | string | yes      | -             | DynamoDB endpoint URL.                           |
| region                | string | yes      | -             | AWS region of the DynamoDB service.              |
| access_key_id         | string | yes      | -             | AWS access key ID.                               |
| secret_access_key     | string | yes      | -             | AWS secret access key.                           |
| table                 | string | no       | -             | Required in single-table mode; mutually exclusive with `tables_configs`. |
| schema                | config | no       | -             | Required in single-table mode; define it per entry in multi-table mode. |
| tables_configs        | list   | no       | -             | Tables and schemas to read in multi-table mode; see below. |
| scan_item_limit       | int    | no       | 1             | Maximum items returned by each scan request.     |
| parallel_scan_threads | int    | no       | 2             | Number of logical segments for parallel scan.    |
| common-options        | object | no       | -             | Source plugin common parameters.                 |

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

The DynamoDB table name to scan in single-table mode.

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

### tables_configs [list]

An alternative to the root-level `table` and `schema` for reading several tables. Each entry requires:

- `table`: the DynamoDB table name to scan.
- `schema`: the SeaTunnel fields to read from that table. `schema.table` sets the output table
  identity; when it is not set, the DynamoDB table name is used. A dot in the name is read as a
  `database.table` separator, so set `schema.table` when the DynamoDB table name contains dots.

An entry can also set `scan_item_limit` and `parallel_scan_threads`. When an entry does not set
them, the root-level values (or their defaults) are used. Each table is scanned with its own
`parallel_scan_threads` segments, and each row carries the identity of the table it was read from.

All entries share the root connection options `url`, `region`, `access_key_id` and
`secret_access_key`; connection options inside an entry are rejected. A root-level `table` or
`schema`, an empty list, an entry without `table` or `schema`, unsupported entry options and
duplicate output table identities are rejected before the job starts. Keep the table identities
stable when restoring a checkpoint; switching between single-table and multi-table mode requires a
fresh job.

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

### Read One Table

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

### Read Multiple Tables

The following example reads `orders` and `customers` with different schemas. Rows from `customers`
are routed downstream as `crm.customers`.

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
    parallel_scan_threads = 2
    tables_configs = [
      {
        table = "orders"
        parallel_scan_threads = 4
        schema = {
          fields {
            id = string
            amount = int
          }
        }
      },
      {
        table = "customers"
        schema = {
          table = "crm.customers"
          fields {
            id = string
            name = string
            vip = boolean
          }
        }
      }
    ]
  }
}

sink {
  Console {}
}
```

## Changelog

<ChangeLog />
