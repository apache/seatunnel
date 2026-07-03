import ChangeLog from '../changelog/connector-qdrant.md';

# Qdrant

> Qdrant sink connector

## Description

[Qdrant](https://qdrant.tech/) is a high-performance vector search engine and vector database.

The Qdrant sink writes SeaTunnel rows into one existing Qdrant collection. Normal columns are written to the point payload, vector columns are written as named vectors, and the primary key column is used as the Qdrant point ID when one is present.

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [x] [multimodal](../../introduction/concepts/connector-v2-features.md)

## Options

| name            | type   | required | default value | description |
|-----------------|--------|----------|---------------|-------------|
| collection_name | string | yes      | -             | Qdrant collection name to write to. |
| host            | string | no       | localhost     | Qdrant gRPC host. |
| port            | int    | no       | 6334          | Qdrant gRPC port. |
| api_key         | string | no       | -             | Qdrant API key for authenticated deployments. |
| use_tls         | bool   | no       | false         | Whether to use TLS for the gRPC connection. |
| common-options  |        | no       | -             | Sink common options. |

### collection_name [string]

The name of the Qdrant collection to write.

### host [string]

The host name of the Qdrant instance.

### port [int]

The gRPC port of the Qdrant instance.

### api_key [string]

The API key used to connect to authenticated Qdrant deployments.

### use_tls [bool]

Whether to use TLS for the gRPC connection. Enable this when connecting to Qdrant Cloud or another HTTPS/TLS endpoint.

### common options

Sink plugin common parameters, see [Sink Common Options](../common-options/sink-common-options.md) for details.

## Supported Types

| SeaTunnel type | Qdrant value |
|----------------|--------------|
| SMALLINT       | payload integer |
| INT            | payload integer or numeric point ID |
| BIGINT         | payload integer |
| FLOAT          | payload double |
| DOUBLE         | payload double |
| STRING         | payload string or UUID point ID |
| DATE           | payload string |
| BOOLEAN        | payload bool |
| FLOAT_VECTOR   | named vector |
| BINARY_VECTOR  | named vector |
| FLOAT16_VECTOR | named vector |
| BFLOAT16_VECTOR | named vector |

The value of the primary key column is used as the Qdrant point ID. Primary key values must be `INT` numeric IDs or `STRING` UUIDs. If no primary key is present, the sink generates a random UUID for each row.

## Notes

- The target collection must already exist before the job starts. The connector does not create collections or vector indexes.
- Vector column names and dimensions must match the vector configuration of the target Qdrant collection.
- The sink writes each incoming row as an upsert request. It does not interpret `UPDATE` or `DELETE` row kinds as CDC operations.
- Each sink block writes to one collection. Use separate sink blocks when different collections need different settings.

## Task Example

The following example writes records from one Qdrant collection to another. `file_name` and `file_size` are written as point payload fields, and `my_vector` is written as a named vector.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Qdrant {
    collection_name = "source_collection"
    host = "localhost"
    port = 6334
    schema = {
      columns = [
        {
          name = file_name
          type = string
        }
        {
          name = file_size
          type = int
        }
        {
          name = my_vector
          type = float_vector
        }
      ]
    }
  }
}

sink {
  Qdrant {
    collection_name = "sink_collection"
    host = "localhost"
    port = 6334
  }
}
```

## Changelog

<ChangeLog />
