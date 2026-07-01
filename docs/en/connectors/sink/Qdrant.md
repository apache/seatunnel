import ChangeLog from '../changelog/connector-qdrant.md';

# Qdrant

> Qdrant Sink Connector

## Description

[Qdrant](https://qdrant.tech/) is a high-performance vector search engine and vector database.

This connector can be used to write data into a Qdrant collection.

## Data Type Mapping

| SeaTunnel Data Type | Qdrant Data Type |
|---------------------|------------------|
| SMALLINT            | INTEGER          |
| INT                 | INTEGER          |
| BIGINT              | INTEGER          |
| FLOAT               | DOUBLE           |
| DOUBLE              | DOUBLE           |
| BOOLEAN             | BOOL             |
| STRING              | STRING           |
| DATE                | STRING           |
| FLOAT_VECTOR        | DENSE_VECTOR     |
| BINARY_VECTOR       | DENSE_VECTOR     |
| FLOAT16_VECTOR      | DENSE_VECTOR     |
| BFLOAT16_VECTOR     | DENSE_VECTOR     |

The value of the primary key column will be used as point ID in Qdrant. Supported primary key types are `INT` for numeric point IDs and `STRING` for UUID point IDs. If no primary key is present, a random UUID will be used.

Non-vector columns are written into the Qdrant payload with the same field name. Vector columns are written as named vectors with the same field name, so the target collection must already define matching vector names and dimensions.

## Options

|      name       |  type  | required | default value |
|-----------------|--------|----------|---------------|
| collection_name | string | yes      | -             |
| host            | string | no       | localhost     |
| port            | int    | no       | 6334          |
| api_key         | string | no       | -             |
| use_tls         | bool   | no       | false         |
| common-options  |        | no       | -             |

### collection_name [string]

The name of the Qdrant collection to write data into.

### host [string]

The host name of the Qdrant instance. Defaults to "localhost".

### port [int]

The gRPC port of the Qdrant instance.

### api_key [string]

The API key to use for authentication if set.

### use_tls [bool]

Whether to use TLS(SSL) connection. Required if using Qdrant cloud(https).

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

## Task Example

The following example writes two payload fields, `file_name` and `file_size`, and one named vector, `my_vector`, into Qdrant.

Before running the job, create the target Qdrant collection and define a vector named `my_vector` with dimension `4`.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 10
    vector.dimension = 4
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
