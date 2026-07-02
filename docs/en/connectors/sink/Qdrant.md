import ChangeLog from '../changelog/connector-qdrant.md';

# Qdrant

> Qdrant Sink Connector

## Description

[Qdrant](https://qdrant.tech/) is a high-performance vector search engine and vector database.

This connector can be used to write data into a Qdrant collection.

The target collection must already exist before the job starts. Vector field names and dimensions in Qdrant must match the vector columns in the SeaTunnel row.

## Data Type Mapping

| SeaTunnel Data Type | Qdrant Data Type |
|---------------------|------------------|
| TINYINT             | INTEGER          |
| SMALLINT            | INTEGER          |
| INT                 | INTEGER          |
| BIGINT              | INTEGER          |
| FLOAT               | DOUBLE           |
| DOUBLE              | DOUBLE           |
| BOOLEAN             | BOOL             |
| STRING              | STRING           |
| ARRAY               | LIST             |
| FLOAT_VECTOR        | DENSE_VECTOR     |
| BINARY_VECTOR       | DENSE_VECTOR     |
| FLOAT16_VECTOR      | DENSE_VECTOR     |
| BFLOAT16_VECTOR     | DENSE_VECTOR     |
| SPARSE_FLOAT_VECTOR | SPARSE_VECTOR    |

The value of the primary key column will be used as point ID in Qdrant. If no primary key is present, a random UUID will be used.

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

The name of the Qdrant collection to write data to.

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

The following example writes records from a Qdrant source collection to another Qdrant collection. Payload fields such as `file_name` and `file_size` are written as point payloads, and `my_vector` is written as a named vector.

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
