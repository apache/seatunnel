import ChangeLog from '../changelog/connector-qdrant.md';

# Qdrant

> Qdrant source connector

## Description

[Qdrant](https://qdrant.tech/) is a high-performance vector search engine and vector database.

This connector can be used to read data from a Qdrant collection.

## Options

|      name       |  type  | required | default value |
|-----------------|--------|----------|---------------|
| collection_name | string | yes      | -             |
| schema          | config | yes      | -             |
| host            | string | no       | localhost     |
| port            | int    | no       | 6334          |
| api_key         | string | no       | -             |
| use_tls         | bool   | no       | false         |
| common-options  |        | no       | -             |

### collection_name [string]

The name of the Qdrant collection to read data from.

### schema [config]

The schema of the table to read data into. For more details, please refer to [Schema Feature](../../introduction/concepts/schema-feature.md).

Eg:

```hocon
schema = {
  fields {
    age = int
    address = string
    some_vector = float_vector
  }
}
```

Each entry in Qdrant is called a point.

Vector columns are read from the vectors of each point. Other columns are read from the JSON payload associated with the point.

If a column is marked as primary key, the ID of the Qdrant point is written into it. It can be of type `"string"` or `"int"`. Since Qdrant only [allows](https://qdrant.tech/documentation/concepts/points/#point-ids) positive integers and UUIDs as point IDs.

If the collection was created with a single default/unnamed vector, use `default_vector` as the vector name.

```hocon
schema = {
  fields {
    age = int
    address = string
    default_vector = float_vector
  }
}
```

The ID of the point in Qdrant will be written into the column which is marked as the primary key. It can be of type `int` or `string`.

### host [string]

The host name of the Qdrant instance. Defaults to "localhost".

### port [int]

The gRPC port of the Qdrant instance.

### api_key [string]

The API key to use for authentication if set.

### use_tls [bool]

Whether to use TLS(SSL) connection. Required if using Qdrant cloud(https).

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

## Task Example

The Qdrant collection must already exist before the job starts. Vector field names and dimensions in the collection must match the schema used by SeaTunnel.

The following example reads payload fields and a named vector from `source_collection`, then writes the rows to `sink_collection`.

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
