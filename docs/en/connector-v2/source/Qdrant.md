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
| use_tls         | int    | no       | false         |
| common-options  |        | no       | -             |

### collection_name [string]

The name of the Qdrant collection to read data from.

### schema [config]

The schema of the table to read data into.

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

The `float_vector` type columns are read from the vectors of each point, others are read from the JSON payload associated with the point.

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

Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details.
