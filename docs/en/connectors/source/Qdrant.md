import ChangeLog from '../changelog/connector-qdrant.md';

# Qdrant

> Qdrant source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

[Qdrant](https://qdrant.tech/) is a high-performance vector search engine and vector database.

The Qdrant source reads points from one existing Qdrant collection. Point payload fields are read as normal SeaTunnel columns, and Qdrant vectors are read as SeaTunnel vector columns.

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [multimodal](../../introduction/concepts/connector-v2-features.md)
- [ ] [support multiple table read](../../introduction/concepts/connector-v2-features.md)

## Options

| name            | type   | required | default value | description |
|-----------------|--------|----------|---------------|-------------|
| collection_name | string | yes      | -             | Qdrant collection name to read from. |
| schema          | config | yes      | -             | SeaTunnel schema used to map Qdrant point IDs, payload fields, and vectors. |
| host            | string | no       | localhost     | Qdrant gRPC host. |
| port            | int    | no       | 6334          | Qdrant gRPC port. |
| api_key         | string | no       | ""            | Qdrant API key for authenticated deployments. |
| use_tls         | bool   | no       | false         | Whether to use TLS for the gRPC connection. |
| common-options  |        | no       | -             | Source common options. |

### collection_name [string]

The name of the Qdrant collection to read.

### schema [config]

The schema of the SeaTunnel rows produced by the source. For more details, see [Schema Feature](../../introduction/concepts/schema-feature.md).

Each Qdrant record is a point:

- A SeaTunnel primary key column is filled from the Qdrant point ID. Qdrant point IDs can be positive integers or UUID strings.
- Vector columns are read from Qdrant vectors. For named vectors, the SeaTunnel column name must match the Qdrant vector name.
- Other supported columns are read from the Qdrant point payload.
- If the collection uses one default unnamed vector, use `default_vector` as the SeaTunnel vector column name.
- Include only payload or vector fields that exist on the points being read. The source maps fields directly by name and expects configured fields to be present.

Example:

```hocon
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
```

### host [string]

The host name of the Qdrant instance.

### port [int]

The gRPC port of the Qdrant instance.

### api_key [string]

The API key used to connect to authenticated Qdrant deployments. Leave it empty when the Qdrant service does not require authentication.

### use_tls [bool]

Whether to use TLS for the gRPC connection. Enable this when connecting to Qdrant Cloud or another HTTPS/TLS endpoint.

### common options

Source plugin common parameters, see [Source Common Options](../common-options/source-common-options.md) for details.

## Supported Types

| SeaTunnel type | Qdrant value |
|----------------|--------------|
| STRING         | payload string or point UUID |
| BOOLEAN        | payload bool |
| TINYINT        | payload integer |
| SMALLINT       | payload integer |
| INT            | payload integer or point numeric ID |
| BIGINT         | payload integer |
| FLOAT          | payload double |
| DOUBLE         | payload double |
| DECIMAL        | payload double |
| FLOAT_VECTOR   | vector |
| BINARY_VECTOR  | vector |
| FLOAT16_VECTOR | vector |
| BFLOAT16_VECTOR | vector |

## Notes

- The collection must already exist before the job starts.
- Vector field names and dimensions in Qdrant must match the vector columns in the SeaTunnel schema.
- For a collection with one unnamed vector, name the SeaTunnel vector column `default_vector`.
- The source is bounded and reads with one split, so it is not a parallel source.
- Qdrant source emits `INSERT` rows only; it does not read CDC changes.

## Task Example

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
