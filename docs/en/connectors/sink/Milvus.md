import ChangeLog from '../changelog/connector-milvus.md';

# Milvus

> Milvus sink connector

## Description

This Milvus sink connector writes data to Milvus or Zilliz Cloud. It can create missing databases
and collections, write vector fields, write dynamic fields, and optionally create vector indexes
or load the target collection after the write client is initialized.

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Data Type Mapping

|  Milvus Data Type   | SeaTunnel Data Type |
|---------------------|---------------------|
| INT8                | TINYINT             |
| INT16               | SMALLINT            |
| INT32               | INT                 |
| INT64               | BIGINT              |
| FLOAT               | FLOAT               |
| DOUBLE              | DOUBLE              |
| BOOL                | BOOLEAN             |
| JSON                | STRING              |
| ARRAY               | ARRAY               |
| VARCHAR             | STRING              |
| FLOAT_VECTOR        | FLOAT_VECTOR        |
| BINARY_VECTOR       | BINARY_VECTOR       |
| FLOAT16_VECTOR      | FLOAT16_VECTOR      |
| BFLOAT16_VECTOR     | BFLOAT16_VECTOR     |
| SPARSE_FLOAT_VECTOR | SPARSE_FLOAT_VECTOR |

## Sink Options

| Name                   | Type                | Required | Default                      | Description                                                                                                                                                         |
|------------------------|---------------------|----------|------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                    | String              | Yes      | -                            | The URL to connect to Milvus or Zilliz Cloud.                                                                                                                       |
| token                  | String              | Yes      | -                            | Milvus authentication token, usually in `username:password` format.                                                                                                 |
| database               | String              | No       | -                            | Target database. If it is not set, the sink uses the upstream table database name.                                                                                   |
| collection             | String              | No       | -                            | Target collection. If it is not set, the sink uses the upstream table name. The deprecated `collection_name` key is accepted as an alias.                           |
| schema_save_mode       | enum                | No       | CREATE_SCHEMA_WHEN_NOT_EXIST | Controls how the target collection schema is handled before writing data.                                                                                           |
| data_save_mode         | enum                | No       | APPEND_DATA                  | Controls how existing target data is handled before writing data. Supported values are `DROP_DATA`, `APPEND_DATA`, and `ERROR_WHEN_DATA_EXISTS`.                    |
| enable_auto_id         | boolean             | No       | false                        | Enables Milvus AutoID for the primary key field when creating a collection. A primary key definition can also override this value.                                  |
| enable_upsert          | boolean             | No       | true                         | Uses Milvus upsert requests instead of insert requests. Set it to `false` when the job only writes new rows and does not need key-based updates.                    |
| enable_dynamic_field   | boolean             | No       | true                         | Enables the Milvus dynamic field when SeaTunnel creates the collection.                                                                                             |
| batch_size             | int                 | No       | 1000                         | Maximum number of rows buffered before a write request is sent. A checkpoint can also trigger a flush.                                                              |
| rate_limit             | int                 | No       | 100000                       | Sets Milvus insert/upsert rate limit for the collection. Values greater than `0` are applied when the writer opens and are reset when it closes.                    |
| partition_key          | String              | No       | -                            | Milvus partition key field used when SeaTunnel creates the collection. If the collection has a partition key, SeaTunnel does not create named partitions separately. |
| create_index           | boolean             | No       | false                        | Creates vector indexes for vector fields or schema vector index constraints.                                                                                        |
| load_collection        | boolean             | No       | false                        | Loads the collection into Milvus memory when the writer opens if the collection is not loaded yet.                                                                  |
| collection_description | Map<String, String> | No       | {}                           | Collection description map. The key is the collection name and the value is the description used when SeaTunnel creates that collection.                            |

### Notes

- If `database` or `collection` is omitted, the sink keeps the upstream table database or table name.
- `collection` is the recommended option name. `collection_name` is only kept for compatibility.
- `create_index = true` needs vector index metadata in the upstream schema when SeaTunnel creates the collection from catalog schema. When it is used after a collection already exists, SeaTunnel creates default indexes for vector fields.
- `enable_upsert = true` requires a meaningful primary key in the table schema. For insert-only jobs, `enable_upsert = false` can be faster.

## Task Example

### Write Vector Data

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
      table = "book_vectors"
      columns = [
        {
          name = book_id
          type = bigint
          nullable = false
        },
        {
          name = book_intro
          type = float_vector
          columnScale = 4
        },
        {
          name = book_title
          type = string
        }
      ]
      primaryKey {
        name = book_id
        columnNames = [book_id]
      }
    }
  }
}

sink {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    database = "default"
    collection = "book_vectors"
    batch_size = 1000
    enable_upsert = false
  }
}
```

### Write Multiple Vector Types

```hocon
source {
  FakeSource {
    row.num = 10
    vector.dimension = 4
    binary.vector.dimension = 8
    schema = {
      table = "multi_vector_books"
      columns = [
        { name = book_id, type = bigint, nullable = false },
        { name = binary_intro, type = binary_vector, columnScale = 8 },
        { name = fp16_intro, type = float16_vector, columnScale = 4 },
        { name = bfloat16_intro, type = bfloat16_vector, columnScale = 4 },
        { name = sparse_intro, type = sparse_float_vector, columnScale = 4 }
      ]
      primaryKey {
        name = book_id
        columnNames = [book_id]
      }
    }
  }
}

sink {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    database = "default"
    collection = "multi_vector_books"
  }
}
```

### Create Indexes and Load Collection

```hocon
sink {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    database = "default"
    collection = "book_vectors"
    create_index = true
    load_collection = true
    rate_limit = 100000
    collection_description = {
      "book_vectors" = "Book embedding vectors for search"
    }
  }
}
```

## Changelog

<ChangeLog />
