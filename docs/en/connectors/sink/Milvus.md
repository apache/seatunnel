import ChangeLog from '../changelog/connector-milvus.md';

# Milvus

> Milvus sink connector

## Description

This Milvus sink connector writes data to Milvus or Zilliz Cloud. It can create the target
collection from the upstream SeaTunnel schema and supports vector fields, dynamic fields,
partition keys, partition metadata, and automatic retries for rate limit or gRPC limit errors.

Common use cases:

- Write `FLOAT_VECTOR`, `BINARY_VECTOR`, `FLOAT16_VECTOR`, `BFLOAT16_VECTOR`, and `SPARSE_FLOAT_VECTOR` fields.
- Create a collection when it does not exist.
- Copy data from one Milvus collection to another Milvus collection.
- Preserve partition information when the upstream Milvus source carries partition metadata.
- Create vector indexes and load the collection after writing when requested.

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
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

| Name                   | Type                | Required | Default                      | Description                                                                                                                                                                                                                      |
|------------------------|---------------------|----------|------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                    | String              | Yes      | -                            | The URL to connect to Milvus or Zilliz Cloud, for example `http://127.0.0.1:19530`.                                                                                                                                              |
| token                  | String              | Yes      | -                            | Milvus authentication token. For a local Milvus server this is usually `username:password`.                                                                                                                                      |
| database               | String              | No       | -                            | Target database. If it is not set, the sink uses the upstream database when available.                                                                                                                                            |
| collection             | String              | No       | -                            | Target collection. If it is not set, the sink uses the upstream table name. The deprecated `collection_name` key is accepted as an alias.                                                                                        |
| schema_save_mode       | enum                | No       | CREATE_SCHEMA_WHEN_NOT_EXIST | Controls how SeaTunnel handles the target collection schema. The default creates the collection only when it does not already exist.                                                                                              |
| data_save_mode         | enum                | No       | APPEND_DATA                  | Controls how SeaTunnel handles existing data. Supported values are `DROP_DATA`, `APPEND_DATA`, and `ERROR_WHEN_DATA_EXISTS`.                                                                                                    |
| enable_auto_id         | boolean             | No       | false                        | Whether Milvus should generate primary key values automatically. If this is `true`, do not send values for the primary key field.                                                                                                 |
| enable_upsert          | boolean             | No       | true                         | Whether to write by upsert instead of insert. Upsert needs a primary key in the collection schema.                                                                                                                               |
| enable_dynamic_field   | boolean             | No       | true                         | Whether to enable Milvus dynamic fields when SeaTunnel creates a collection.                                                                                                                                                     |
| batch_size             | int                 | No       | 1000                         | Number of records buffered before one write. In streaming jobs, data can also be flushed when a checkpoint is triggered.                                                                                                         |
| rate_limit             | int                 | No       | 100000                       | Maximum write rate used by the connector retry/rate-limit control.                                                                                                                                                              |
| partition_key          | String              | No       | -                            | Field name used as the Milvus partition key when SeaTunnel creates a collection.                                                                                                                                                 |
| create_index           | boolean             | No       | false                        | Whether to create vector indexes for the target collection. When copying from Milvus source, existing vector index metadata can be used to create matching indexes on the target collection.                                     |
| load_collection        | boolean             | No       | false                        | Whether to load the target collection into Milvus memory after the collection is created. This is useful when the data should be queried immediately after writing.                                                              |
| collection_description | Map<String, String> | No       | {}                           | Collection descriptions map. The key is the collection name and the value is the description used when the collection is created.                                                                                                |

## Notes

- Vector dimensions come from the SeaTunnel schema. For vector fields generated by `FakeSource`, `columnScale` is used as the vector dimension.
- If `collection` is not set, the sink uses the upstream table name as the Milvus collection name. This is useful for multi-table jobs.
- When a Milvus source reads partitions, the sink can create the same partition names on the target collection if the target collection does not use a partition key.
- `create_index = true` only creates vector indexes when index metadata is available from the upstream catalog, such as when the source is Milvus.

## Task Example

### Write a SeaTunnel Schema to Milvus

This example writes 10 rows into the `test1.simple_example_1` collection. The sink uses the source
table name as the collection name because `collection` is not set.

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 10
    vector.dimension = 4
    schema = {
      table = "simple_example_1"
      columns = [
        {
          name = book_id
          type = bigint
          nullable = false
          defaultValue = 0
          comment = "primary key id"
        },
        {
          name = book_intro
          type = float_vector
          columnScale = 4
          comment = "vector"
        },
        {
          name = book_title
          type = string
          nullable = true
          comment = "topic"
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
    database = "test1"
  }
}
```

### Write Multiple Vector Types

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 10
    vector.dimension = 4
    binary.vector.dimension = 8
    schema = {
      table = "simple_example_2"
      columns = [
        {
          name = book_id
          type = bigint
          nullable = false
          defaultValue = 0
          comment = "primary key id"
        },
        {
          name = book_intro_1
          type = binary_vector
          columnScale = 8
          comment = "binary vector"
        },
        {
          name = book_intro_2
          type = float16_vector
          columnScale = 4
          comment = "float16 vector"
        },
        {
          name = book_intro_3
          type = bfloat16_vector
          columnScale = 4
          comment = "bfloat16 vector"
        },
        {
          name = book_intro_4
          type = sparse_float_vector
          columnScale = 4
          comment = "sparse vector"
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
    database = "test2"
  }
}
```

### Copy One Milvus Collection and Create Indexes

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    collection = "simple_example"
  }
}

sink {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    database = "test_index_preservation"
    collection = "simple_example_preservation"
    create_index = true
    load_collection = true
  }
}
```

### Streaming Write with Checkpoint Flush

```bash
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  FakeSource {
    row.num = 10
    vector.dimension = 4
    schema = {
      table = "streaming_simple_example"
      columns = [
        {
          name = book_id
          type = bigint
          nullable = false
          defaultValue = 0
          comment = "primary key id"
        },
        {
          name = book_intro
          type = float_vector
          columnScale = 4
          comment = "vector"
        },
        {
          name = book_title
          type = string
          nullable = true
          comment = "topic"
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
    database = "streaming_test"
    enable_upsert = false
    batch_size = 3
  }
}
```

## Changelog

<ChangeLog />
