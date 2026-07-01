import ChangeLog from '../changelog/connector-milvus.md';

# Milvus

> Milvus source connector

## Description

This Milvus source connector reads data from Milvus or Zilliz Cloud. It can read one collection
or all collections in a database, and it carries Milvus metadata such as partition information and
vector index information to downstream connectors when the target connector can use it.

Common use cases:

- Read one Milvus collection by setting `collection`.
- Read all collections in a Milvus database by leaving `collection` empty.
- Copy data from Milvus to Milvus while preserving vector fields, partition metadata, and index metadata.
- Read `FLOAT_VECTOR`, `BINARY_VECTOR`, `FLOAT16_VECTOR`, `BFLOAT16_VECTOR`, and `SPARSE_FLOAT_VECTOR` fields.
- Retry automatically to bypass rate limit or gRPC limit errors.

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)

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

## Source Options

| Name       | Type   | Required | Default | Description                                                                                                                                                                      |
|------------|--------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url        | String | Yes      | -       | The URL to connect to Milvus or Zilliz Cloud, for example `http://127.0.0.1:19530`.                                                                                              |
| token      | String | Yes      | -       | Milvus authentication token. For a local Milvus server this is usually `username:password`.                                                                                      |
| database   | String | No       | default | Source database.                                                                                                                                                                 |
| collection | String | No       | -       | Source collection. If it is set, only this collection is read. If it is not set, all collections under `database` are read. The deprecated `collection_name` key is accepted as an alias. |

## Notes

- `database` defaults to `default`, so simple local Milvus jobs do not need to set it.
- `collection` is optional. Set it when the job should read exactly one collection.
- When the source reads a collection with partitions, downstream Milvus sink can use that metadata to create the same partition names on the target collection.
- When the source reads vector indexes, downstream Milvus sink can use that metadata with `create_index = true` to create matching vector indexes.

## Task Example

### Read One Collection

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    database = "default"
    collection = "simple_example"
  }
}

sink {
  Console {}
}
```

### Read All Collections in a Database

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    database = "default"
  }
}

sink {
  Console {}
}
```

### Copy a Milvus Collection to Another Database

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
    database = "test"
    collection = "simple_example"
  }
}
```

### Copy a Collection and Recreate Vector Indexes

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
  }
}
```

## Changelog

<ChangeLog />
