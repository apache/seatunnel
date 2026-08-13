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
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table read](../../introduction/concepts/connector-v2-features.md)

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

| Name        | Type    | Required | Default    | Description                                                                                                                                                                                                                |
|-------------|---------|----------|------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url         | String  | Yes      | -          | The URL to connect to Milvus or Zilliz Cloud, for example `http://127.0.0.1:19530`.                                                                                                                                        |
| token       | String  | Yes      | -          | Milvus authentication token. For a local Milvus server this is usually `username:password`.                                                                                                                                |
| database    | String  | No       | `default`  | Source database.                                                                                                                                                                                                            |
| collection  | String  | No       | -          | Source collection. If it is set, only this collection is read. If it is not set, all collections under `database` are read. The legacy alias `collection_name` is also accepted.                                          |
| batch_size  | Integer | No       | 1000       | Number of records to fetch from Milvus in one batch. A larger value improves throughput but uses more memory; set it to a smaller value when records contain large vector payloads.                                          |
| rate_limit  | Integer | No       | 1000000    | Maximum number of records the reader requests from Milvus per second. Use this to throttle a streaming job against the Milvus quota (QPS) or gRPC message-size limit. Set to `-1` to disable throttling.                    |

## Notes

- `database` defaults to `default`, so simple local Milvus jobs do not need to set it.
- `collection` is optional. Set it when the job should read exactly one collection.
- `batch_size` controls the per-fetch page size, not the parallelism of readers. Tune it together with `parallelism` to balance throughput and memory.
- `rate_limit` is a server-side hint that protects against Milvus `GRPC limit` errors when reading large volumes of vector data. Leave it at the default unless you see rate-limit or gRPC errors in the logs.
- When `collection` is not set, the source discovers all collections in `database` and exposes each collection as a separate SeaTunnel table.
- The source splits work by Milvus partition. Collections with a partition key are read with one split; collections without a partition key are split by partition name and assigned across readers.
- When the source reads a collection with partitions, downstream Milvus sink can use that metadata to create the same partition names on the target collection.
- When the source reads vector indexes, downstream Milvus sink can use that metadata with `create_index = true` to create matching vector indexes.
- Streaming jobs should set a checkpoint interval and reuse the same Milvus token across restarts so that incremental reads resume correctly from the committed offset.

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

### Stream From One Collection With Checkpoints

This example runs the source in `STREAMING` mode with a 30 second checkpoint interval.
The downstream sink uses `enable_upsert = false` so each row is inserted once and
duplicates are rejected.

```bash
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    database = "streaming_test"
    collection = "simple_example"
    batch_size = 500
    rate_limit = 200000
  }
}

sink {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    database = "streaming_test"
    enable_upsert = false
    batch_size = 1000
  }
}
```

### Throttle Reads Against a Shared Cluster

When the Milvus cluster is shared with other jobs, lower `rate_limit` and `batch_size`
so the source does not exceed the cluster's gRPC message-size limit.

```bash
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    database = "shared"
    batch_size = 200
    rate_limit = 100000
  }
}

sink {
  Console {}
}
```

## Changelog

<ChangeLog />
