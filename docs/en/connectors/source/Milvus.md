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
| rate_limit  | Integer | No       | 1000000    | Server-side query rate limit (QPS) applied to the source collection via the Milvus `collection.queryRate.max.qps` property. The reader mutates this collection-wide setting while the job is running, so it affects every client of the collection, not just this SeaTunnel job. Set to `-1` to disable.                                                                                                                                |

## Notes

- `database` defaults to `default`, so simple local Milvus jobs do not need to set it.
- `collection` is optional. Set it when the job should read exactly one collection.
- `batch_size` controls the per-fetch page size, not the parallelism of readers. Tune it together with `parallelism` to balance throughput and memory.
- `rate_limit` mutates the server-side `collection.queryRate.max.qps` property on every collection the job reads, so the new limit applies to all clients of that collection while the job is running. The reader resets the property to `-1` on close, but a job crash before `close()` will leave the collection throttled until it is restored manually. Leave it at the default unless you observe throttling errors in the logs.
- When `collection` is not set, the source discovers all collections in `database` and exposes each collection as a separate SeaTunnel table.
- The source splits work by Milvus partition. Collections with a partition key are read with one split; collections without a partition key are split by partition name and assigned across readers.
- When the source reads a collection with partitions, downstream Milvus sink can use that metadata to create the same partition names on the target collection.
- When the source reads vector indexes, downstream Milvus sink can use that metadata with `create_index = true` to create matching vector indexes.
- The Milvus source is BOUNDED: the job finishes naturally once every partition (split) has been fully scanned, and unlike Kafka or Fluss there is no per-record offset for continuous incremental reads. Checkpoint/restore is at split (partition) granularity — a partition that has already been fully scanned is not re-read, but a partition that was in progress when the job failed is re-scanned from the beginning of that partition. If you need to keep ingesting newly written vectors, re-submit the SeaTunnel job periodically from an external scheduler.

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

### Re-submit with Checkpoints for Periodic Ingestion

The Milvus source is BOUNDED, so the job finishes naturally once every partition has been fully scanned. This example runs in `STREAMING` mode with a short checkpoint interval — to keep ingesting newly written vectors, re-submit the job from an external scheduler on demand. On restore, recovery is at split (partition) granularity: partitions that were already fully scanned are not re-read, while partitions that were in progress when the job failed are re-scanned from the beginning of that partition. The downstream sink uses `enable_upsert = true` and dedupes by primary key to avoid duplicate writes on re-scanned partitions.

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
    enable_upsert = true
    batch_size = 1000
  }
}
```

### Throttle Reads Against a Shared Cluster

When the Milvus cluster is shared with other jobs, lower `rate_limit` and `batch_size`
so the source does not exceed the cluster's per-collection query quota.

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
