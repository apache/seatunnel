import ChangeLog from '../changelog/connector-milvus.md';

# Milvus

> Milvus source connector

## Description

This Milvus source connector reads data from Milvus or Zilliz Cloud. It can read one collection
or every collection in a database, keep vector fields in SeaTunnel vector types, and read dynamic
fields into metadata columns.

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

| Name       | Type   | Required | Default | Description                                                                                          |
|------------|--------|----------|---------|------------------------------------------------------------------------------------------------------|
| url        | String | Yes      | -       | The URL to connect to Milvus or Zilliz Cloud.                                                        |
| token      | String | Yes      | -       | Milvus authentication token, usually in `username:password` format.                                  |
| database   | String | No       | default | Source database.                                                                                     |
| collection | String | No       | -       | Source collection. If it is not set, SeaTunnel reads all collections in the configured database. The deprecated `collection_name` key is accepted as an alias. |

## Task Example

### Read All Collections in a Database

```hocon
source {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    database = "default"
  }
}
```

### Read One Collection

```hocon
source {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    database = "default"
    collection = "book_vectors"
  }
}
```

## Changelog

<ChangeLog />
