# Milvus

> Milvus source connector

## Description

Read data from Milvus or Zilliz Cloud

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)

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

|    Name    |  Type  | Required | Default |                                        Description                                         |
|------------|--------|----------|---------|--------------------------------------------------------------------------------------------|
| url        | String | Yes      | -       | The URL to connect to Milvus or Zilliz Cloud.                                              |
| token      | String | Yes      | -       | User:password                                                                              |
| database   | String | Yes      | default | Read data from which database.                                                             |
| collection | String | No       | -       | If set, will only read one collection, otherwise will read all collections under database. |

## Task Example

```bash
source {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    database = "default"
  }
}
```

