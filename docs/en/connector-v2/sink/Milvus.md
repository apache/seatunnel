import ChangeLog from '../changelog/connector-milvus.md';

# Milvus

> Milvus sink connector

## Description

This Milvus sink connector write data to Milvus or Zilliz Cloud, it has the following features:
- support read and write data by partition
- support write dynamic schema data from Metadata Column
- json data will be converted to json string and sink as json as well
- retry automatically to bypass ratelimit and grpc limit
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

## Sink Options

| Name                   | Type                | Required | Default                      | Description                                                                                                                                         |
|------------------------|---------------------|----------|------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| url                    | String              | Yes      | -                            | The URL to connect to Milvus or Zilliz Cloud.                                                                                                       |
| token                  | String              | Yes      | -                            | User:password                                                                                                                                       |
| database               | String              | No       | -                            | Write data to which database, default is source database.                                                                                           |
| schema_save_mode       | enum                | No       | CREATE_SCHEMA_WHEN_NOT_EXIST | Auto create table when table not exist.                                                                                                             |
| enable_auto_id         | boolean             | No       | false                        | Primary key column enable autoId.                                                                                                                   |
| enable_upsert          | boolean             | No       | false                        | Upsert data not insert.                                                                                                                             |
| enable_dynamic_field   | boolean             | No       | true                         | Enable create table with dynamic field.                                                                                                             |
| batch_size             | int                 | No       | 1000                         | Write batch size. When the number of buffered records reaches `batch_size` or the time reaches `checkpoint.interval`, it will trigger a write flush |
| partition_key          | String              | No       |                              | Milvus partition key field                                                                                                                          |
| create_index           | boolean             | No       | false                        | Automatically create vector indexes for collection to improve query performance.                                                                    |
| load_collection        | boolean             | No       | false                        | Load collection into Milvus memory for immediate query availability.                                                                                |
| collection_description | Map<String, String> | No       | {}                           | Collection descriptions map where key is collection name and value is description.                                                                  |                                         

## Task Example

### Basic Configuration
```bash
sink {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    batch_size = 1000
  }
}
```

### Advanced Configuration with Index and Loading
```bash
sink {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    batch_size = 1000
    create_index = true
    load_collection = true
    collection_description = {
      "user_vectors" = "User embedding vectors for recommendation"
      "product_vectors" = "Product feature vectors for search"
    }
  }
}
```

## Changelog

<ChangeLog />