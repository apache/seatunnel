# Milvus

> Milvus水槽连接器

## 描述

这个Milvus sink连接器将数据写入Milvus或Zilliz Cloud，它具有以下功能：
- 支持按分区读写数据
- 支持从元数据列写入动态模式数据
- json数据将转换为json字符串，并将sink转换为json
- 自动重试以绕过速率限制和grpc限制
## 主要特性

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)

##数据类型映射

|  Milvus数据类型   | SeaTunnel 数据类型 |
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

## Sink 选项

|         名字         | 类型    | 需要 |           默认            | 描述                                               |
|----------------------|---------|----------|------------------------------|-----------------------------------------------------------|
| url                  | String  | 是      | -                            | The URL to connect to Milvus or Zilliz Cloud.             |
| token                | String  | 是      | -                            | User:password                                             |
| database             | String  | 否       | -                            | Write data to which database, default is source database. |
| schema_save_mode     | enum    | 否       | CREATE_SCHEMA_WHEN_NOT_EXIST | Auto create table when table not exist.                   |
| enable_auto_id       | boolean | 否       | false                        | Primary key column enable autoId.                         |
| enable_upsert        | boolean | 否       | false                        | Upsert data not insert.                                   |
| enable_dynamic_field | boolean | 否       | true                         | Enable create table with dynamic field.                   |
| batch_size           | int     | 否       | 1000                         | Write batch size.                                         |
| partition_key        | String  | 否       |                              | Milvus partition key field                                |                                         

## 任务示例

```bash
sink {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    batch_size = 1000
  }
}
```

