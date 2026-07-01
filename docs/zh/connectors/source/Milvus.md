import ChangeLog from '../changelog/connector-milvus.md';

# Milvus

> Milvus 源连接器

## 描述

Milvus Source 连接器用于从 Milvus 或 Zilliz Cloud 读取数据。它可以读取单个集合，
也可以读取一个数据库下的全部集合，并保留向量字段为 SeaTunnel 的向量类型。

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)

## 数据类型映射

|  Milvus 数据类型   | SeaTunnel 数据类型 |
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

## 源选项

| 名称         | 类型     | 必填 | 默认值     | 描述                                                                 |
|------------|--------|----|---------|--------------------------------------------------------------------|
| url        | String | 是  | -       | 连接 Milvus 或 Zilliz Cloud 的地址。                                      |
| token      | String | 是  | -       | Milvus 认证信息，通常是 `username:password` 格式。                           |
| database   | String | 否  | default | 源数据库。                                                              |
| collection | String | 否  | -       | 源集合。不配置时，SeaTunnel 会读取该数据库下的全部集合。旧配置键 `collection_name` 仍兼容。 |

## 任务示例

### 读取数据库下的全部集合

```hocon
source {
  Milvus {
    url = "http://127.0.0.1:19530"
    token = "username:password"
    database = "default"
  }
}
```

### 读取单个集合

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

## 变更日志

<ChangeLog />
