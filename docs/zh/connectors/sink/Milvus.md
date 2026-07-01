import ChangeLog from '../changelog/connector-milvus.md';

# Milvus

> Milvus数据接收器

## 描述

Milvus Sink 连接器用于把数据写入 Milvus 或 Zilliz Cloud。它可以创建缺失的数据库和集合，
写入向量字段、动态字段，也可以在写入客户端初始化时创建向量索引或加载目标集合。

## 主要特性

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## 数据类型映射

| Milvus数据类型          | SeaTunnel 数据类型      |
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

| 名字                     | 类型                  | 是否必传 | 默认值                          | 描述                                                                                         |
|------------------------|---------------------|------|------------------------------|--------------------------------------------------------------------------------------------|
| url                    | String              | 是    | -                            | 连接 Milvus 或 Zilliz Cloud 的地址。                                                               |
| token                  | String              | 是    | -                            | Milvus 认证信息，通常是 `username:password` 格式。                                                   |
| database               | String              | 否    | -                            | 目标数据库。不配置时，使用上游表的数据库名。                                                                  |
| collection             | String              | 否    | -                            | 目标集合。不配置时，使用上游表名。旧配置键 `collection_name` 仍兼容，但建议使用 `collection`。                         |
| schema_save_mode       | enum                | 否    | CREATE_SCHEMA_WHEN_NOT_EXIST | 写入前如何处理目标集合结构。                                                                           |
| data_save_mode         | enum                | 否    | APPEND_DATA                  | 写入前如何处理目标已有数据。支持 `DROP_DATA`、`APPEND_DATA`、`ERROR_WHEN_DATA_EXISTS`。                         |
| enable_auto_id         | boolean             | 否    | false                        | 创建集合时是否启用 Milvus 主键 AutoID。表结构里的主键定义也可以覆盖该值。                                             |
| enable_upsert          | boolean             | 否    | true                         | 使用 Milvus upsert 写入，而不是 insert 写入。如果任务只追加新数据，可以设为 `false` 来提升写入速度。                    |
| enable_dynamic_field   | boolean             | 否    | true                         | SeaTunnel 创建集合时是否启用 Milvus 动态字段。                                                          |
| batch_size             | int                 | 否    | 1000                         | 每次写入请求前最多缓存的行数。checkpoint 也会触发刷新。                                                        |
| rate_limit             | int                 | 否    | 100000                       | 设置集合的 insert/upsert 写入限速。大于 `0` 时在 writer 打开时设置，关闭时重置。                                  |
| partition_key          | String              | 否    | -                            | 创建集合时使用的 Milvus 分区键字段。如果集合有分区键，SeaTunnel 不会再额外创建具名分区。                                  |
| create_index           | boolean             | 否    | false                        | 创建向量索引。                                                                                  |
| load_collection        | boolean             | 否    | false                        | writer 打开时，如果集合尚未加载，则将集合加载到 Milvus 内存。                                                     |
| collection_description | Map<String, String> | 否    | {}                           | 集合描述映射。key 是集合名，value 是 SeaTunnel 创建该集合时使用的描述。                                           |

### 使用说明

- 不配置 `database` 或 `collection` 时，Sink 会沿用上游表的数据库名或表名。
- 推荐使用 `collection`。`collection_name` 只是为了兼容旧配置。
- `create_index = true` 在创建集合时需要上游 schema 中带有向量索引元数据；如果集合已存在，则会为向量字段创建默认索引。
- `enable_upsert = true` 需要表结构中有有效主键。纯追加写入时，可以设置 `enable_upsert = false`。

## 任务示例

### 写入向量数据

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

### 写入多种向量类型

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

### 创建索引并加载集合

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

## 变更日志

<ChangeLog />
