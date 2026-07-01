import ChangeLog from '../changelog/connector-milvus.md';

# Milvus

> Milvus数据接收器

## 描述

Milvus sink 连接器用于把数据写入 Milvus 或 Zilliz Cloud。它可以根据上游
SeaTunnel 表结构创建目标集合，并支持向量字段、动态字段、分区键、分区元数据，以及
遇到限流或 gRPC 限制时自动重试。

常见用法：

- 写入 `FLOAT_VECTOR`、`BINARY_VECTOR`、`FLOAT16_VECTOR`、`BFLOAT16_VECTOR` 和 `SPARSE_FLOAT_VECTOR` 字段。
- 目标集合不存在时自动创建集合。
- 从一个 Milvus 集合复制数据到另一个 Milvus 集合。
- 上游 Milvus source 携带分区信息时，在目标集合保留分区。
- 按需创建向量索引，并在写入后加载集合。

## 主要特性

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
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

| 名字                     | 类型                  | 是否必传 | 默认值                          | 描述                                                                                      |
|------------------------|---------------------|------|------------------------------|-----------------------------------------------------------------------------------------|
| url                    | String              | 是    | -                            | Milvus 或 Zilliz Cloud 的连接地址，例如 `http://127.0.0.1:19530`。                             |
| token                  | String              | 是    | -                            | Milvus 认证 token。本地 Milvus 通常使用 `username:password`。                                   |
| database               | String              | 否    | -                            | 目标数据库。不填时，如果上游带有数据库信息，则使用上游数据库。                                                    |
| collection             | String              | 否    | -                            | 目标集合。不填时使用上游表名作为集合名。兼容旧配置键 `collection_name`。                                      |
| schema_save_mode       | enum                | 否    | CREATE_SCHEMA_WHEN_NOT_EXIST | 控制如何处理目标集合结构。默认值表示集合不存在时才创建集合。                                                   |
| data_save_mode         | enum                | 否    | APPEND_DATA                  | 控制如何处理已有数据。支持 `DROP_DATA`、`APPEND_DATA`、`ERROR_WHEN_DATA_EXISTS`。                         |
| enable_auto_id         | boolean             | 否    | false                        | 是否让 Milvus 自动生成主键。设置为 `true` 时，不要再向主键字段写值。                                          |
| enable_upsert          | boolean             | 否    | true                         | 是否使用 upsert 写入，而不是普通 insert。upsert 需要集合中有主键。                                        |
| enable_dynamic_field   | boolean             | 否    | true                         | SeaTunnel 创建集合时，是否启用 Milvus 动态字段。                                                       |
| batch_size             | int                 | 否    | 1000                         | 写入前缓存的记录数。流任务中，checkpoint 触发时也会刷新写入。                                                |
| rate_limit             | int                 | 否    | 100000                       | 连接器用于重试和限流控制的最大写入速率。                                                                 |
| partition_key          | String              | 否    | -                            | SeaTunnel 创建集合时使用的 Milvus 分区键字段名。                                                       |
| create_index           | boolean             | 否    | false                        | 是否为目标集合创建向量索引。从 Milvus source 复制时，可以利用上游索引元数据在目标集合创建相同索引。                         |
| load_collection        | boolean             | 否    | false                        | 创建集合后是否把目标集合加载到 Milvus 内存中。如果写完马上要查询，可以开启。                                        |
| collection_description | Map<String, String> | 否    | {}                           | 集合描述映射。key 是集合名，value 是创建集合时写入的描述。                                                  |

## 注意事项

- 向量维度来自 SeaTunnel 表结构。使用 `FakeSource` 生成向量字段时，`columnScale` 表示向量维度。
- 不配置 `collection` 时，sink 会使用上游表名作为 Milvus 集合名，适合多表写入。
- Milvus source 读取分区后，如果目标集合没有使用分区键，sink 可以在目标集合创建相同分区名。
- `create_index = true` 只有在上游 catalog 能提供索引元数据时才会创建向量索引，例如上游也是 Milvus。

## 任务示例

### 写入 SeaTunnel 表结构到 Milvus

这个示例会把 10 行数据写入 `test1.simple_example_1` 集合。因为没有配置
`collection`，sink 会使用 source 的表名作为集合名。

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

### 写入多种向量类型

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

### 复制一个 Milvus 集合并创建索引

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

### 流式写入并通过 checkpoint 刷新

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

## 变更日志

<ChangeLog />
