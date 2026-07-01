import ChangeLog from '../changelog/connector-qdrant.md';

# Qdrant

> Qdrant 数据连接器

[Qdrant](https://qdrant.tech/) 是一个高性能的向量搜索引擎和向量数据库。

该连接器可用于将数据写入 Qdrant 集合。

目标 collection 必须在作业启动前已经存在。Qdrant 中的向量字段名和维度需要与 SeaTunnel 数据行中的向量列保持一致。

## 数据类型映射

|   SeaTunnel 数据类型    |  Qdrant 数据类型  |
|---------------------|---------------|
| TINYINT             | INTEGER       |
| SMALLINT            | INTEGER       |
| INT                 | INTEGER       |
| BIGINT              | INTEGER       |
| FLOAT               | DOUBLE        |
| DOUBLE              | DOUBLE        |
| BOOLEAN             | BOOL          |
| STRING              | STRING        |
| ARRAY               | LIST          |
| FLOAT_VECTOR        | DENSE_VECTOR  |
| BINARY_VECTOR       | DENSE_VECTOR  |
| FLOAT16_VECTOR      | DENSE_VECTOR  |
| BFLOAT16_VECTOR     | DENSE_VECTOR  |
| SPARSE_FLOAT_VECTOR | SPARSE_VECTOR |

主键列的值将用作 Qdrant 中的点 ID。如果没有主键，则将使用随机 UUID。

## 选项

|       名称        |   类型   | 必填 |    默认值    |
|-----------------|--------|----|-----------|
| collection_name | string | 是  | -         |
| host            | string | 否  | localhost |
| port            | int    | 否  | 6334      |
| api_key         | string | 否  | -         |
| use_tls         | bool   | 否  | false     |
| common-options  |        | 否  | -         |

### collection_name [string]

要写入数据的 Qdrant 集合的名称。

### host [string]

Qdrant 实例的主机名。默认为 "localhost"。

### port [int]

Qdrant 实例的 gRPC 端口。

### api_key [string]

用于身份验证的 API 密钥（如果设置）。

### use_tls [bool]

是否使用 TLS（SSL）连接。如果使用 Qdrant 云（https），则需要。

### 通用选项

Sink插件通用参数，请参考[Sink通用选项](../common-options/sink-common-options.md)了解详情。

## 任务示例

下面的示例会把一个 Qdrant source collection 中的记录写入另一个 Qdrant collection。`file_name`、`file_size` 等普通字段会写成 point payload，`my_vector` 会写成命名向量。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Qdrant {
    collection_name = "source_collection"
    host = "localhost"
    port = 6334
    schema = {
      columns = [
        {
          name = file_name
          type = string
        }
        {
          name = file_size
          type = int
        }
        {
          name = my_vector
          type = float_vector
        }
      ]
    }
  }
}

sink {
  Qdrant {
    collection_name = "sink_collection"
    host = "localhost"
    port = 6334
  }
}
```

## 变更日志

<ChangeLog />
