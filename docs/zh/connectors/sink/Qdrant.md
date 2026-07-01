import ChangeLog from '../changelog/connector-qdrant.md';

# Qdrant

> Qdrant 数据连接器

[Qdrant](https://qdrant.tech/) 是一个高性能的向量搜索引擎和向量数据库。

该连接器可用于将数据写入 Qdrant 集合。

## 数据类型映射

|   SeaTunnel 数据类型    |  Qdrant 数据类型  |
|---------------------|---------------|
| SMALLINT            | INTEGER       |
| INT                 | INTEGER       |
| BIGINT              | INTEGER       |
| FLOAT               | DOUBLE        |
| DOUBLE              | DOUBLE        |
| BOOLEAN             | BOOL          |
| STRING              | STRING        |
| DATE                | STRING        |
| FLOAT_VECTOR        | DENSE_VECTOR  |
| BINARY_VECTOR       | DENSE_VECTOR  |
| FLOAT16_VECTOR      | DENSE_VECTOR  |
| BFLOAT16_VECTOR     | DENSE_VECTOR  |

主键列的值将用作 Qdrant 中的点 ID。主键支持 `INT` 类型的数字 ID，以及 `STRING` 类型的 UUID ID。如果没有主键，则将使用随机 UUID。

非向量字段会按同名字段写入 Qdrant payload。向量字段会按同名字段写入 Qdrant named vector，所以目标集合中需要提前定义好同名向量，并保证维度一致。

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

下面的示例会把 `file_name`、`file_size` 两个 payload 字段和 `my_vector` 这个向量字段写入 Qdrant。

运行任务前，请先创建目标 Qdrant 集合，并定义一个名为 `my_vector`、维度为 `4` 的向量。

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
