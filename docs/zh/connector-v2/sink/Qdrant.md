# Qdrant

> Qdrant 数据连接器

[Qdrant](https://qdrant.tech/) 是一个高性能的向量搜索引擎和向量数据库。

该连接器可用于将数据写入 Qdrant 集合。

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
| batch_size      | int    | 否  | 64        |
| host            | string | 否  | localhost |
| port            | int    | 否  | 6334      |
| api_key         | string | 否  | -         |
| use_tls         | bool   | 否  | false     |
| common-options  |        | 否  | -         |

### collection_name [string]

要从中读取数据的 Qdrant 集合的名称。

### batch_size [int]

每个 upsert 请求到 Qdrant 的批量大小。

### host [string]

Qdrant 实例的主机名。默认为 "localhost"。

### port [int]

Qdrant 实例的 gRPC 端口。

### api_key [string]

用于身份验证的 API 密钥（如果设置）。

### use_tls [bool]

是否使用 TLS（SSL）连接。如果使用 Qdrant 云（https），则需要。

### 通用选项

接收插件的通用参数，请参考[源通用选项](../sink-common-options.md)了解详情。
