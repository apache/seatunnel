# Qdrant

> Qdrant 数据源连接器

[Qdrant](https://qdrant.tech/) 是一个高性能的向量搜索引擎和向量数据库。

该连接器可用于从 Qdrant 集合中读取数据。

## 选项

|       名称        |   类型   | 必填 |    默认值    |
|-----------------|--------|----|-----------|
| collection_name | string | 是  | -         |
| schema          | config | 是  | -         |
| host            | string | 否  | localhost |
| port            | int    | 否  | 6334      |
| api_key         | string | 否  | -         |
| use_tls         | bool   | 否  | false     |
| common-options  |        | 否  | -         |

### collection_name [string]

要从中读取数据的 Qdrant 集合的名称。

### schema [config]

要将数据读取到的表的模式。

例如：

```hocon
schema = {
  fields {
    age = int
    address = string
    some_vector = float_vector
  }
}
```

Qdrant 中的每个条目称为一个点。

`float_vector` 类型的列从每个点的向量中读取，其他列从与该点关联的 JSON 有效负载中读取。

如果列被标记为主键，Qdrant 点的 ID 将写入其中。它可以是 `"string"` 或 `"int"` 类型。因为 Qdrant 仅[允许](https://qdrant.tech/documentation/concepts/points/#point-ids)使用正整数和 UUID 作为点 ID。

如果集合是用单个默认/未命名向量创建的，请使用 `default_vector` 作为向量名称。

```hocon
schema = {
  fields {
    age = int
    address = string
    default_vector = float_vector
  }
}
```

Qdrant 中点的 ID 将写入标记为主键的列中。它可以是 `int` 或 `string` 类型。

### host [string]

Qdrant 实例的主机名。默认为 "localhost"。

### port [int]

Qdrant 实例的 gRPC 端口。

### api_key [string]

用于身份验证的 API 密钥（如果设置）。

### use_tls [bool]

是否使用 TLS（SSL）连接。如果使用 Qdrant 云（https），则需要。

### 通用选项

源插件的通用参数，请参考[源通用选项](../source-common-options.md)了解详情。****
