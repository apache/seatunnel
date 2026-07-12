import ChangeLog from '../changelog/connector-hugegraph.md';

# HugeGraph Source Connector

`Source: HugeGraph`

## 描述

HugeGraph source connector 允许你从 Apache HugeGraph 读取顶点或边数据到 SeaTunnel。

该 connector 执行有界的全标签扫描，支持服务端分页。当未提供用户自定义的 `schema` 时，connector 会从 HugeGraph 的 `PropertyKey` 定义自动推断行类型。

## 关键特性

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [support multiple table read](../../introduction/concepts/connector-v2-features.md)

## 配置选项

| 名称                | 类型    | 是否必填 | 默认值 | 描述                                                                        |
| ------------------- | ------- | -------- | ------ |----------------------------------------------------------------------------|
| `host`              | String  | 是       | -      | HugeGraph 服务器主机地址。                                                   |
| `port`              | Integer | 是       | -      | HugeGraph 服务器端口。                                                       |
| `graph_name`        | String  | 是       | -      | 要读取的图名称。                                                             |
| `graph_space`       | String  | 否       | -      | 图的图空间。                                                                 |
| `username`          | String  | 否       | -      | HugeGraph 认证用户名。                                                       |
| `password`          | String  | 否       | -      | HugeGraph 认证密码。                                                         |
| `protocol`          | String  | 否       | `http` | HugeGraph 服务器连接协议（`http` 或 `https`）。                              |
| `label`             | String  | 是       | -      | 要读取的顶点或边标签。                                                       |
| `type`              | String  | 是       | -      | 要读取的图元素类型。必须为 `VERTEX` 或 `EDGE`。                              |
| `properties`        | List    | 否       | -      | 要读取的属性名列表。未指定时读取所有属性。                                    |
| `page_size`         | Integer | 否       | 500    | 每页从 HugeGraph 获取的记录数。必须大于 0。                                  |
| `limit`             | Integer | 否       | -      | 最大读取记录数。指定时必须大于 0。                                           |
| `schema`            | Object  | 否       | -      | 用户自定义 schema。未指定时 connector 从 HugeGraph 推断 schema。            |

## 数据类型映射

当 connector 从 HugeGraph `PropertyKey` 定义推断 schema 时，使用以下映射：

| HugeGraph 类型 | SeaTunnel 类型 | 说明 |
|----------------|---------------|------|
| `BOOLEAN`      | `BOOLEAN`     |      |
| `INT`          | `INT`         |      |
| `LONG`         | `LONG`        |      |
| `FLOAT`        | `FLOAT`       |      |
| `DOUBLE`       | `DOUBLE`      |      |
| `DATE`         | `LOCAL_DATE`  | 以 UTC 返回 `LocalDate`。HugeGraph 服务器可能以空格分隔字符串返回 DATE，connector 内部处理。 |
| `UUID`         | `STRING`      |      |
| `TEXT`         | `STRING`      |      |
| `BLOB`         | `STRING`      |      |
| `LIST` / `SET` | `ARRAY<T>`    | `T` 为映射后的基础类型。`SINGLE` 基数属性直接映射。 |

## Schema 推断规则

- 顶点行始终包含 `id`（`STRING`）和 `label`（`STRING`）作为前两个字段，其后是标签的属性（按声明顺序）。
- 边行始终包含 `id`（`STRING`）、`label`（`STRING`）、`source_id`（`STRING`）和 `target_id`（`STRING`）作为前四个字段，其后是标签的属性。
- 所有标识字段（`id`、`source_id`、`target_id`）在运行时均归一化为 `STRING`，以匹配推断的 schema。

## 使用示例

### 1. 使用推断 Schema 读取顶点

```hocon
env {
  job.mode = "BATCH"
}

source {
  HugeGraph {
    host = "localhost"
    port = 8080
    graph_name = "hugegraph"
    label = "person"
    type = "VERTEX"
  }
}

sink {
  Console {}
}
```

### 2. 使用用户自定义 Schema 读取边

```hocon
env {
  job.mode = "BATCH"
}

source {
  HugeGraph {
    host = "localhost"
    port = 8080
    graph_name = "hugegraph"
    label = "knows"
    type = "EDGE"
    properties = ["since"]
    page_size = 1000
    limit = 10000
    schema = {
      fields = {
        id = "string"
        label = "string"
        source_id = "string"
        target_id = "string"
        since = "int"
      }
    }
  }
}

sink {
  Console {}
}
```

### 3. HTTPS 连接

```hocon
source {
  HugeGraph {
    host = "hugegraph.example.com"
    port = 8443
    protocol = "https"
    graph_name = "hugegraph"
    username = "admin"
    password = "secret"
    label = "person"
    type = "VERTEX"
  }
}
```

## 更新日志

<ChangeLog />
