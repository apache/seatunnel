import ChangeLog from '../changelog/connector-hugegraph.md';

# HugeGraph Source Connector

`Source: HugeGraph`

## 描述

HugeGraph Source Connector 通过 HugeGraph REST API 读取 Apache HugeGraph 图数据。

对一个顶点标签或一个边标签执行有界扫描，并保存读取进度以便作业在故障后恢复。

- 当 `parallelism = 1` 时，通过服务端 list API 分页读取该 label，按 HugeGraph page-marker 读取到服务端返回 `page = null` 为止；此模式支持服务端 `filter`（属性等值过滤）。
- 当 `parallelism > 1` 时，通过 HugeGraph `traverser().vertexShards / edgeShards` API 将 keyspace 切分为多个 shard，由多个 Reader 并行扫描。由于 shard 扫描按 key-range 返回所有 label，connector 会在客户端按配置的 `label` 过滤。详见[并行读取](#并行读取)。

## 主要特性

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)

## 参数

| 名称               | 类型    | 是否必填 | 默认值   | 描述 |
|--------------------|---------|----------|----------|------|
| `host`             | String  | 是       | -        | HugeGraph 服务地址。 |
| `port`             | Integer | 是       | -        | HugeGraph 服务端口。 |
| `protocol`         | String  | 否       | `http`   | 服务协议，支持 `http`、`https`。HTTPS 使用 JVM trust store。 |
| `graph_name`       | String  | 是       | -        | HugeGraph 图名称。 |
| `label`            | String  | 是       | -        | 要读取的顶点标签或边标签。 |
| `schema`           | Object  | 否       | -        | 通过 `schema.fields` 声明输出属性列。保留图字段由 connector 自动添加。**省略时，connector 会从服务端读取 `label` 定义并自动发现全部属性列（类型自动推断，列按名称排序）。** 详见[Schema 自动发现](#schema-自动发现)。 |
| `label_type`       | Enum    | 否       | `VERTEX` | 标签类型，支持 `VERTEX`、`EDGE`。 |
| `page_size`        | Integer | 否       | `1000`   | 每页读取记录数，取值范围为 `[100, 10000]`。 |
| `split_size`       | Long    | 否       | `1048576` | `parallelism > 1` 时每个 key-range shard 的目标字节大小。值越大 shard 越少越大。`parallelism = 1` 时忽略。需要支持 scan 的后端（RocksDB / HBase / Cassandra）。 |
| `filter`           | Map     | 否       | -        | 可选的服务端属性等值过滤条件，例如 `{ country = "US", active = "true" }`。仅返回所有条件均匹配的元素。每个 key 必须是 `label` 的属性，未知 key 会在启动时报错。省略时读取该 label 的全部元素。**不能与 `parallelism > 1` 同时使用**（shard 扫描无法把属性过滤下推到服务端），两者同时设置会在启动时报错。 |
| `time_zone`        | String  | 否       | Worker JVM 默认时区 | HugeGraph DATE epoch 值转换使用的 ZoneId，例如 `UTC` 或 `Asia/Shanghai`。Worker JVM 时区可能不一致时应显式设置。 |
| `graph_space`      | String  | 否       | `DEFAULT` | 图所属的图空间（graph space）。 |
| `username`         | String  | 否       | -        | HugeGraph 用户名。 |
| `password`         | String  | 否       | -        | HugeGraph 密码。 |
| `max_retries`      | Integer | 否       | `3`      | 首次请求失败后的重试次数。设置为 `0` 可禁用重试。 |
| `retry_backoff_ms` | Integer | 否       | `5000`   | 重试的基础退避时间（毫秒），按尝试次数指数增长（`retry_backoff_ms * 2^(attempt-1)`），上限为 `retry_backoff_max_ms`。 |
| `retry_backoff_max_ms` | Integer | 否   | `30000`  | 指数退避的上限（毫秒）。 |

## 输出 Schema

顶点输出列：

```text
~id, ~label, <schema.fields columns...>
```

边输出列：

```text
~id, ~label, ~source_id, ~source_label, ~target_id, ~target_label, <schema.fields columns...>
```

`~` 前缀字段为 connector 自动添加的保留字段。HugeGraph 属性键不能以 `~` 开头，因此不会与用户属性冲突。

## 类型映射

`schema.fields` 中声明的类型必须与 HugeGraph PropertyKey 类型匹配。Connector 会在读取前校验。

| HugeGraph 类型 | SeaTunnel 类型 |
|----------------|----------------|
| `TEXT`         | `STRING`       |
| `INT`          | `INT`          |
| `LONG`         | `BIGINT`       |
| `FLOAT`        | `FLOAT`        |
| `DOUBLE`       | `DOUBLE`       |
| `BOOLEAN`      | `BOOLEAN`      |
| `DATE`         | `TIMESTAMP`    |
| `UUID`         | `STRING`       |
| `BLOB`         | `BYTES`        |

### 多值（LIST / SET）属性

cardinality 为 `LIST` 或 `SET` 的 HugeGraph 属性会被读为 SeaTunnel 的 `ARRAY`。在 `schema.fields` 中声明为 `array<T>`，其中 `T` 是元素的 SeaTunnel 类型（见上表）。例如名为 `tags` 的 `LIST<TEXT>` 属性声明为 `tags = "array<string>"`。

注意：

- `SET` 元素在服务端无固定顺序；需要顺序时请使用 `LIST`。
- 若服务端某属性 cardinality 为 `LIST`/`SET` 却被声明为标量（或反之），作业会在启动时失败并提示正确的声明方式。
- 不支持 `LIST`/`SET` 中嵌套 `BLOB` 元素。

## 示例

```hocon
source {
  HugeGraph {
    host = "localhost"
    port = 8080
    graph_name = "hugegraph"
    label = "person"
    label_type = "VERTEX"
    page_size = 1000
    schema = {
      fields = {
        name = "string"
        age = "int"
      }
    }
  }
}
```

## Schema 自动发现

`schema` 为可选项。省略时，connector 会在作业构建阶段连接服务端，读取 `label` 的定义，为每个属性键生成一个输出列（类型见[类型映射](#类型映射)表，`LIST`/`SET` 映射为 `array<T>`），并按属性名排序。适合「整 label 全字段 dump」而又不想逐字段手写声明的场景。

```hocon
source {
  HugeGraph {
    host = "localhost"
    port = 8080
    graph_name = "hugegraph"
    label = "person"
    label_type = "VERTEX"
    # 不写 schema：读取 "person" 的全部属性
  }
}
```

注意：

- 该 label 必须已存在于服务端，否则作业在构建阶段失败。
- 无任何属性键的 label 只会产生保留列（`~id`、`~label` 等）。
- 当只想读取部分属性、固定列顺序或指定类型时，请显式声明 `schema.fields`。

## 并行读取

对于大图，设置 `parallelism > 1` 可并行读取一个 label。Enumerator 请求 HugeGraph 将该 label 的 keyspace 切分为大小约为 `split_size` 字节的多个 shard，并以 round-robin 方式分配给各 Reader，使吞吐随并行度提升，而不再受单一分页游标限制。

```hocon
source {
  HugeGraph {
    host = "localhost"
    port = 8080
    graph_name = "hugegraph"
    label = "person"
    label_type = "VERTEX"
    parallelism = 8
    split_size = 1048576
    schema = {
      fields = {
        name = "string"
        age = "int"
      }
    }
  }
}
```

注意：

- Shard 扫描需要支持 scan 的后端（RocksDB / HBase / Cassandra）；`memory` 后端不支持 shard 切分，请在其上使用 `parallelism = 1`。
- Shard 扫描会返回 key-range 内所有 label 的元素，connector 仅保留配置的 `label`。当目标 label 只占全图很小比例时，单并行度的 `filter` 读取可能搬运更少数据（尽管无法并行）。
- `filter` 不能与 `parallelism > 1` 同时使用；要用服务端过滤请保持 `parallelism = 1`，要并行请去掉 filter。
- 调优 `split_size`：值越小 shard 越多越小（负载更均衡、请求更多）；值越大 shard 越少越大。

## Changelog

<ChangeLog />
