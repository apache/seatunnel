import ChangeLog from '../changelog/connector-hugegraph.md';

# HugeGraph Source Connector

`Source: HugeGraph`

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

HugeGraph Source Connector 通过 HugeGraph REST API 读取 Apache HugeGraph 图数据。

对一个顶点标签或一个边标签执行有界扫描——或在单个作业中读取某一类型的**全部** label——并保存读取进度以便作业在故障后恢复。

- 当 `parallelism = 1` 时，通过服务端 list API 分页读取该 label，按 HugeGraph page-marker 读取到服务端返回 `page = null` 为止；此模式支持服务端 `filter`（属性等值过滤）。
- 当 `parallelism > 1` 时，通过 HugeGraph `traverser().vertexShards / edgeShards` API 将 keyspace 切分为多个 shard，由多个 Reader 并行扫描。由于 shard 扫描按 key-range 返回所有 label，connector 会在客户端按配置的 `label` 过滤。详见[并行读取](#并行读取)。
- 省略 `label` 时，在单个作业中读取 `label_type`（默认 `VERTEX`）下的全部 label，每个 label 产出一张输出表。详见[读取全部 label](#读取全部-label)。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [变更数据捕获](../../introduction/concepts/connector-v2-features.md)

## 参数

| 名称               | 类型    | 是否必填 | 默认值   | 描述 |
|--------------------|---------|----------|----------|------|
| `host`             | String  | 是       | -        | HugeGraph 服务地址。 |
| `port`             | Integer | 是       | -        | HugeGraph 服务端口。 |
| `protocol`         | String  | 否       | `http`   | 服务协议，支持 `http`、`https`。HTTPS 使用 JVM trust store。 |
| `graph_name`       | String  | 是       | -        | HugeGraph 图名称。 |
| `label`            | String  | 否       | -        | 要读取的顶点标签或边标签。**省略时，connector 会在单个作业中读取 `label_type` 下的全部 label，每个 label 产出一张表**（详见[读取全部 label](#读取全部-label)）；该模式下不允许配置 `schema` 与 `filter`。 |
| `schema`           | Object  | 否       | -        | 通过 `schema.fields` 声明输出属性列。保留图字段由 connector 自动添加。**省略时，connector 会从服务端读取 `label` 定义并自动发现全部属性列（类型自动推断，列按名称排序）。** 详见[Schema 自动发现](#schema-自动发现)。 |
| `label_type`       | Enum    | 否       | `VERTEX` | 标签类型，支持 `VERTEX`、`EDGE`。 |
| `page_size`        | Integer | 否       | `1000`   | 每页读取记录数，取值范围为 `[100, 10000]`。 |
| `split_size`       | Long    | 否       | `1048576` | `parallelism > 1` 时每个 key-range shard 的目标字节大小。值越大 shard 越少越大。必须不小于 `1048576`（1 MiB，HugeGraph 的最小分片大小）——更小的值会在启动时被拒绝，以避免 shard 爆炸。`parallelism = 1` 时忽略。需要支持 scan 的后端（RocksDB / HBase / Cassandra）。 |
| `filter`           | Map     | 否       | -        | 可选的服务端属性等值过滤条件，例如 `{ country = "US", active = "true" }`。仅返回所有条件均匹配的元素。每个 key 必须是 `label` 的属性（未知 key 会在启动时报错），且每个值会被转换为该属性的类型（例如 `"true"` → 布尔、`"7"` → 对应数值类型）以便与服务端匹配——无法转换的值会在启动时报错，而不是静默返回 0 行。省略时读取该 label 的全部元素。**不能与 `parallelism > 1` 同时使用**（shard 扫描无法把属性过滤下推到服务端），两者同时设置会在启动时报错。 |
| `time_zone`        | String  | 否       | Worker JVM 默认时区 | 用于转换服务端以 epoch/Date 返回的 HugeGraph DATE 值的 ZoneId，例如 `UTC` 或 `Asia/Shanghai`。对服务端已序列化为字符串（wall-clock）的 DATE 不生效（此类值不携带时区、原样保留）。Worker JVM 时区可能不一致时应显式设置。 |
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
| `BYTE`         | `TINYINT`      |
| `INT`          | `INT`          |
| `LONG`         | `BIGINT`       |
| `FLOAT`        | `FLOAT`        |
| `DOUBLE`       | `DOUBLE`       |
| `BOOLEAN`      | `BOOLEAN`      |
| `DATE`         | `TIMESTAMP`    |
| `UUID`         | `STRING`       |
| `OBJECT`       | `STRING`       |
| `BLOB`         | `BYTES`        |

### 多值（LIST / SET）属性

cardinality 为 `LIST` 或 `SET` 的 HugeGraph 属性会被读为 SeaTunnel 的 `ARRAY`。在 `schema.fields` 中声明为 `array<T>`，其中 `T` 是元素的 SeaTunnel 类型（见上表）。例如名为 `tags` 的 `LIST<TEXT>` 属性声明为 `tags = "array<string>"`。

注意：

- `SET` 元素在服务端无固定顺序；需要顺序时请使用 `LIST`。
- 若服务端某属性 cardinality 为 `LIST`/`SET` 却被声明为标量（或反之），作业会在启动时失败并提示正确的声明方式。
- 不支持 `LIST`/`SET` 中嵌套 `BLOB` 元素。

## 示例

### 显式声明 schema 读取顶点 label

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

### 读取边 label

读取边时，将 `label_type` 设为 `EDGE`，并在 `schema.fields` 中列出边的属性。输出还会包含保留列 `~source_id`、`~source_label`、`~target_id`、`~target_label`。

```hocon
source {
  HugeGraph {
    host = "localhost"
    port = 8080
    graph_name = "hugegraph"
    label = "knows"
    label_type = "EDGE"
    schema = {
      fields = {
        since = "int"
      }
    }
  }
}
```

### 使用服务端属性等值过滤

`parallelism = 1` 时，可以通过 `filter` 把属性等值条件推到服务端，只返回匹配全部条件的元素。

```hocon
source {
  HugeGraph {
    host = "localhost"
    port = 8080
    graph_name = "hugegraph"
    label = "person"
    label_type = "VERTEX"
    filter = {
      country = "US"
      active = "true"
    }
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

## 读取全部 label

省略 `label` 即可在单个作业中读取 `label_type`（默认 `VERTEX`）下的**全部** label——适合整图迁移 / 备份，而无需为每个 label 各配一个 source。作业构建阶段 connector 会从服务端 schema 列出该类型的所有 label，为每个 label 产出一张输出表，各自按 [Schema 自动发现](#schema-自动发现)推断列。每行都会带上其 label 对应的 table id，因此下游多表 sink 可据此将行路由到对应表。

```hocon
source {
  HugeGraph {
    host = "localhost"
    port = 8080
    graph_name = "hugegraph"
    label_type = "VERTEX"
    # 不写 label：读取全部顶点 label，每个 label 一张表
  }
}
```

注意：

- 一个作业读取顶点**或**边，不能混读：设置 `label_type = "EDGE"` 以读取全部边 label。
- 不允许配置 `schema`（单一 schema 无法描述多个 label），列始终按 label 自动发现。
- 不允许配置 `filter`（属性等值过滤要求该属性存在于每个 label）。
- 每个 label 对应一个 `LABEL_LIST` split，分配给各 Reader（并行度上限为 label 数量）。此模式不使用单个 label 内部的 shard 级并行。
- 若图中不存在该类型的任何 label，作业在构建阶段失败。

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
- 调优 `split_size`：值越小 shard 越多越小（负载更均衡、请求更多）；值越大 shard 越少越大。最小值为 `1048576`（1 MiB），更小的值会被拒绝，以避免把 keyspace 切分成过多的 shard。

## Changelog

<ChangeLog />
