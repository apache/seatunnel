import ChangeLog from '../changelog/connector-hugegraph.md';

# HugeGraph Source Connector

`Source: HugeGraph`

## 描述

HugeGraph Source Connector 通过 HugeGraph REST API 读取 Apache HugeGraph 图数据。

V1 支持有界的全量标签扫描，使用单 Reader 读取一个顶点标签或一个边标签，在分页边界保存 checkpoint，并通过 HugeGraph page-marker 读取到服务端返回 `page = null` 为止。

## 主要特性

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)

## 参数

| 名称               | 类型    | 是否必填 | 默认值   | 描述 |
|--------------------|---------|----------|----------|------|
| `host`             | String  | 是       | -        | HugeGraph 服务地址。 |
| `port`             | Integer | 是       | -        | HugeGraph 服务端口。 |
| `protocol`         | String  | 否       | `http`   | 服务协议，支持 `http`、`https`。HTTPS 使用 JVM trust store。 |
| `graph_name`       | String  | 是       | -        | HugeGraph 图名称。 |
| `label`            | String  | 是       | -        | 要读取的顶点标签或边标签。 |
| `schema`           | Object  | 是       | -        | 通过 `schema.fields` 声明输出属性列。保留图字段由 connector 自动添加。 |
| `label_type`       | Enum    | 否       | `VERTEX` | 标签类型，支持 `VERTEX`、`EDGE`。 |
| `page_size`        | Integer | 否       | `1000`   | 每页读取记录数，取值范围为 `[100, 10000]`。 |
| `time_zone`        | String  | 否       | Worker JVM 默认时区 | HugeGraph DATE epoch 值转换使用的 ZoneId，例如 `UTC` 或 `Asia/Shanghai`。Worker JVM 时区可能不一致时应显式设置。 |
| `graph_space`      | String  | 否       | -        | 当前 HugeGraph client 依赖不支持该参数。设置该参数时 connector 会 fail-fast。 |
| `username`         | String  | 否       | -        | HugeGraph 用户名。 |
| `password`         | String  | 否       | -        | HugeGraph 密码。 |
| `max_retries`      | Integer | 否       | `3`      | 首次请求失败后的重试次数。设置为 `0` 可禁用重试。 |
| `retry_backoff_ms` | Integer | 否       | `5000`   | 重试间隔，单位毫秒。 |

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

## Changelog

<ChangeLog />
