# 类型与 Schema 常见问题

本页回答跨连接器的 SeaTunnel schema、源端类型映射、format 解析问题。精确类型映射仍以具体
Source 和 Sink 页面中的类型表为准。

## 精确类型映射应该看哪里？

先看 Source 连接器页面和 Sink 连接器页面：

- Source 连接器页面说明外部数据库类型如何映射为 SeaTunnel 逻辑类型。
- 文件 Sink 页面说明 SeaTunnel 逻辑类型如何写入 ORC、Parquet 等文件格式。
- Format 页面说明 Debezium JSON、Canal JSON、Maxwell JSON、OGG JSON 等 CDC envelope 格式。

例如，MySQL JDBC Source 默认把 `BIT(1)` 和 `TINYINT(1)` 映射为 `BOOLEAN`，把 `TINYINT`
映射为 `BYTE`。MySQL Source 还提供 `int_type_narrowing`，用于控制 `TINYINT(1)` 的收窄行为。

## 没有 schema 的 JSON 或 Kafka 作业，为什么下游 Transform/Sink 会失败？

Transform 和很多 Sink 处理的是 SeaTunnel 行字段，而不是无类型 JSON 文本。如果希望把 JSON
对象成员作为列使用，需要配置 Source `schema`，或选择要求 schema 的 format。

常见模式：

- Kafka `format = json`：当下游需要命名列时配置 `schema`。
- Kafka `format = debezium_json`：同时配置 `schema` 和 `debezium_record_include_schema`。
- Kafka `format = text`：先把整条消息保留为文本字段，后续需要字段时再用 `JsonPath` 抽取。
- HTTP `format = json`：配置 `schema`；如果只需要嵌套片段，用 `content_field` 或 `json_field`。

如果其他系统的配置示例里出现 `num_as_string` 这类参数，除非 SeaTunnel 对应连接器页面明确
记录该参数，否则不要直接搬到 SeaTunnel。SeaTunnel 中应通过 `schema` 选择目标字段类型，再用
Transform 做必要的类型转换或字段重塑。

## Oracle CLOB、NCLOB 和二进制值应该如何处理？

Oracle JDBC Source 将 `CLOB`、`NCLOB` 映射为 SeaTunnel `STRING`，将 `BLOB`、`RAW`、
`LONG RAW` 等二进制类型映射为 `BYTES`。通用 JDBC Source 还提供 `handle_blob_as_string`，
用于在写入下游系统前把 Oracle BLOB 暴露为字符串。

如果下游数据库因为文本中包含 NUL 字节或其他不支持字符而拒绝写入，应把它视为目标系统约束。
请在 Sink 写入前清洗或替换该值，例如使用 SQL Transform 或自定义 Transform。

## MySQL TINYINT 写入 ORC/Parquet 后为什么看起来不一样？

链路中实际有两段映射：

1. MySQL 或 JDBC Source 类型 -> SeaTunnel 逻辑类型。
2. SeaTunnel 逻辑类型 -> 文件格式类型。

对于 S3/HDFS/OBS 文件 Sink，ORC 会把 SeaTunnel `TINYINT` 写为 ORC `BYTE`；Parquet 会把
SeaTunnel `TINYINT` 写为 Parquet `INT_8`。如果 Trino 等下游读取器展示了不同类型，请同时
核对 SeaTunnel 文件 Sink 类型表和该读取器自身对 ORC/Parquet 类型的解释。

## 类型转换应该写在 Source query 里，还是写在 Transform 里？

如果转换强依赖源数据库语义，并且希望由数据库执行，放在 Source query 中。如果转换属于
SeaTunnel 管道契约，希望在 Source 和 Sink 之间保持可见，放在 Transform 中更清晰。

对于 CDC 作业，避免使用会丢失主键或 schema 元信息的 SQL 表达式，除非你已经确认下游 Sink
不再依赖这些信息来做 upsert、delete、自动建表或 schema evolution。
