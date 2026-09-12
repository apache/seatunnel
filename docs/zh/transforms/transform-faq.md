# Transform 常见问题

本页回答跨多个 Transform 插件的常见配置问题。完整参数表仍以各插件页面为准。

## 字段调整应该选哪个 Transform？

| 目标 | 推荐 Transform | 说明 |
|---|---|---|
| 字段重命名、重排或删除 | [`FieldMapper`](./field-mapper.md) | 把输入字段名映射为输出字段名；未写入 `field_mapper` 的字段不会进入输出 schema。 |
| 只重命名字段 | [`FieldRename`](./field-rename.md) | 适用于字段集合和顺序基本不变，只改字段名的场景。 |
| 把一个字段复制到另一个字段 | [`Copy`](./copy.md) | 保留源字段，并按配置新增或覆盖目标字段。 |
| 通过表达式计算新字段 | [`Sql`](./sql.md) | 使用 `SELECT` 表达式和 SQL 函数，例如 `UUID()`。 |
| 从单列 JSON 中抽取字段 | [`JsonPath`](./jsonpath.md) | 上游行里需要有一个 STRING/BYTES 字段保存 JSON 原文。 |

Transform 的输出 schema 会传给下游 Sink。Transform 本身不会自动在目标端建表或改表；是否能创建
或演进目标 schema，取决于下游 Sink 是否支持对应的 save mode 或 schema evolution 能力。

## SQL Transform 可以生成 UUID 吗？

可以。SQL Transform 支持 `UUID()` 函数：

```hocon
transform {
  Sql {
    plugin_input = "source_table"
    plugin_output = "with_uuid"
    query = "select UUID() as id, name, age from source_table"
  }
}
```

函数说明请参考 [`SQL Functions`](./sql-functions.md#uuid)。

## HTTP 或 Kafka Source 后面的 SQL Transform 为什么会失败？

SQL Transform 查询的是 SeaTunnel 行 schema，因此 Source 必须输出 SQL 能引用的命名字段。

- `Http` 使用 `format = json` 时，建议配置 `schema`，让响应体解析成命名列。
- `Http` 使用 `format = text` 时，输出的是文本载荷列；如果要从嵌套 JSON 中取字段，先用
  `JsonPath` 抽取后再接 SQL。
- `Kafka` 使用 `format = text` 时，输出 message value 文本字段；使用 `format = NATIVE`
  时会暴露 `key`、`value`、`partition`、`timestamp` 等 Kafka 元数据。
- `Kafka` 使用 `format = debezium_json` 时，需要根据 Debezium 消息 envelope 配置表
  `schema` 和 `debezium_record_include_schema`。

当 Source 输出多张上游表时，请使用 [`Multi-Table Transform`](./transform-multi-table.md) 的
`table_match_regex` 或 `table_transform` 等选项限定 Transform 作用的表。

## SQL Transform 可以 JOIN 两张 CDC 表吗？

不可以。SQL Transform 用于转换它接收到的单表行流，不提供两张 CDC 表之间、或两个不同 Source
之间的有状态流式 JOIN。

多表能力边界和替代方案请参考
[`多表 Transform 能力边界`](./multi-table-transform-and-join-boundary.md)。

## 月份分区这类日期变量应该怎么写？

SeaTunnel 作业变量使用 `${name}` 语法，通过 `-i name=value`、`--variable name=value` 或
环境变量传入。例如：

```hocon
source {
  LocalFile {
    path = "/data/orders/${biz_month}/"
  }
}
```

提交时传入：

```bash
sh bin/seatunnel.sh --config job.conf -e local -i biz_month=2026-07
```

`$[yyyy-MM]` 这类表达式通常是调度系统表达式，不是 SeaTunnel 作业变量语法。如果你的调度器
支持这种写法，应先由调度器渲染成具体值，再以 `${biz_month}` 的变量值传给 SeaTunnel。

文件 Sink 还有一个额外的文件名能力：当 `custom_filename = true` 时，`file_name_expression`
可以使用 `${now}` 和 `${uuid}`，其中 `${now}` 的格式由 `filename_time_format` 控制。
