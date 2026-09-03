# Transform FAQ

This page answers common transform configuration questions that span several transform plugins.
For full option tables, use the linked plugin pages as the source of truth.

## Which transform should I use for field changes?

| Goal | Recommended transform | Notes |
|---|---|---|
| Rename, reorder, or drop fields | [`FieldMapper`](./field-mapper.md) | Maps input field names to output field names. Fields not listed in `field_mapper` are removed from the output schema. |
| Rename fields only | [`FieldRename`](./field-rename.md) | Use when the field set and order should otherwise stay the same. |
| Copy one field into another field | [`Copy`](./copy.md) | Keeps the source field and adds or overwrites the destination field, depending on the configuration. |
| Compute a new field from an expression | [`Sql`](./sql.md) | Use `SELECT` expressions and SQL functions such as `UUID()`. |
| Extract fields from JSON stored in one column | [`JsonPath`](./jsonpath.md) | The source row must contain a STRING/BYTES field that holds the JSON payload. |

The transform output schema is passed to the downstream sink. It does not by itself create or alter
the target system schema unless the sink supports the required save mode or schema evolution feature.

## Can SQL Transform generate a UUID?

Yes. SQL Transform supports the `UUID()` function:

```hocon
transform {
  Sql {
    plugin_input = "source_table"
    plugin_output = "with_uuid"
    query = "select UUID() as id, name, age from source_table"
  }
}
```

See [`SQL Functions`](./sql-functions.md#uuid) for the function reference.

## Why does my SQL Transform fail after an HTTP or Kafka source?

SQL Transform queries a SeaTunnel row schema. The source must therefore output named fields that the
SQL query can reference.

- `Http` with `format = json` should define `schema` so the response becomes named columns.
- `Http` with `format = text` outputs a text payload column; use `JsonPath` first if you need to
  extract nested JSON fields before SQL.
- `Kafka` with `format = text` outputs the message value as a text field, while `format = NATIVE`
  exposes Kafka metadata such as `key`, `value`, `partition`, and `timestamp`.
- `Kafka` with `format = debezium_json` must define the table `schema` and
  `debezium_record_include_schema` according to the Debezium message envelope.

When a source emits multiple upstream tables, use [`Multi-Table Transform`](./transform-multi-table.md)
options such as `table_match_regex` or `table_transform` to scope the transform to the intended
table.

## Can SQL Transform join two CDC tables?

No. SQL Transform is for transforming the rows it receives from one input table stream. It does not
provide a stateful streaming join between two CDC tables or two different sources.

For supported multi-table patterns and alternatives, see
[`Multi-Table Transform Capability Boundary`](./multi-table-transform-and-join-boundary.md).

## How should I use date variables such as month partitions?

SeaTunnel job variables use `${name}` syntax and are passed by `-i name=value`,
`--variable name=value`, or environment variables. For example:

```hocon
source {
  LocalFile {
    path = "/data/orders/${biz_month}/"
  }
}
```

Then submit with:

```bash
sh bin/seatunnel.sh --config job.conf -e local -i biz_month=2026-07
```

Expressions such as `$[yyyy-MM]` are scheduler expressions, not SeaTunnel job-variable syntax. If
your scheduler supports that syntax, render it in the scheduler first and pass the rendered value
to SeaTunnel as `${biz_month}`.

File sinks have an additional filename feature: when `custom_filename = true`, `file_name_expression`
can use `${now}` and `${uuid}`, and `filename_time_format` controls the `${now}` format.
