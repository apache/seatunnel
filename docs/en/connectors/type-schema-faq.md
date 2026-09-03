# Type and Schema FAQ

This page answers cross-connector questions about SeaTunnel schemas, source type mapping, and format
parsing. Connector-specific type tables remain the authority for exact mappings.

## Where should I check the exact type mapping?

Start from the source connector page and the sink connector page:

- Source connector pages describe how external database types become SeaTunnel logical types.
- File sink pages describe how SeaTunnel logical types are written to formats such as ORC and
  Parquet.
- Format pages describe CDC envelope formats such as Debezium JSON, Canal JSON, Maxwell JSON, and
  OGG JSON.

For example, the MySQL JDBC source maps `BIT(1)` and `TINYINT(1)` to `BOOLEAN` by default, while
`TINYINT` maps to `BYTE`. The MySQL source also provides `int_type_narrowing` when you need to
control the `TINYINT(1)` narrowing behavior.

## Why does a schema-less JSON or Kafka job fail in a downstream transform or sink?

Transforms and many sinks work with SeaTunnel row fields, not with an untyped JSON blob. If the
source should expose JSON object members as columns, configure the source `schema` or use a format
that requires schema.

Common patterns:

- Kafka `format = json`: configure `schema` when downstream transforms or sinks need named columns.
- Kafka `format = debezium_json`: configure both `schema` and `debezium_record_include_schema`.
- Kafka `format = text`: keep the whole message as a text field and use `JsonPath` if you need to
  extract fields later.
- HTTP `format = json`: configure `schema`; use `content_field` or `json_field` when only a nested
  fragment should become the row.

If a configuration example from another system uses an option such as `num_as_string`, do not copy
that option into SeaTunnel unless the SeaTunnel connector page documents it. In SeaTunnel, choose
the intended field type in `schema`, then cast or reshape fields with Transform when needed.

## How should I handle Oracle CLOB, NCLOB, and binary values?

The Oracle JDBC source maps `CLOB` and `NCLOB` to SeaTunnel `STRING`, and binary Oracle types such
as `BLOB`, `RAW`, and `LONG RAW` to `BYTES`. The generic JDBC source also has
`handle_blob_as_string` for Oracle BLOB values when they should be exposed as strings before writing
to a downstream system.

If a downstream database rejects a value because the text contains a NUL byte or another unsupported
character, treat it as a target-system constraint. Clean or replace that value before the sink writes
it, for example with SQL Transform or a custom Transform.

## Why does MySQL TINYINT look different after writing ORC or Parquet?

There are two mappings in the path:

1. MySQL or JDBC source type -> SeaTunnel logical type.
2. SeaTunnel logical type -> file-format type.

For S3/HDFS/OBS file sinks, ORC maps SeaTunnel `TINYINT` to ORC `BYTE`; Parquet maps SeaTunnel
`TINYINT` to Parquet `INT_8`. If the downstream reader such as Trino shows a different display type,
check both the SeaTunnel file sink type table and the reader's own ORC/Parquet type interpretation.

## Should I put all casts in the source query or in Transform?

Use the source query when the cast is specific to the source database and you want the database to
perform it. Use Transform when the cast is part of the SeaTunnel pipeline contract and should stay
visible between source and sink.

For CDC jobs, avoid SQL expressions that remove primary key or schema metadata unless you have
confirmed that the downstream sink no longer needs that metadata for upsert, delete, auto-create, or
schema evolution behavior.
