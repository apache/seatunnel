---
slug: /connectors/formats
sidebar_position: 1
---

# Formats

Format docs explain how SeaTunnel maps between its internal row model and external data encodings such as Avro, Debezium JSON, or Protobuf. Read this section when the connector itself is not enough and you also need to control the payload shape.

## When You Should Read Format Docs

- your source or sink exchanges schema-based payloads
- your CDC pipeline depends on a specific envelope format
- you need to align external serialization with downstream consumers

## DECIMAL Precision in JSON

When a schema declares a `DECIMAL` field, including one nested in an array, the JSON reader parses floating-point numbers as `BigDecimal` to avoid losing numeric precision. This covers `ARRAY<DECIMAL>`, `ARRAY<ARRAY<DECIMAL>>` and `ARRAY<ROW<DECIMAL>>`. The setting applies to the whole document: a bare floating-point number assigned to a `STRING` sibling is rendered from its decimal value and may retain more digits. It does not preserve the original token spelling; for example, `0.0000001234` can become `1.234E-7`. The same applies to a JSON object or array captured as `STRING`: the floating-point numbers inside it are re-rendered from their decimal values, so their text can change too, while numbers quoted inside that object or array keep the original text. Quoted strings and schemas without `DECIMAL` are unchanged.

## Useful Next Pages

- [Data Format Handling](../../architecture/data-format-handling.md)
- [Connector FAQ](../connector-faq.md)
