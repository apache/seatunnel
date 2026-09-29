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

## Useful Next Pages

- [Data Format Handling](../../architecture/data-format-handling.md)
- [Connector FAQ](../connector-faq.md)
