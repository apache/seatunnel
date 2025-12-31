# Formats Overview

SeaTunnel supports various data formats for serialization and deserialization in different connectors, especially for streaming data pipelines. These formats allow you to work with structured data in different representations.

## Supported Formats

The following formats are currently supported in SeaTunnel:

### Avro Format
[Avro](avro.md) is a compact binary format that is very popular in streaming data pipelines. It's particularly well-suited for Kafka connectors.

### Canal JSON Format
[Canal JSON](canal-json.md) is a changelog data capture format that streams changes from MySQL databases. It's compatible with Alibaba's Canal CDC tool.

### CDC Compatible Debezium JSON Format
[CDC Compatible Debezium JSON](cdc-compatible-debezium-json.md) enables SeaTunnel to interpret CDC records as Debezium-JSON messages, providing compatibility with the Debezium ecosystem.

### Debezium JSON Format
[Debezium JSON](debezium-json.md) is a unified format for changelog data capture that works with various databases. It captures row-level changes and represents them as INSERT/UPDATE/DELETE operations.

### Kafka Compatible Kafka Connect JSON Format
[Kafka Compatible Kafka Connect JSON](kafka-compatible-kafkaconnect-json.md) allows parsing data extracted through Kafka Connect sources, especially from JDBC and Debezium connectors.

### Maxwell JSON Format
[Maxwell JSON](maxwell-json.md) is a changelog data capture format that streams changes from MySQL databases using the Maxwell CDC tool.

### OGG JSON Format
[OGG JSON](ogg-json.md) is a format for Oracle GoldenGate that provides changelog data capture capabilities for Oracle databases.

### Protobuf Format
[Protobuf](protobuf.md) is Google's language-neutral, platform-independent data serialization format that provides efficient encoding of structured data.

## Usage

Each format can be used with specific connectors. Most formats are commonly used with Kafka connectors for both source and sink operations. Check individual format documentation for detailed usage examples and configuration options.

## Choosing the Right Format

When selecting a format, consider:
- Your data source and destination systems
- The type of data operations you need to support (INSERT/UPDATE/DELETE)
- Compatibility requirements with existing systems
- Performance and storage considerations
- Schema evolution needs

Refer to each format's specific documentation for detailed configuration options and usage examples.