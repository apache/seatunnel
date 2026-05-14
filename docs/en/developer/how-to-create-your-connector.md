# Develop Your Own Connector

If you want to develop your own connector for the new SeaTunnel connector API (Connector V2), please check [here](../../../seatunnel-connectors-v2/README.md).

## Start Here

If you are building a new connector, use this page as the index and then branch to the relevant implementation guide:

- [Source Connector Development](./source-connector-development.md) for source-side design and implementation flow
- [Sink Connector Development](./sink-connector-development.md) for sink-side commit and recovery design
- [Plugin Discovery and Class Loading](../architecture/plugin-discovery-and-class-loading.md) for packaging, SPI, and dependency isolation concerns
- [Configuration And Option System](../architecture/configuration-and-option-system.md) for stable user-facing options
- [CDC Pipeline Architecture](../architecture/cdc-pipeline-architecture.md) if the connector participates in CDC-style pipelines

## Architecture Reference

For detailed information on SeaTunnel's API design and engine architecture, see:

- [Architecture Overview](../architecture/overview.md) - Overall architecture and design principles
- [Source Architecture](../architecture/api-design/source-architecture.md) - Deep dive into Source API design
- [Sink Architecture](../architecture/api-design/sink-architecture.md) - Deep dive into Sink API design
- [Translation Layer](../architecture/api-design/translation-layer.md) - How connectors work across different engines
- [Checkpoint Mechanism](../architecture/fault-tolerance/checkpoint-mechanism.md) - Fault tolerance and state management

These documents will help you understand the underlying architecture and design patterns used in SeaTunnel connectors.
