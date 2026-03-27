# Apache SeaTunnel

<img src="https://seatunnel.apache.org/image/logo.png" alt="SeaTunnel Logo" height="200px" align="right" />

[![Build Workflow](https://github.com/apache/seatunnel/actions/workflows/build_main.yml/badge.svg?branch=dev)](https://github.com/apache/seatunnel/actions/workflows/build_main.yml)
[![Join Slack](https://img.shields.io/badge/slack-%23seatunnel-4f8eba?logo=slack)](https://s.apache.org/seatunnel-slack)
[![Twitter Follow](https://img.shields.io/twitter/follow/ASFSeaTunnel.svg?label=Follow&logo=twitter)](https://twitter.com/ASFSeaTunnel)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/apache/seatunnel)

## Overview
SeaTunnel is a multimodal, high-performance, distributed data integration tool, capable of synchronizing vast amounts of data daily. It's trusted by numerous companies for its efficiency and stability.

## Why Choose SeaTunnel
SeaTunnel addresses common data integration challenges:
- **Diverse Data Sources**: Seamlessly integrates with hundreds of evolving data sources.
- **Multimodal Data Integration**: Supports the integration of video, images, binary files, structured and unstructured text data.
- **Complex Synchronization Scenarios**: Supports various synchronization methods, including real-time, CDC, and full database synchronization.
- **Resource Efficiency**: Minimizes computing resources and JDBC connections for real-time synchronization.
- **Quality and Monitoring**: Provides data quality and monitoring to prevent data loss or duplication.

## Key Features
- **Diverse Connectors**: Offers support for over 160 connectors, with ongoing expansion.
- **Batch-Stream Integration**: Easily adaptable connectors simplify data integration management.
- **Distributed Snapshot Algorithm**: Ensures data consistency across synchronized data.
- **Multi-Engine Support**: Works with SeaTunnel Zeta Engine, Flink, and Spark.
- **JDBC Multiplexing and Log Parsing**: Efficiently synchronizes multi-tables and databases.
- **High Throughput and Low Latency**: Provides high-throughput data synchronization with low latency.
- **Real-Time Monitoring**: Offers detailed insights during synchronization.

## SeaTunnel Workflow
![SeaTunnel Workflow](docs/images/architecture_diagram.png)

Configure jobs, select execution engines, and parallelize data using Source Connectors. Easily develop and extend connectors to meet your needs.

## Supported Connectors
- [Source Connectors](https://seatunnel.apache.org/docs/connectors/source)
- [Sink Connectors](https://seatunnel.apache.org/docs/connectors/sink)
- [Transform Connectors](https://seatunnel.apache.org/docs/transforms)

## Getting Started
Download SeaTunnel from the [Official Website](https://seatunnel.apache.org/download).
Choose your runtime execution engine:
- [SeaTunnel Zeta Engine](https://seatunnel.apache.org/docs/getting-started/locally/quick-start-seatunnel-engine)
- [Spark](https://seatunnel.apache.org/docs/getting-started/locally/quick-start-spark)
- [Flink](https://seatunnel.apache.org/docs/getting-started/locally/quick-start-flink)

## Contributing
We welcome all kinds of contributions from the community! Whether it's reporting bugs, improving documentation, or submitting code for new connectors. Please check our [Contribution Guide](https://seatunnel.apache.org/community/contribution_guide/subscribe) to get started and join our [Slack](https://s.apache.org/seatunnel-slack) to discuss ideas with the maintainers.

## License
Apache SeaTunnel is licensed under the [Apache License 2.0](LICENSE).