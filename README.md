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

## Multimodal Data Integration
- Most data integration tools support structured and unstructured text data, and SeaTunnel does as well. Simply refer to the desired Source/Sink to use.
- For integrating video, images, and binary files with SeaTunnel, please refer to the documentation for detailed instructions.

## Apache SeaTunnel Tools
SeaTunnel Tools provides a range of peripheral tools, including Apache SeaTunnel Mcp Server, etc,please refer to [SeaTunnel Tools](https://github.com/apache/seatunnel-tools).

## Users
Companies and organizations worldwide use SeaTunnel for research, production, and commercial products. 
Explore real-world use cases of SeaTunnel, such as JP morgan, S7, JDT, Bytedance, Tencent Cloud. More use cases can be found on the [SeaTunnel Users](https://seatunnel.apache.org/user).

## Code of Conduct
Participate in this project in accordance with the Contributor Covenant [Code of Conduct](https://www.apache.org/foundation/policies/conduct).

## Contributors
We appreciate all developers for their contributions. See the [List Of Contributors](https://github.com/apache/seatunnel/graphs/contributors).

## How to Compile
Refer to this [Setup](https://seatunnel.apache.org/docs/developer/setup) for compilation instructions.

## Contact Us
- Mail list: **dev@seatunnel.apache.org**. Subscribe by sending an email to `dev-subscribe@seatunnel.apache.org`.
- Slack: [Join SeaTunnel Slack](https://s.apache.org/seatunnel-slack)
- Twitter: [ASFSeaTunnel on Twitter](https://twitter.com/ASFSeaTunnel)

## Landscapes
SeaTunnel enriches the [CNCF CLOUD NATIVE Landscape](https://landscape.cncf.io/?landscape=observability-and-analysis&license=apache-license-2-0).

## License
[Apache 2.0 License](LICENSE)

## Frequently Asked Questions

### 1. How do I install SeaTunnel?

Follow the [Local Deployment](https://seatunnel.apache.org/docs/getting-started/locally/deployment) on SeaTunnel website to get 
started quickly.
Please refer to the [Cluster Deployment](https://seatunnel.apache.org/docs/engines/zeta/hybrid-cluster-deployment)

### 2. Where can I find documentation and tutorials?
[Official Documentation](https://seatunnel.apache.org/docs) includes detailed guides and tutorials to help you get started.

### 3. Is there a community or support channel?
You can submit an issue on [GitHub Issues](https://github.com/apache/seatunnel/issues).
Join our Slack community [SeaTunnel Slack](https://s.apache.org/seatunnel-slack).
More information, please refer to [FAQ](https://seatunnel.apache.org/docs/faq). 

### 4. How can I contribute to SeaTunnel?
We welcome contributions! Please refer to our [Contribution Guidelines](https://seatunnel.apache.org/docs/developer/coding-guide) for details.

