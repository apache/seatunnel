# Apache SeaTunnel

<img src="https://seatunnel.apache.org/image/logo.png" alt="SeaTunnel Logo" height="200px" align="right" />

[![Build Workflow](https://github.com/apache/seatunnel/actions/workflows/build_main.yml/badge.svg?branch=dev)](https://github.com/apache/seatunnel/actions/workflows/build_main.yml)
[![Join Slack](https://img.shields.io/badge/slack-%23seatunnel-4f8eba?logo=slack)](https://s.apache.org/seatunnel-slack)
[![Twitter Follow](https://img.shields.io/twitter/follow/ASFSeaTunnel.svg?label=Follow&logo=twitter)](https://twitter.com/ASFSeaTunnel)

## Table of Contents
- [Overview](#overview)
- [Why Choose SeaTunnel](#why-choose-seatunnel)
- [Key Features](#key-features)
- [SeaTunnel Workflow](#seatunnel-workflow)
- [Supported Connectors](#supported-connectors)
- [Getting Started](#getting-started)
- [Use Cases](#use-cases)
- [Code of Conduct](#code-of-conduct)
- [Contributors](#contributors)
- [How to Compile](#how-to-compile)
- [Contact Us](#contact-us)
- [Landscapes](#landscapes)
- [Apache SeaTunnel Web Project](#apache-seaTunnel-web-project)
- [Our Users](#our-users)
- [License](#license)
- [Frequently Asked Questions](#frequently-asked-questions)

## Overview

SeaTunnel is a next-generation, high-performance, distributed data integration tool, capable of synchronizing vast amounts of data daily. It's trusted by numerous companies for its efficiency and stability.


## Why Choose SeaTunnel

SeaTunnel addresses common data integration challenges:

- **Diverse Data Sources**: Seamlessly integrates with hundreds of evolving data sources.
  
- **Complex Synchronization Scenarios**: Supports various synchronization methods, including real-time, CDC, and full database synchronization.
  
- **Resource Efficiency**: Minimizes computing resources and JDBC connections for real-time synchronization.
  
- **Quality and Monitoring**: Provides data quality and monitoring to prevent data loss or duplication.

## Key Features

- **Diverse Connectors**: Offers support for over 100 connectors, with ongoing expansion.
  
- **Batch-Stream Integration**: Easily adaptable connectors simplify data integration management.
  
- **Distributed Snapshot Algorithm**: Ensures data consistency across synchronized data.
  
- **Multi-Engine Support**: Works with SeaTunnel Zeta Engine, Flink, and Spark.
  
- **JDBC Multiplexing and Log Parsing**: Efficiently synchronizes multi-tables and databases.
  
- **High Throughput and Low Latency**: Provides high-throughput data synchronization with low latency.
  
- **Real-Time Monitoring**: Offers detailed insights during synchronization.
  
- **Two Job Development Methods**: Supports coding and visual job management with the [SeaTunnel Web Project](https://github.com/apache/seatunnel-web).

## SeaTunnel Workflow

![SeaTunnel Workflow](docs/images/architecture_diagram.png)

Configure jobs, select execution engines, and parallelize data using Source Connectors. Easily develop and extend connectors to meet your needs.

## Supported Connectors

- [Source Connectors](https://seatunnel.apache.org/docs/connector-v2/source)
- [Sink Connectors](https://seatunnel.apache.org/docs/connector-v2/sink)
- [Transform Connectors](docs/en/transform-v2)

## Getting Started

Download SeaTunnel from the [Official Website](https://seatunnel.apache.org/download).

Choose your runtime execution engine:
- [SeaTunnel Zeta Engine](https://seatunnel.apache.org/docs/start-v2/locally/quick-start-seatunnel-engine/)
- [Spark](https://seatunnel.apache.org/docs/start-v2/locally/quick-start-spark)
- [Flink](https://seatunnel.apache.org/docs/start-v2/locally/quick-start-flink)

## Use Cases

Explore real-world use cases of SeaTunnel, such as Weibo, Tencent Cloud, Sina, Sogou, and Yonghui Superstores. More use cases can be found on the [SeaTunnel Blog](https://seatunnel.apache.org/blog).

## Code of Conduct

Participate in this project in accordance with the Contributor Covenant [Code of Conduct](https://www.apache.org/foundation/policies/conduct).

## Contributors

We appreciate all developers for their contributions. See the [List Of Contributors](https://github.com/apache/seatunnel/graphs/contributors).

## How to Compile

Refer to this [Setup](docs/en/contribution/setup.md) for compilation instructions.

## Contact Us

- Mail list: **dev@seatunnel.apache.org**. Subscribe by sending an email to `dev-subscribe@seatunnel.apache.org`.

- Slack: [Join SeaTunnel Slack](https://s.apache.org/seatunnel-slack)

- Twitter: [ASFSeaTunnel on Twitter](https://twitter.com/ASFSeaTunnel)

## Landscapes

SeaTunnel enriches the [CNCF CLOUD NATIVE Landscape](https://landscape.cncf.io/?landscape=observability-and-analysis&license=apache-license-2-0).

## Apache SeaTunnel Web Project

SeaTunnel Web is a web project that provides visual management of jobs, scheduling, running and monitoring capabilities. It is developed based on the SeaTunnel Connector API and the SeaTunnel Zeta Engine. It is a web project that can be deployed independently. It is also a sub-project of SeaTunnel.
For more information, please refer to [SeaTunnel Web](https://github.com/apache/seatunnel-web)

## Our Users

Companies and organizations worldwide use SeaTunnel for research, production, and commercial products. Visit our [Users](https://seatunnel.apache.org/user) for more information.

## License

[Apache 2.0 License](LICENSE)

## Frequently Asked Questions

### 1. How do I install SeaTunnel?

Follow the [Installation Guide](https://seatunnel.apache.org/docs/start-v2/locally/deployment/) on our website to get
started.

### 2. How can I contribute to SeaTunnel?

We welcome contributions! Please refer to our [Contribution Guidelines](https://github.com/apache/seatunnel/blob/dev/docs/en/contribution/coding-guide.md) for details.

### 3. How do I report issues or request features?

You can report issues or request features on our [GitHub Repository](https://github.com/apache/seatunnel/issues).

### 4. Can I use SeaTunnel for commercial purposes?

Yes, SeaTunnel is available under the Apache 2.0 License, allowing commercial use.

### 5. Where can I find documentation and tutorials?

Our [Official Documentation](https://seatunnel.apache.org/docs) includes detailed guides and tutorials to help you get started.

### 7. Is there a community or support channel?

Join our Slack community for support and discussions: [SeaTunnel Slack](https://s.apache.org/seatunnel-slack).
