# Apache SeaTunnel

<img src="https://seatunnel.apache.org/image/logo.png" alt="SeaTunnel Logo" height="200px" align="right" />

[![Build Workflow](https://github.com/apache/seatunnel/actions/workflows/build_main.yml/badge.svg?branch=dev)](https://github.com/apache/seatunnel/actions/workflows/build_main.yml)
[![Join Slack](https://img.shields.io/badge/slack-%23seatunnel-4f8eba?logo=slack)](https://s.apache.org/seatunnel-slack)
[![Twitter Follow](https://img.shields.io/twitter/follow/ASFSeaTunnel.svg?label=Follow&logo=twitter)](https://twitter.com/ASFSeaTunnel)

---

SeaTunnel is a next-generation, super high-performance, distributed, massive data integration tool. It efficiently and stably synchronizes tens of billions of data daily and has been adopted by numerous companies.

## Why Choose SeaTunnel

SeaTunnel specializes in data integration and synchronization, addressing common challenges in this domain:

- **Diverse Data Sources**: With hundreds of data sources and evolving technologies, finding a tool capable of rapidly adapting to these sources can be challenging.

- **Complex Synchronization Scenarios**: Data synchronization requirements encompass offline-full synchronization, offline-incremental synchronization, CDC, real-time synchronization, and full database synchronization.

- **Resource Efficiency**: Many existing data integration and synchronization tools demand significant computing resources and JDBC connections for real-time synchronization, adding strain to enterprises.

- **Quality and Monitoring**: Data integration and synchronization processes often encounter data loss or duplication. A lack of monitoring makes it difficult to gain insights during task execution.

## Key Features

- **Diverse Connectors**: SeaTunnel offers support for over 100 connectors, with ongoing expansion. You can explore the list of supported and planned connectors [here](https://github.com/apache/seatunnel/issues/3018).

- **Batch-Stream Integration**: Connectors developed using the SeaTunnel Connector API are highly adaptable to various synchronization scenarios, simplifying data integration management.

- **Distributed Snapshot Algorithm**: Ensures data consistency across synchronized data.

- **Multi-Engine Support**: Defaulting to the SeaTunnel Zeta Engine, SeaTunnel also supports Flink and Spark as execution engines, catering to various enterprise requirements.

- **JDBC Multiplexing and Log Parsing**: Supports multi-table or whole database synchronization, efficiently addressing over-JDBC connections and CDC multi-table synchronization issues.

- **High Throughput and Low Latency**: Provides stable, high-throughput data synchronization with low latency.

- **Real-Time Monitoring**: Detailed monitoring information throughout the synchronization process offers insights into data volume, size, QPS, and more.

- **Two Job Development Methods**: Supports both coding and canvas design. The [SeaTunnel web project](https://github.com/apache/seatunnel-web) offers visual job management, scheduling, running, and monitoring.

In addition to these features, SeaTunnel provides a versatile Connector API that is not tied to a specific execution engine, ensuring flexibility and compatibility with various engines.

## SeaTunnel Workflow

![SeaTunnel Workflow](docs/en/images/architecture_diagram.png)

The SeaTunnel runtime process involves configuring job information, selecting the execution engine, and parallelizing data using Source Connectors. These connectors can easily be developed and extended to meet specific needs. By default, SeaTunnel uses the [SeaTunnel Engine](seatunnel-engine/README.md). If desired, it can package the Connector for execution using Flink or Spark.

## Connectors Supported by SeaTunnel

- Supported Source Connectors: [Check Out](https://seatunnel.apache.org/docs/category/source-v2)
- Supported Sink Connectors: [Check Out](https://seatunnel.apache.org/docs/category/sink-v2)
- Transform Connectors: [Check Out](docs/en/transform-v2)

For a list of connectors and their health status, visit the [Connector Status](docs/en/Connector-v2-release-state.md).

## Downloads

Download SeaTunnel directly from the [official website](https://seatunnel.apache.org/download).

## Quick Start

SeaTunnel defaults to using the SeaTunnel Zeta Engine as the runtime execution engine for data synchronization. For the best functionality and performance, we recommend using the Zeta engine. However, SeaTunnel also supports Flink and Spark as execution engines.

- **SeaTunnel Zeta Engine**: [Quick Start](https://seatunnel.apache.org/docs/start-v2/locally/quick-start-seatunnel-engine/)

- **Spark**: [Quick Start](https://seatunnel.apache.org/docs/start-v2/locally/quick-start-spark)

- **Flink**: [Quick Start](https://seatunnel.apache.org/docs/start-v2/locally/quick-start-flink)

## Application Use Cases

- **Weibo, Value-added Business Department Data Platform**: Weibo's business utilizes a customized version of SeaTunnel for task monitoring in real-time streaming computing.

- **Tencent Cloud**: Collects and extracts data from various business services into Apache Kafka, storing it in Clickhouse.

- **Sina, Big Data Operation Analysis Platform**: Performs real-time and offline analysis of data operation and maintenance for services such as Sina News, CDN, and writes the data into Clickhouse.

- **Sogou, Sogou Qiqian System**: Uses SeaTunnel as an ETL tool to establish a real-time data warehouse system.

- **Yonghui Superstores Founders' Alliance-Yonghui Yunchuang Technology, Member E-commerce Data Analysis Platform**: Provides real-time streaming and offline SQL computing of e-commerce user behavior data for Yonghui Life.

For more use cases, please refer to the [SeaTunnel blog](https://seatunnel.apache.org/blog).

## Apache SeaTunnel Web Project

SeaTunnel Web is a web project that provides visual management of jobs, scheduling, running and monitoring capabilities. It is developed based on the SeaTunnel Connector API and the SeaTunnel Zeta Engine. It is a web project that can be deployed independently. It is also a sub-project of SeaTunnel.
For more information, please refer to [SeaTunnel Web](https://github.com/apache/seatunnel-web)

## Code of Conduct

This project adheres to the Contributor Covenant [Code of Conduct](https://www.apache.org/foundation/policies/conduct). By participating, you are expected to uphold this code. Please follow the [REPORTING GUIDELINES](https://www.apache.org/foundation/policies/conduct#reporting-guidelines) to report unacceptable behavior.

## Contributors

Thanks to [all developers](https://github.com/apache/seatunnel/graphs/contributors)!

<a href="https://github.com/apache/seatunnel/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=apache/seatunnel" />
</a>

## How to Compile

Please follow this [document](docs/en/contribution/setup.md).

## Contact Us

- Mail list: **dev@seatunnel.apache.org**. Mail to `dev-subscribe@seatunnel.apache.org`, follow the reply to subscribe to the mail list.

- Slack: [Join SeaTunnel Slack](https://s.apache.org/seatunnel-slack)

- Twitter: [ASFSeaTunnel on Twitter](https://twitter.com/ASFSeaTunnel)

- [Bilibili](https://space.bilibili.com/1542095008) (for Chinese users)

## Landscapes

<p align="center">
<br/><br/>
<img src="https://landscape.cncf.io/images/left-logo.svg" width="150" alt=""/>&nbsp;&nbsp;<img src="https://landscape.cncf.io/images/right-logo.svg" width="200" alt=""/>
<br/><br/>
SeaTunnel enriches the [CNCF CLOUD NATIVE Landscape](https://landscape.cncf.io/?landscape=observability-and-analysis&license=apache-license-2-0).

</p>

## Our Users

Various companies and organizations use SeaTunnel for research, production, and commercial products. Visit our [website](https://seatunnel.apache.org/user) to find the user page.

## License

[Apache 2.0 License](LICENSE)
