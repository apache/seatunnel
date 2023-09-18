# Apache SeaTunnel

<img src="https://seatunnel.apache.org/image/logo.png" alt="seatunnel logo" height="200px" align="right" />

[![Backend Workflow](https://github.com/apache/seatunnel/actions/workflows/backend.yml/badge.svg?branch=dev)](https://github.com/apache/seatunnel/actions/workflows/backend.yml)
[![Slack](https://img.shields.io/badge/slack-%23seatunnel-4f8eba?logo=slack)](https://s.apache.org/seatunnel-slack)
[![Twitter Follow](https://img.shields.io/twitter/follow/ASFSeaTunnel.svg?label=Follow&logo=twitter)](https://twitter.com/ASFSeaTunnel)

---
[![EN doc](https://img.shields.io/badge/document-English-blue.svg)](README.md)

SeaTunnel was formerly named Waterdrop , and renamed SeaTunnel since October 12, 2021.

---

SeaTunnel is a next-generation super high-performance, distributed, massive data integration tool. It can synchronize tens of billions of data stably and efficiently every day, and has been used in the production of many companies.

## Why do we need SeaTunnel

SeaTunnel focuses on data integration and data synchronization, and is mainly designed to solve common problems in the field of data integration:

- Various data sources: There are hundreds of commonly-used data sources of which versions are incompatible. With the emergence of new technologies, more data sources are appearing. It is difficult for users to find a tool that can fully and quickly support these data sources.
- Complex synchronization scenarios: Data synchronization needs to support various synchronization scenarios such as offline-full synchronization, offline-incremental synchronization, CDC, real-time synchronization, and full database synchronization.
- High demand in resource: Existing data integration and data synchronization tools often require vast computing resources or JDBC connection resources to complete real-time synchronization of massive small tables. This has increased the burden on enterprises to a certain extent.
- Lack of quality and monitoring: Data integration and synchronization processes often experience loss or duplication of data. The synchronization process lacks monitoring, and it is impossible to intuitively understand the real-situation of the data during the task process.

## Features of SeaTunnel

- Diverse Connectors: SeaTunnel has supported more than 100 Connectors, and the number is surging. Here is the list of connectors we [supported and plan to support](https://github.com/apache/seatunnel/issues/3018).
- Batch-stream integration: Connectors developed based on SeaTunnel Connector API are perfectly compatible with offline synchronization, real-time synchronization, full- synchronization, incremental synchronization and other scenarios. It greatly reduces the difficulty of managing data integration tasks.
- Support distributed snapshot algorithm to ensure data consistency.
- Multi-engine support: SeaTunnel uses SeaTunnel Zeta Engine for data synchronization by default. At the same time, SeaTunnel also supports the use of Flink or Spark as the execution engine of the Connector to adapt to the existing technical components of the enterprise. In addition, SeaTunnel supports multiple versions of Spark and Flink.
- JDBC multiplexing, database log multi-table parsing: SeaTunnel supports multi-table or whole database synchronization, which solves the problem of over-JDBC connections; supports multi-table or whole database log reading and parsing, which solves the need for CDC multi-table synchronization scenarios problems with repeated reading and parsing of logs.
- High throughput and low latency: SeaTunnel supports parallel reading and writing, providing stable and reliable data synchronization capabilities with high throughput and low latency.
- Perfect real-time monitoring: SeaTunnel supports detailed monitoring information of each step in the data synchronization process, allowing users to easily understand the number of data, data size, QPS and other information read and written by the synchronization task.
- Two job development methods are supported: coding and canvas design. The SeaTunnel web project https://github.com/apache/seatunnel-web provides visual management of jobs, scheduling, running and monitoring capabilities.

Besides, SeaTunnel provides a Connector API that does not depend on a specific execution engine. Connectors (Source, Transform, Sink) developed based on this API can run on many different engines, such as SeaTunnel Zeta Engine, Flink, Spark that are currently supported.

## SeaTunnel work flowchart

![SeaTunnel work flowchart](docs/en/images/architecture_diagram.png)

The runtime process of SeaTunnel is shown in the figure above. 

The user configures the job information and selects the execution engine to submit the job. 

The Source Connector is responsible for parallelizing the data and sending the data to the downstream Transform or directly to the Sink, and the Sink writes the data to the destination. It is worth noting that both Source and Transform and Sink can be easily developed and extended by yourself. 

The default engine use by SeaTunnel is [SeaTunnel Engine](seatunnel-engine/README.md). If you choose to use the Flink or Spark engine, SeaTunnel will package the Connector into a Flink or Spark program and submit it to Flink or Spark to run.


## Connectors supported by SeaTunnel

- Source Connectors supported [check out](https://seatunnel.apache.org/docs/category/source-v2)

- Sink Connectors supported [check out](https://seatunnel.apache.org/docs/category/sink-v2)

- Transform supported [check out](docs/en/transform-v2)

### Here's a list of our connectors with their health status.[connector status](docs/en/Connector-v2-release-state.md)


## Downloads

Download address for run-directly software package : https://seatunnel.apache.org/download

## Quick start
SeaTunnel uses SeaTunnel Zeta Engine as the runtime execution engine for data synchronization by default. We highly recommend utilizing Zeta engine as the runtime engine, as it offers superior functionality and performance. By the way, SeaTunnel also supports the use of Flink or Spark as the execution engine. 

**SeaTunnel Zeta Engine**
https://seatunnel.apache.org/docs/start-v2/locally/quick-start-seatunnel-engine/

**Spark**
https://seatunnel.apache.org/docs/start-v2/locally/quick-start-spark

**Flink**
https://seatunnel.apache.org/docs/start-v2/locally/quick-start-flink

## Application practice cases

- Weibo, Value-added Business Department Data Platform

Weibo business uses an internal customized version of SeaTunnel and its sub-project Guardian for SeaTunnel On Yarn task
monitoring for hundreds of real-time streaming computing tasks.

- Tencent Cloud

Collecting various logs from business services into Apache Kafka, some of the data in Apache Kafka is consumed and extracted through SeaTunnel, and then store into Clickhouse. 

- Sina, Big Data Operation Analysis Platform

Sina Data Operation Analysis Platform uses SeaTunnel to perform real-time and offline analysis of data operation and
maintenance for Sina News, CDN and other services, and write it into Clickhouse.

- Sogou, Sogou Qiqian System

Sogou Qiqian System takes SeaTunnel as an ETL tool to help establish a real-time data warehouse system.

- Yonghui Superstores Founders' Alliance-Yonghui Yunchuang Technology, Member E-commerce Data Analysis Platform

SeaTunnel provides real-time streaming and offline SQL computing of e-commerce user behavior data for Yonghui Life, a
new retail brand of Yonghui Yunchuang Technology.

For more use cases, please refer to: https://seatunnel.apache.org/blog

## Code of conduct

This project adheres to the Contributor Covenant [code of conduct](https://www.apache.org/foundation/policies/conduct).
By participating, you are expected to uphold this code. Please follow
the [REPORTING GUIDELINES](https://www.apache.org/foundation/policies/conduct#reporting-guidelines) to report
unacceptable behavior.

## Contributors

Thanks to [all developers](https://github.com/apache/seatunnel/graphs/contributors)!

<a href="https://github.com/apache/seatunnel/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=apache/seatunnel" />
</a>

## How to compile
Please follow this [document](docs/en/contribution/setup.md).

## Contact Us

* Mail list: **dev@seatunnel.apache.org**. Mail to `dev-subscribe@seatunnel.apache.org`, follow the reply to subscribe
  the mail list.
* Slack: https://s.apache.org/seatunnel-slack
* Twitter: https://twitter.com/ASFSeaTunnel
* [Bilibili](https://space.bilibili.com/1542095008) (for Chinese users)

## Landscapes

<p align="center">
<br/><br/>
<img src="https://landscape.cncf.io/images/left-logo.svg" width="150" alt=""/>&nbsp;&nbsp;<img src="https://landscape.cncf.io/images/right-logo.svg" width="200" alt=""/>
<br/><br/>
SeaTunnel enriches the <a href="https://landscape.cncf.io/?landscape=observability-and-analysis&license=apache-license-2-0">CNCF CLOUD NATIVE Landscape.</a >

</p >

## Our Users
Various companies and organizations use SeaTunnel for research, production and commercial products.
Visit our [website](https://seatunnel.apache.org/user) to find the user page.

## License
[Apache 2.0 License.](LICENSE)
