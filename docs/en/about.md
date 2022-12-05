# About Seatunnel

<img src="https://seatunnel.apache.org/image/logo.png" alt="seatunnel logo" width="200px" height="200px" align="right" />

[![Slack](https://img.shields.io/badge/slack-%23seatunnel-4f8eba?logo=slack)](https://join.slack.com/t/apacheseatunnel/shared_invite/zt-123jmewxe-RjB_DW3M3gV~xL91pZ0oVQ)
[![Twitter Follow](https://img.shields.io/twitter/follow/ASFSeaTunnel.svg?label=Follow&logo=twitter)](https://twitter.com/ASFSeaTunnel)

SeaTunnel is a very easy-to-use ultra-high-performance distributed data integration platform that supports real-time
synchronization of massive data. It can synchronize tens of billions of data stably and efficiently every day, and has
been used in the production of nearly 100 companies.


## Why do we need SeaTunnel

SeaTunnel focuses on data integration and data synchronization, and is mainly designed to solve common problems in the field of data integration:

- Various data sources: There are hundreds of commonly-used data sources of which versions are incompatible. With the emergence of new technologies, more data sources are appearing. It is difficult for users to find a tool that can fully and quickly support these data sources.
- Complex synchronization scenarios: Data synchronization needs to support various synchronization scenarios such as offline-full synchronization, offline- incremental synchronization, CDC, real-time synchronization, and full database synchronization.
- High demand in resource: Existing data integration and data synchronization tools often require vast computing resources or JDBC connection resources to complete real-time synchronization of massive small tables. This has increased the burden on enterprises to a certain extent.
- Lack of quality and monitoring: Data integration and synchronization processes often experience loss or duplication of data. The synchronization process lacks monitoring, and it is impossible to intuitively understand the real-situation of the data during the task process.
- Complex technology stack: The technology components used by enterprises are different, and users need to develop corresponding synchronization programs for different components to complete data integration.
- Difficulty in management and maintenance: Limited to different underlying technology components (Flink/Spark) , offline synchronization and real-time synchronization often have be developed and managed separately, which increases thedifficulty of the management and maintainance.

## Features of SeaTunnel

- Rich and extensible Connector: SeaTunnel provides a Connector API that does not depend on a specific execution engine. Connectors (Source, Transform, Sink) developed based on this API can run On many different engines, such as SeaTunnel Engine, Flink, Spark that are currently supported.
- Connector plug-in: The plug-in design allows users to easily develop their own Connector and integrate it into the SeaTunnel project. Currently, SeaTunnel has supported more than 70 Connectors, and the number is surging. There is the list of the [currently-supported connectors](Connector-v2-release-state.md)
- Batch-stream integration: Connectors developed based on SeaTunnel Connector API are perfectly compatible with offline synchronization, real-time synchronization, full- synchronization, incremental synchronization and other scenarios. It greatly reduces the difficulty of managing data integration tasks.
- Support distributed snapshot algorithm to ensure data consistency.
- Multi-engine support: SeaTunnel uses SeaTunnel Engine for data synchronization by default. At the same time, SeaTunnel also supports the use of Flink or Spark as the execution engine of the Connector to adapt to the existing technical components of the enterprise. SeaTunnel supports multiple versions of Spark and Flink.
- JDBC multiplexing, database log multi-table parsing: SeaTunnel supports multi-table or whole database synchronization, which solves the problem of over- JDBC connections; supports multi-table or whole database log reading and parsing, which solves the need for CDC multi-table synchronization scenarios Problems with repeated reading and parsing of logs.
- High throughput and low latency: SeaTunnel supports parallel reading and writing, providing stable and reliable data synchronization capabilities with high throughput and low latency.
- Perfect real-time monitoring: SeaTunnel supports detailed monitoring information of each step in the data synchronization process, allowing users to easily understand the number of data, data size, QPS and other information read and written by the synchronization task.
- Two job development methods are supported: coding and canvas design: The SeaTunnel web project https://github.com/apache/incubator-seatunnel-web provides visual management of jobs, scheduling, running and monitoring capabilities.

## SeaTunnel work flowchart

![SeaTunnel work flowchart](images/architecture_diagram.png)

The runtime process of SeaTunnel is shown in the figure above.

The user configures the job information and selects the execution engine to submit the job.

The Source Connector is responsible for parallel read the data and sending the data to the downstream Transform or directly to the Sink, and the Sink writes the data to the destination. It is worth noting that both Source and Transform and Sink can be easily developed and extended by yourself.

SeaTunnel is an EL(T) data integration platform. Therefore, in SeaTunnel, Transform can only be used to perform some simple transformations on data, such as converting the data of a column to uppercase or lowercase, changing the column name, or splitting a column into multiple columns.

The default engine use by SeaTunnel is [SeaTunnel Engine](seatunnel-engine/about.md). If you choose to use the Flink or Spark engine, SeaTunnel will package the Connector into a Flink or Spark program and submit it to Flink or Spark to run.

## Connector

- **Source Connectors** SeaTunnel support read data from various relational databases, graph databases, NoSQL databases, document databases, and memory databases. Various distributed file systems such as HDFS. A variety of cloud storage, such as S3 and OSS. At the same time, we also support data reading of many common SaaS services. You can access the detailed list [here](connector-v2/source). If you want, You can develop your own source connector and easily integrate it into seatunnel.  

- **Transform Connector** 

- **Sink Connector** SeaTunnel support write data to various relational databases, graph databases, NoSQL databases, document databases, and memory databases. Various distributed file systems such as HDFS. A variety of cloud storage, such as S3 and OSS. At the same time, we also support write data to many common SaaS services. You can access the detailed list [here](connector-v2/sink). If you want, You can develop your own sink connector and easily integrate it into seatunnel.

## Who Use SeaTunnel

SeaTunnel have lots of users which you can find more information in [users](https://seatunnel.apache.org/user)

## Landscapes

<p align="center">
<br/><br/>
<img src="https://landscape.cncf.io/images/left-logo.svg" width="150" alt=""/>&nbsp;&nbsp;<img src="https://landscape.cncf.io/images/right-logo.svg" width="200" alt=""/>
<br/><br/>
SeaTunnel enriches the <a href="https://landscape.cncf.io/card-mode?category=streaming-messaging&license=apache-license-2-0&grouping=category&selected=sea-tunnal">CNCF CLOUD NATIVE Landscape.</a >
</p >

## What's More

You can see [Quick Start](/docs/category/start-v2) for the next step.
