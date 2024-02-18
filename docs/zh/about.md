# 关于 SeaTunnel

<img src="https://seatunnel.apache.org/image/logo.png" alt="seatunnel logo" width="200px" height="200px" align="right" />

[![Slack](https://img.shields.io/badge/slack-%23seatunnel-4f8eba?logo=slack)](https://s.apache.org/seatunnel-slack)
[![Twitter Follow](https://img.shields.io/twitter/follow/ASFSeaTunnel.svg?label=Follow&logo=twitter)](https://twitter.com/ASFSeaTunnel)

SeaTunnel是一个非常易用、超高性能的分布式数据集成平台，支持实时
海量数据同步。 每天可稳定高效同步数百亿数据，拥有
已被近百家企业应用于生产。

## 我们为什么需要 SeaTunnel

SeaTunnel专注于数据集成和数据同步，主要旨在解决数据集成领域的常见问题：

- 数据源多样：常用数据源有数百种，版本不兼容。 随着新技术的出现，更多的数据源不断出现。 用户很难找到一个能够全面、快速支持这些数据源的工具。
- 同步场景复杂：数据同步需要支持离线全量同步、离线增量同步、CDC、实时同步、全库同步等多种同步场景。
- 资源需求高：现有的数据集成和数据同步工具往往需要大量的计算资源或JDBC连接资源来完成海量小表的实时同步。 这增加了企业的负担。
- 缺乏质量和监控：数据集成和同步过程经常会出现数据丢失或重复的情况。 同步过程缺乏监控，无法直观了解任务过程中数据的真实情况。
- 技术栈复杂：企业使用的技术组件不同，用户需要针对不同组件开发相应的同步程序来完成数据集成。
- 管理和维护困难：受限于底层技术组件（Flink/Spark）不同，离线同步和实时同步往往需要分开开发和管理，增加了管理和维护的难度。

## Features of SeaTunnel

- 丰富且可扩展的Connector：SeaTunnel提供了不依赖于特定执行引擎的Connector API。 基于该API开发的Connector（Source、Transform、Sink）可以运行在很多不同的引擎上，例如目前支持的SeaTunnel Engine、Flink、Spark等。
- Connector插件：插件式设计让用户可以轻松开发自己的Connector并将其集成到SeaTunnel项目中。 目前，SeaTunnel 支持超过 100 个连接器，并且数量正在激增。 这是[当前支持的连接器]的列表(Connector-v2-release-state.md)
- 批流集成：基于SeaTunnel Connector API开发的Connector完美兼容离线同步、实时同步、全量同步、增量同步等场景。 它们大大降低了管理数据集成任务的难度。
- 支持分布式快照算法，保证数据一致性。
- 多引擎支持：SeaTunnel默认使用SeaTunnel引擎进行数据同步。 SeaTunnel还支持使用Flink或Spark作为Connector的执行引擎，以适应企业现有的技术组件。 SeaTunnel 支持 Spark 和 Flink 的多个版本。
- JDBC复用、数据库日志多表解析：SeaTunnel支持多表或全库同步，解决了过度JDBC连接的问题； 支持多表或全库日志读取解析，解决了CDC多表同步场景下需要处理日志重复读取解析的问题。
- 高吞吐量、低延迟：SeaTunnel支持并行读写，提供稳定可靠、高吞吐量、低延迟的数据同步能力。
- 完善的实时监控：SeaTunnel支持数据同步过程中每一步的详细监控信息，让用户轻松了解同步任务读写的数据数量、数据大小、QPS等信息。
- 支持两种作业开发方法：编码和画布设计。 SeaTunnel Web 项目 https://github.com/apache/seatunnel-web 提供作业、调度、运行和监控功能的可视化管理。

## SeaTunnel work flowchart

![SeaTunnel work flowchart](images/architecture_diagram.png)

The runtime process of SeaTunnel is shown in the figure above.

The user configures the job information and selects the execution engine to submit the job.

The Source Connector is responsible for parallel reading the data and sending the data to the downstream Transform or directly to the Sink, and the Sink writes the data to the destination. It is worth noting that Source, Transform and Sink can be easily developed and extended by yourself.

SeaTunnel is an EL(T) data integration platform. Therefore, in SeaTunnel, Transform can only be used to perform some simple transformations on data, such as converting the data of a column to uppercase or lowercase, changing the column name, or splitting a column into multiple columns.

The default engine use by SeaTunnel is [SeaTunnel Engine](seatunnel-engine/about.md). If you choose to use the Flink or Spark engine, SeaTunnel will package the Connector into a Flink or Spark program and submit it to Flink or Spark to run.

## Connector

- **Source Connectors** SeaTunnel supports reading data from various relational, graph, NoSQL, document, and memory databases; distributed file systems such as HDFS; and a variety of cloud storage solutions, such as S3 and OSS. We also support data reading of many common SaaS services. You can access the detailed list [here](connector-v2/source). If you want, You can develop your own source connector and easily integrate it into SeaTunnel.

- **Transform Connector** If the schema is different between source and Sink, You can use the Transform Connector to change the schema read from source and make it the same as the Sink schema.

- **Sink Connector** SeaTunnel supports writing data to various relational, graph, NoSQL, document, and memory databases; distributed file systems such as HDFS; and a variety of cloud storage solutions, such as S3 and OSS. We also support writing data to many common SaaS services. You can access the detailed list [here](connector-v2/sink). If you want, you can develop your own Sink connector and easily integrate it into SeaTunnel.

## Who uses SeaTunnel

SeaTunnel has lots of users. You can find more information about them in [users](https://seatunnel.apache.org/user).

## Landscapes

<p align="center">
<br/><br/>
<img src="https://landscape.cncf.io/images/left-logo.svg" width="150" alt=""/>&nbsp;&nbsp;<img src="https://landscape.cncf.io/images/right-logo.svg" width="200" alt=""/>
<br/><br/>
SeaTunnel enriches the <a href="https://landscape.cncf.io/card-mode?category=streaming-messaging&license=apache-license-2-0&grouping=category&selected=sea-tunnal">CNCF CLOUD NATIVE Landscape</a >.
</p >

## Learn more

You can see [Quick Start](/docs/category/start-v2) for the next steps.
