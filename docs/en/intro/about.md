---
sidebar_position: 1
---

# About Seatunnel

<img src="https://seatunnel.apache.org/image/logo.png" alt="seatunnel logo" width="200px" height="200px" align="right" />

[![Slack](https://img.shields.io/badge/slack-%23seatunnel-4f8eba?logo=slack)](https://join.slack.com/t/apacheseatunnel/shared_invite/zt-123jmewxe-RjB_DW3M3gV~xL91pZ0oVQ)
[![Twitter Follow](https://img.shields.io/twitter/follow/ASFSeaTunnel.svg?label=Follow&logo=twitter)](https://twitter.com/ASFSeaTunnel)

SeaTunnel is a very easy-to-use ultra-high-performance distributed data integration platform that supports real-time
synchronization of massive data. It can synchronize tens of billions of data stably and efficiently every day, and has
been used in the production of nearly 100 companies.

## Use Scenarios

- Mass data synchronization
- Mass data integration
- ETL with massive data
- Mass data aggregation
- Multi-source data processing

## Features

- Easy to use, flexible configuration, low code development
- Real-time streaming
- Offline multi-source data analysis
- High-performance, massive data processing capabilities
- Modular and plug-in mechanism, easy to extend
- Support data processing and aggregation by SQL
- Support Spark structured streaming
- Support Spark 2.x

## Workflow

![seatunnel-workflow.svg](../images/seatunnel-workflow.svg)

```text
Source[Data Source Input] -> Transform[Data Processing] -> Sink[Result Output]
```

The data processing pipeline is constituted by multiple filters to meet a variety of data processing needs. If you are
accustomed to SQL, you can also directly construct a data processing pipeline by SQL, which is simple and efficient.
Currently, the filter list supported by SeaTunnel is still being expanded. Furthermore, you can develop your own data
processing plug-in, because the whole system is easy to expand.

## Connector

- Input plugin Fake, File, Hdfs, Kafka, Druid, InfluxDB, S3, Socket, self-developed Input plugin

- Filter plugin Add, Checksum, Convert, Date, Drop, Grok, Json, Kv, Lowercase, Remove, Rename, Repartition, Replace,
  Sample, Split, Sql, Table, Truncate, Uppercase, Uuid, Self-developed Filter plugin

- Output plugin Elasticsearch, File, Hdfs, Jdbc, Kafka, Druid, InfluxDB, Mysql, S3, Stdout, self-developed Output plugin

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

You can see [Quick Start](/docs/category/start) for the next step.
