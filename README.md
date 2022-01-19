# SeaTunnel

<img src="https://seatunnel.apache.org/image/logo.png" alt="seatunnel logo" height="200px" align="right" />

[![Backend Workflow](https://github.com/apache/incubator-seatunnel/actions/workflows/backend.yml/badge.svg?branch=dev)](https://github.com/apache/incubator-seatunnel/actions/workflows/backend.yml)

---
[![EN doc](https://img.shields.io/badge/document-English-blue.svg)](README.md)
[![CN doc](https://img.shields.io/badge/文档-中文版-blue.svg)](README_zh_CN.md)

SeaTunnel was formerly named Waterdrop , and renamed SeaTunnel since October 12, 2021.

---

SeaTunnel is a very easy-to-use ultra-high-performance distributed data integration platform that supports real-time
synchronization of massive data. It can synchronize tens of billions of data stably and efficiently every day, and has
been used in the production of nearly 100 companies.

## Why do we need SeaTunnel

SeaTunnel will do its best to solve the problems that may be encountered in the synchronization of massive data:

- Data loss and duplication
- Task accumulation and delay
- Low throughput
- Long cycle to be applied in the production environment
- Lack of application running status monitoring

## SeaTunnel use scenarios

- Mass data synchronization
- Mass data integration
- ETL with massive data
- Mass data aggregation
- Multi-source data processing

## Features of SeaTunnel

- Easy to use, flexible configuration, low code development
- Real-time streaming
- Offline multi-source data analysis
- High-performance, massive data processing capabilities
- Modular and plug-in mechanism, easy to extend
- Support data processing and aggregation by SQL
- Support Spark structured streaming
- Support Spark 2.x

## Workflow of SeaTunnel

![seatunnel-workflow.svg](https://github.com/apache/incubator-seatunnel-website/blob/main/static/image/seatunnel-workflow.svg)

Input[Data Source Input] -> Filter[Data Processing] -> Output[Result Output]

The data processing pipeline is constituted by multiple filters to meet a variety of data processing needs. If you are
accustomed to SQL, you can also directly construct a data processing pipeline by SQL, which is simple and efficient.
Currently, the filter list supported by SeaTunnel is still being expanded. Furthermore, you can develop your own data
processing plug-in, because the whole system is easy to expand.

## Plugins supported by SeaTunnel

- Input plugin Fake, File, Hdfs, Kafka, Druid, S3, Socket, self-developed Input plugin

- Filter plugin Add, Checksum, Convert, Date, Drop, Grok, Json, Kv, Lowercase, Remove, Rename, Repartition, Replace,
  Sample, Split, Sql, Table, Truncate, Uppercase, Uuid, Self-developed Filter plugin

- Output plugin Elasticsearch, File, Hdfs, Jdbc, Kafka, Druid, Mysql, S3, Stdout, self-developed Output plugin

## Environmental dependency

1. java runtime environment, java >= 8

2. If you want to run SeaTunnel in a cluster environment, any of the following Spark cluster environments is usable:

- Spark on Yarn
- Spark Standalone

If the data volume is small, or the goal is merely for functional verification, you can also start in local mode without
a cluster environment, because SeaTunnel supports standalone operation. Note: SeaTunnel 2.0 supports running on Spark
and Flink.

## Downloads

Download address for run-directly software package :https://github.com/apache/incubator-seatunnel/releases

## Quick start

Quick start: https://interestinglab.github.io/seatunnel-docs/#/zh-cn/v1/quick-start

Detailed documentation on SeaTunnel:https://interestinglab.github.io/seatunnel-docs/#/

## Application practice cases

- Weibo, Value-added Business Department Data Platform

Weibo business uses an internal customized version of SeaTunnel and its sub-project Guardian for SeaTunnel On Yarn task
monitoring for hundreds of real-time streaming computing tasks.

- Sina, Big Data Operation Analysis Platform

Sina Data Operation Analysis Platform uses SeaTunnel to perform real-time and offline analysis of data operation and
maintenance for Sina News, CDN and other services, and write it into Clickhouse.

- Sogou, Sogou Qiqian System

Sogou Qiqian System takes SeaTunnel as an ETL tool to help establish a real-time data warehouse system.

- Qutoutiao, Qutoutiao Data Center

Qutoutiao Data Center uses SeaTunnel to support mysql to hive offline ETL tasks, real-time hive to clickhouse backfill
technical support, and well covers most offline and real-time tasks needs.

- Yixia Technology, Yizhibo Data Platform

- Yonghui Superstores Founders' Alliance-Yonghui Yunchuang Technology, Member E-commerce Data Analysis Platform

SeaTunnel provides real-time streaming and offline SQL computing of e-commerce user behavior data for Yonghui Life, a
new retail brand of Yonghui Yunchuang Technology.

- Shuidichou, Data Platform

Shuidichou adopts SeaTunnel to do real-time streaming and regular offline batch processing on Yarn, processing 3~4T data
volume average daily, and later writing the data to Clickhouse.

- Tencent Cloud

Collecting various logs from business services into Apache Kafka, some of the data in Apache Kafka is consumed and extracted through Seatunnel, and then store into Clickhouse. 

For more use cases, please refer to: https://interestinglab.github.io/seatunnel-docs/#/zh-cn/case_study/

# Code of conduct

This project adheres to the Contributor Covenant [code of conduct](https://www.apache.org/foundation/policies/conduct).
By participating, you are expected to uphold this code. Please follow
the [REPORTING GUIDELINES](https://www.apache.org/foundation/policies/conduct#reporting-guidelines) to report
unacceptable behavior.

## Developer

Thanks to all developers!

[![](https://opencollective.com/seatunnel/contributors.svg?width=666)](https://github.com/apache/incubator-seatunnel/graphs/contributors)

## Contact Us

* Mail list: **dev@seatunnel.apache.org**. Mail to `dev-subscribe@seatunnel.apache.org`, follow the reply to subscribe
  the mail list.
* Slack: https://join.slack.com/t/apacheseatunnel/shared_invite/zt-10u1eujlc-g4E~ppbinD0oKpGeoo_dAw
* Twitter: https://twitter.com/ASFSeaTunnel
* [bilibili](https://space.bilibili.com/1542095008) (for china users)

# Landscapes

<p align="center">
<br/><br/>
<img src="https://landscape.cncf.io/images/left-logo.svg" width="150"/>&nbsp;&nbsp;<img src="https://landscape.cncf.io/images/right-logo.svg" width="200"/>
<br/><br/>
SeaTunnel enriches the <a href="https://landscape.cncf.io/landscape=observability-and-analysis&license=apache-license-2-0">CNCF CLOUD NATIVE Landscape.</a >

</p >
