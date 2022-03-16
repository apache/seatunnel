# Apache SeaTunnel (Incubating)

<img src="https://seatunnel.apache.org/image/logo.png" alt="seatunnel logo" height="200px" align="right" />

[![Backend Workflow](https://github.com/apache/incubator-seatunnel/actions/workflows/backend.yml/badge.svg?branch=dev)](https://github.com/apache/incubator-seatunnel/actions/workflows/backend.yml)
[![Slack](https://img.shields.io/badge/slack-%23seatunnel-4f8eba?logo=slack)](https://join.slack.com/t/apacheseatunnel/shared_invite/zt-123jmewxe-RjB_DW3M3gV~xL91pZ0oVQ)
[![Twitter Follow](https://img.shields.io/twitter/follow/ASFSeaTunnel.svg?label=Follow&logo=twitter)](https://twitter.com/ASFSeaTunnel)

---
[![EN doc](https://img.shields.io/badge/document-English-blue.svg)](README.md)
[![CN doc](https://img.shields.io/badge/文档-中文版-blue.svg)](README_zh_CN.md)

更名通知：SeaTunnel 原名为 waterdrop，于 2021 年 10 月 12 日更名为 SeaTunnel。

---

SeaTunnel 是一个非常易用的支持海量数据实时同步的超高性能分布式数据集成平台，每天可以稳定高效同步数百亿数据，已在近百家公司生产上使用。

## 为什么我们需要 SeaTunnel

SeaTunnel 尽所能为您解决海量数据同步中可能遇到的问题：

* 数据丢失与重复
* 任务堆积与延迟
* 吞吐量低
* 应用到生产环境周期长
* 缺少应用运行状态监控

## SeaTunnel 使用场景

* 海量数据同步
* 海量数据集成
* 海量数据的 ETL
* 海量数据聚合
* 多源数据处理

## SeaTunnel 的特性

* 简单易用，灵活配置，无需开发
* 实时流式处理
* 离线多源数据分析
* 高性能、海量数据处理能力
* 模块化和插件化，易于扩展
* 支持利用 SQL 做数据处理和聚合
* 支持 Spark Structured Streaming
* 支持 Spark 2.x

## SeaTunnel 的工作流程

![seatunnel-workflow.svg](https://raw.githubusercontent.com/apache/incubator-seatunnel-website/main/static/image/seatunnel-workflow.svg)

```
Source[数据源输入] -> Transform[数据处理] -> Sink[结果输出]
```

多个 Transform 构建了数据处理的 Pipeline，满足各种各样的数据处理需求，如果您熟悉 SQL，也可以直接通过 SQL 构建数据处理的 Pipeline，简单高效。目前 seatunnel
支持的[Transform 列表](https://seatunnel.apache.org/docs/spark/configuration/transform-plugins/transform-plugin),
仍然在不断扩充中。您也可以开发自己的数据处理插件，整个系统是易于扩展的。

## SeaTunnel 支持的插件

| <div style="width: 130pt">Spark Connector Plugins | <div style="width: 80pt">Database Type | <div style="width: 50pt">Source | <div style="width: 50pt">Sink                        |
|:------------------------:|:--------------:|:------------------------------------------------------------------:|:-------------------------------------------------------------------:|
|Batch                     |Fake            |[doc](./docs/en/spark/configuration/source-plugins/Fake.md)         |                                                                     |
|                          |ElasticSearch   |[doc](./docs/en/spark/configuration/source-plugins/Elasticsearch.md)|[doc](./docs/en/spark/configuration/sink-plugins/Elasticsearch.md)   |
|                          |File            |[doc](./docs/en/spark/configuration/source-plugins/File.md)         |[doc](./docs/en/spark/configuration/sink-plugins/File.md)            |
|                          |Hive            |[doc](./docs/en/spark/configuration/source-plugins/Hive.md)         |[doc](./docs/en/spark/configuration/sink-plugins/Hive.md)          |
|                          |Hudi            |[doc](./docs/en/spark/configuration/source-plugins/Hudi.md)         |[doc](./docs/en/spark/configuration/sink-plugins/Hudi.md)            |
|                          |Jdbc            |[doc](./docs/en/spark/configuration/source-plugins/Jdbc.md)         |[doc](./docs/en/spark/configuration/sink-plugins/Jdbc.md)            |
|                          |MongoDB         |[doc](./docs/en/spark/configuration/source-plugins/MongoDB.md)      |[doc](./docs/en/spark/configuration/sink-plugins/MongoDB.md)         |
|                          |neo4j           |[doc](./docs/en/spark/configuration/source-plugins/neo4j.md)        |                                                                     |
|                          |Phoenix         |[doc](./docs/en/spark/configuration/source-plugins/Phoenix.md)      |[doc](./docs/en/spark/configuration/sink-plugins/Phoenix.md)         |
|                          |Redis           |[doc](./docs/en/spark/configuration/source-plugins/Redis.md)        |[doc](./docs/en/spark/configuration/sink-plugins/Redis.md)           |
|                          |Tidb            |[doc](./docs/en/spark/configuration/source-plugins/Tidb.md)         |[doc](./docs/en/spark/configuration/sink-plugins/Tidb.md)            |
|                          |Clickhouse      |                                                                    |[doc](./docs/en/spark/configuration/sink-plugins/Clickhouse.md)      |  
|                          |Doris           |                                                                    |[doc](./docs/en/spark/configuration/sink-plugins/Doris.md)           |
|                          |Email           |                                                                    |[doc](./docs/en/spark/configuration/sink-plugins/Email.md)           |
|                          |Hbase           |[doc](./docs/en/spark/configuration/source-plugins/Hbase.md)        |[doc](./docs/en/spark/configuration/sink-plugins/Hbase.md)           |
|                          |Kafka           |                                                                    |[doc](./docs/en/spark/configuration/sink-plugins/Kafka.md)           |
|                          |Console         |                                                                    |[doc](./docs/en/spark/configuration/sink-plugins/Console.md)         |
|                          |Kudu            |[doc](./docs/en/spark/configuration/source-plugins/Kudu.md)         |[doc](./docs/en/spark/configuration/sink-plugins/Kudu.md)            |
|                          |Redis           |[doc](./docs/en/spark/configuration/source-plugins/Redis.md)        |[doc](./docs/en/spark/configuration/sink-plugins/Redis.md)           |
|Stream                    |FakeStream      |[doc](./docs/en/spark/configuration/source-plugins/FakeStream.md)   |                                                                     |
|                          |KafkaStream     |[doc](./docs/en/spark/configuration/source-plugins/KafkaStream.md)  |                                                                     |
|                          |SocketStream    |[doc](./docs/en/spark/configuration/source-plugins/SocketStream.md) |                                                                     |

| <div style="width: 130pt">Flink Connector Plugins | <div style="width: 80pt">Database Type  | <div style="width: 50pt">Source | <div style="width: 50pt">Sink                                                                |
|:------------------------:|:--------------:|:------------------------------------------------------------------:|:-------------------------------------------------------------------:|
|                          |Druid           |[doc](./docs/en/flink/configuration/source-plugins/Druid.md)        |[doc](./docs/en/flink/configuration/sink-plugins/Druid.md)           |
|                          |Fake            |[doc](./docs/en/flink/configuration/source-plugins/Fake.md)         |                                                                     |
|                          |File            |[doc](./docs/en/flink/configuration/source-plugins/File.md)         |[doc](./docs/en/flink/configuration/sink-plugins/File.md)            |
|                          |InfluxDb        |[doc](./docs/en/flink/configuration/source-plugins/InfluxDb.md)     |[doc](./docs/en/flink/configuration/sink-plugins/InfluxDb.md)        |
|                          |Jdbc            |[doc](./docs/en/flink/configuration/source-plugins/Jdbc.md)         |[doc](./docs/en/flink/configuration/sink-plugins/Jdbc.md)            |
|                          |Kafka           |[doc](./docs/en/flink/configuration/source-plugins/Kafka.md)        |[doc](./docs/en/flink/configuration/sink-plugins/Kafka.md)           |
|                          |Socket          |[doc](./docs/en/flink/configuration/source-plugins/Socket.md)       |                                                                     |
|                          |Console         |                                                                    |[doc](./docs/en/flink/configuration/sink-plugins/Console.md)         |
|                          |Doris           |                                                                    |[doc](./docs/en/flink/configuration/sink-plugins/Doris.md)           |
|                          |ElasticSearch   |                                                                    |[doc](./docs/en/flink/configuration/sink-plugins/Elasticsearch.md)   |

|<div style="width: 130pt">Transform Plugins| <div style="width: 100pt">Spark                                    | <div style="width: 100pt">Flink                                     |
|:-----------------------------------------:|:------------------------------------------------------------------:|:-------------------------------------------------------------------:|
|Add                                        |                                                                    |                                                                     |
|CheckSum                                   |                                                                    |                                                                     |
|Convert                                    |                                                                    |                                                                     |
|Date                                       |                                                                    |                                                                     |
|Drop                                       |                                                                    |                                                                     |
|Grok                                       |                                                                    |                                                                     |
|Json                                       |[doc](./docs/en/spark/configuration/transform-plugins/Json.md)      |                                                                     |
|Kv                                         |                                                                    |                                                                     |
|Lowercase                                  |                                                                    |                                                                     |
|Remove                                     |                                                                    |                                                                     |
|Rename                                     |                                                                    |                                                                     |
|Repartition                                |                                                                    |                                                                     |
|Replace                                    |                                                                    |                                                                     |
|Sample                                     |                                                                    |                                                                     |
|Split                                      |[doc](./docs/en/spark/configuration/transform-plugins/Split.md)     |[doc](./docs/en/flink/configuration/transform-plugins/Split.md)      |
|Sql                                        |[doc](./docs/en/spark/configuration/transform-plugins/Sql.md)       |[doc](./docs/en/flink/configuration/transform-plugins/Sql.md)        |
|Table                                      |                                                                    |                                                                     |
|Truncate                                   |                                                                    |                                                                     |
|Uppercase                                  |                                                                    |                                                                     |
|Uuid                                       |                                                                    |                                                                     |

## 环境依赖

1. java 运行环境，java >= 8

2. 如果您要在集群环境中运行 seatunnel，那么需要以下 Spark 集群环境的任意一种：

* Spark on Yarn
* Spark Standalone

如果您的数据量较小或者只是做功能验证，也可以仅使用 `local` 模式启动，无需集群环境，seatunnel 支持单机运行。 注: seatunnel 2.0 支持 Spark 和 Flink 上运行

## 下载

可以直接运行的软件包下载地址：https://github.com/apache/incubator-seatunnel/releases

## 快速入门

**Spark**
https://seatunnel.apache.org/docs/spark/quick-start

**Flink**
https://seatunnel.apache.org/docs/flink/quick-start

关于 SeaTunnel 的[详细文档](https://seatunnel.apache.org/docs/introduction)

## 生产应用案例

* [微博](https://weibo.com), 增值业务部数据平台 微博某业务有数百个实时流式计算任务使用内部定制版
  SeaTunnel，以及其子项目[Guardian](https://github.com/InterestingLab/guardian) 做 seatunnel On Yarn 的任务监控。

* [新浪](http://www.sina.com.cn/), 大数据运维分析平台 新浪运维数据分析平台使用 SeaTunnel 为新浪新闻，CDN 等服务做运维大数据的实时和离线分析，并写入 Clickhouse。

* [搜狗](http://sogou.com/) ，搜狗奇点系统 搜狗奇点系统使用 SeaTunnel 作为 ETL 工具, 帮助建立实时数仓体系

* [趣头条](https://www.qutoutiao.net/) ，趣头条数据中心 趣头条数据中心，使用 SeaTunnel 支撑 mysql to hive 的离线 ETL 任务、实时 hive to clickhouse 的
  backfill 技术支撑，很好的 cover 离线、实时大部分任务场景。

* [一下科技](https://www.yixia.com/), 一直播数据平台
* 永辉超市子公司-永辉云创，会员电商数据分析平台 SeaTunnel 为永辉云创旗下新零售品牌永辉生活提供电商用户行为数据实时流式与离线 SQL 计算。

* 水滴筹, 数据平台 水滴筹在 Yarn 上使用 SeaTunnel 做实时流式以及定时的离线批处理，每天处理 3～4T 的数据量，最终将数据写入 Clickhouse。

更多案例参见: https://seatunnel.apache.org/blog

## 行为准则

SeaTunnel遵守贡献者公约[code of conduct](https://www.apache.org/foundation/policies/conduct) ，
通过参与，我们期望大家可以一起维护这一准则，请遵循 [REPORTING GUIDELINES](https://www.apache.org/foundation/policies/conduct#reporting-guidelines)来报告不当行为
.

## 开发者

感谢所有开发者！

[![](https://opencollective.com/seatunnel/contributors.svg?width=666)](https://github.com/apache/incubator-seatunnel/graphs/contributors)


## 欢迎联系

* 邮件列表: **dev@seatunnel.apache.org**. 发送任意内容至 `dev-subscribe@seatunnel.apache.org`， 按照回复订阅邮件列表。
* Slack: 发送 `Request to join SeaTunnel slack` 邮件到邮件列表 (`dev@seatunnel.apache.org`), 我们会邀请你加入（在此之前请确认已经注册Slack）.
* [bilibili B站 视频](https://space.bilibili.com/1542095008)

## 我们的用户
各种公司和组织将 SeaTunnel 用于科研、商业化产品使用。
访问我们的 [官方网站](https://seatunnel.apache.org/user) 查找用户页面。
## License
[Apache 2.0 License.](LICENSE)
