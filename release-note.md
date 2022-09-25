# 2.2.0 Beta

## [Feature & Improve]

- Connector V2 API, Decoupling connectors from compute engines
    - [Translation] Support Flink 1.13.x
    - [Translation] Support Spark 2.4
    - [Connector-V2] [Fake] Support FakeSource (#1864)
    - [Connector-V2] [Console] Support ConsoleSink (#1864)
    - [Connector-V2] [ElasticSearch] Support ElasticSearchSink (#2330)
    - [Connector-V2] [ClickHouse] Support ClickHouse Source & Sink (#2051)
    - [Connector-V2] [JDBC] Support JDBC Source & Sink (#2048)
    - [Connector-V2] [JDBC] [Greenplum] Support Greenplum Source & Sink(#2429)
    - [Connector-V2] [JDBC] [DM] Support DaMengDB Source & Sink(#2377)
    - [Connector-V2] [File] Support Source & Sink for Local, HDFS & OSS File
    - [Connector-V2] [File] [FTP] Support FTP File Source & Sink (#2774)
    - [Connector-V2] [Hudi] Support Hudi Source (#2147)
    - [Connector-V2] [Icebreg] Support Icebreg Source (#2615)
    - [Connector-V2] [Kafka] Support Kafka Source (#1940)
    - [Connector-V2] [Kafka] Support Kafka Sink (#1952)
    - [Connector-V2] [Kudu] Support Kudu Source & Sink (#2254)
    - [Connector-V2] [MongoDB] Support MongoDB Source (#2596)
    - [Connector-V2] [MongoDB] Support MongoDB Sink (#2649)
    - [Connector-V2] [Neo4j] Support Neo4j Sink (#2434)
    - [Connector-V2] [Phoenix] Support Phoenix Source & Sink (#2499)
    - [Connector-V2] [Redis] Support Redis Source (#2569)
    - [Connector-V2] [Redis] Support Redis Sink (#2647)
    - [Connector-V2] [Socket] Support Socket Source (#1999)
    - [Connector-V2] [Socket] Support Socket Sink (#2549)
    - [Connector-V2] [HTTP] Support HTTP Source (#2012)
    - [Connector-V2] [HTTP] Support HTTP Sink (#2348)
    - [Connector-V2] [HTTP] [Wechat] Support Wechat Source Sink(#2412)
    - [Connector-V2] [Pulsar] Support Pulsar Source (#1984)
    - [Connector-V2] [Email] Support Email Sink (#2304)
    - [Connector-V2] [Sentry] Support Sentry Sink (#2244)
    - [Connector-V2] [DingTalk] Support DingTalk Sink (#2257)
    - [Connector-V2] [IotDB] Support IotDB Source (#2431)
    - [Connector-V2] [IotDB] Support IotDB Sink (#2407)
    - [Connector-V2] [Hive] Support Hive Source & Sink(#2708)
    - [Connector-V2] [Datahub] Support Datahub Sink(#2558)
- [Catalog] MySQL Catalog (#2042)
- [Format] JSON Format (#2014)
- [Spark] [ClickHouse] Support unauthorized ClickHouse (#2393)
- [Binary-Package] Add script to automatically download plugins (#2831)
- [License] Update binary license (#2798)
- [e2e] Improved e2e start sleep (#2677)
- [e2e] Container only copy required connector jars (#2675)
- [build] delete connectors*-dist modules (#2709)
- [build] Dependency management split (#2606)
- [build] The e2e module don't depend on the connector*-dist module (#2702)
- [build] Improved scope of maven-shade-plugin (#2665)
- [build] make sure flatten-maven-plugin runs after maven-shade-plugin (#2603)
- [Starter] Use the public CommandLine util class to parse the args  (#2470)
- [Spark] [Redis] Self-Achieved Redis Proxy which is not support redis function of "info replication" (#2389)
- [Flink] [Transform] support multi split,and add custome split function name (#2268)
- [Test] Upgrade junit to 5.+ (#2305)

## [Bugfix]

- [Starter] Ensure that output paths constructed from zip archive entries are validated to prevent writing files to
  unexpected locations (#2843)
- [Starter] Let the SparkCommandArgs do not split the variable value with comma (#2523)
- [Spark] fix the problem of calling the getData() method twice (#2764)
- [e2e] Fix path split exception in win10,not check file existed (#2715)

## [Docs]

- [Kafka] Update Kafka.md (#2863)
- [JDBC] Fix inconsistency between document (#2776)
- [Flink-SQL] [ElasticSearch] Updated prepare section (#2634)
- [Contribution] add CheckStyle-IDEA Plugin introduction (#2535)
- [Contribution] Update new-license.md (#2494)