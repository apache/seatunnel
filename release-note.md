# 2.3.0 Beta

## [Connector V2]

### [New Connector V2 Added]

- [Source] [Kafka] Add Kafka Source Connector ([2953](https://github.com/apache/incubator-seatunnel/pull/2953))
- [Source] [Pulsar] Add Pulsar Source Connector ([1980](https://github.com/apache/incubator-seatunnel/pull/1980))
- [Source] [S3File] Add S3 File Source Connector ([3119](https://github.com/apache/incubator-seatunnel/pull/3119))
- [Source] [JDBC] [Phoenix] Add Phoenix JDBC Source Connector ([2499](https://github.com/apache/incubator-seatunnel/pull/2499))
- [Source] [JDBC] [SQL Server] Add SQL Server JDBC Source Connector ([2646](https://github.com/apache/incubator-seatunnel/pull/2646))
- [Source] [JDBC] [Oracle] Add Oracle JDBC Source Connector ([2550](https://github.com/apache/incubator-seatunnel/pull/2550))
- [Source] [JDBC] [GBase8a] Add GBase8a JDBC Source Connector ([3026](https://github.com/apache/incubator-seatunnel/pull/3026))
- [Source] [JDBC] [StarRocks] Add StarRocks JDBC Source Connector ([3060](https://github.com/apache/incubator-seatunnel/pull/3060))
- [Sink] [Kafka] Add Kafka Source Connector ([2953](https://github.com/apache/incubator-seatunnel/pull/2953))
- [Sink] [S3File] Add S3 File Sink Connector ([3119](https://github.com/apache/incubator-seatunnel/pull/3119))
- [Sink] [Hive] [Improve] Hive Sink supports automatic partition repair ([3133](https://github.com/apache/incubator-seatunnel/pull/3133))

### [Improve & Bug Fix]

- [Source] [Fake]
  - [Improve] Supports direct definition of data values(row) ([2839](https://github.com/apache/incubator-seatunnel/pull/2839))
  - [Improve] Improve fake source connector: ([2944](https://github.com/apache/incubator-seatunnel/pull/2944))
    - Support user-defined map size
    - Support user-defined array size
    - Support user-defined string length
    - Support user-defined bytes length
  - [Improve] Support multiple splits for fake source connector ([2974](https://github.com/apache/incubator-seatunnel/pull/2974))
  - [Improve] Supports setting the number of splits per parallelism and the reading interval between two splits ([3098](https://github.com/apache/incubator-seatunnel/pull/3098))

- [Source] [Clickhouse]
  - [Improve] Clickhouse Source random use host when config multi-host ([3108](https://github.com/apache/incubator-seatunnel/pull/3108))

- [Source] [FtpFile]
  - [BugFix] Fix the bug of incorrect path in windows environment ([2980](https://github.com/apache/incubator-seatunnel/pull/2980))
  - [Improve] Support extract partition from SeaTunnelRow fields ([3085](https://github.com/apache/incubator-seatunnel/pull/3085))
  - [Improve] Support parse field from file path ([2985](https://github.com/apache/incubator-seatunnel/pull/2985))

- [Source] [HDFSFile]
  - [BugFix] Fix the bug of incorrect path in windows environment ([2980](https://github.com/apache/incubator-seatunnel/pull/2980))
  - [Improve] Support extract partition from SeaTunnelRow fields ([3085](https://github.com/apache/incubator-seatunnel/pull/3085))
  - [Improve] Support parse field from file path ([2985](https://github.com/apache/incubator-seatunnel/pull/2985))

- [Source] [LocalFile]
  - [BugFix] Fix the bug of incorrect path in windows environment ([2980](https://github.com/apache/incubator-seatunnel/pull/2980))
  - [Improve] Support extract partition from SeaTunnelRow fields ([3085](https://github.com/apache/incubator-seatunnel/pull/3085))
  - [Improve] Support parse field from file path ([2985](https://github.com/apache/incubator-seatunnel/pull/2985))

- [Source] [OSSFile]
  - [BugFix] Fix the bug of incorrect path in windows environment ([2980](https://github.com/apache/incubator-seatunnel/pull/2980))
  - [Improve] Support extract partition from SeaTunnelRow fields ([3085](https://github.com/apache/incubator-seatunnel/pull/3085))
  - [Improve] Support parse field from file path ([2985](https://github.com/apache/incubator-seatunnel/pull/2985))

- [Source] [IoTDB]
  - [Improve] Improve IoTDB Source Connector ([2917](https://github.com/apache/incubator-seatunnel/pull/2917))
    - Support extract timestamp、device、measurement from SeaTunnelRow
    - Support TINYINT、SMALLINT
    - Support flush cache to database before prepareCommit

- [Source] [JDBC]
  - [Feature] Support Phoenix JDBC Source ([2499](https://github.com/apache/incubator-seatunnel/pull/2499))
  - [Feature] Support SQL Server JDBC Source ([2646](https://github.com/apache/incubator-seatunnel/pull/2646))
  - [Feature] Support Oracle JDBC Source ([2550](https://github.com/apache/incubator-seatunnel/pull/2550))
  - [Feature] Support StarRocks JDBC Source ([3060](https://github.com/apache/incubator-seatunnel/pull/3060))
  - [Feature] Support GBase8a JDBC Source ([3026](https://github.com/apache/incubator-seatunnel/pull/3026))

- [Sink] [Assert]
  - [Improve] 1.Support check the number of rows ([2844](https://github.com/apache/incubator-seatunnel/pull/2844)) ([3031](https://github.com/apache/incubator-seatunnel/pull/3031)):
    - check rows not empty
    - check minimum number of rows
    - check maximum number of rows
  - [Improve] 2.Support direct define of data values(row) ([2844](https://github.com/apache/incubator-seatunnel/pull/2844)) ([3031](https://github.com/apache/incubator-seatunnel/pull/3031))
  - [Improve] 3.Support setting parallelism as 1 ([2844](https://github.com/apache/incubator-seatunnel/pull/2844)) ([3031](https://github.com/apache/incubator-seatunnel/pull/3031))

- [Sink] [Clickhouse]
  - [Improve] Clickhouse Support Int128,Int256 Type ([3067](https://github.com/apache/incubator-seatunnel/pull/3067))

- [Sink] [Console]
  - [Improve] Console sink support print subtask index ([3000](https://github.com/apache/incubator-seatunnel/pull/3000))

- [Sink] [Enterprise-WeChat]
  - [BugFix] Fix Enterprise-WeChat Sink data serialization ([2856](https://github.com/apache/incubator-seatunnel/pull/2856))

- [Sink] [FtpFile]
  - [BugFix] Fix the bug of incorrect path in windows environment ([2980](https://github.com/apache/incubator-seatunnel/pull/2980))
  - [BugFix] Fix filesystem get error ([3117](https://github.com/apache/incubator-seatunnel/pull/3117))
  - [BugFix] Solved the bug of can not parse '\t' as delimiter from config file ([3083](https://github.com/apache/incubator-seatunnel/pull/3083))

- [Sink] [HDFSFile]
  - [BugFix] Fix the bug of incorrect path in windows environment ([2980](https://github.com/apache/incubator-seatunnel/pull/2980))
  - [BugFix] Fix filesystem get error ([3117](https://github.com/apache/incubator-seatunnel/pull/3117))
  - [BugFix] Solved the bug of can not parse '\t' as delimiter from config file ([3083](https://github.com/apache/incubator-seatunnel/pull/3083))

- [Sink] [LocalFile]
  - [BugFix] Fix the bug of incorrect path in windows environment ([2980](https://github.com/apache/incubator-seatunnel/pull/2980))
  - [BugFix] Fix filesystem get error ([3117](https://github.com/apache/incubator-seatunnel/pull/3117))
  - [BugFix] Solved the bug of can not parse '\t' as delimiter from config file ([3083](https://github.com/apache/incubator-seatunnel/pull/3083))

- [Sink] [OSSFile]
  - [BugFix] Fix the bug of incorrect path in windows environment ([2980](https://github.com/apache/incubator-seatunnel/pull/2980))
  - [BugFix] Fix filesystem get error ([3117](https://github.com/apache/incubator-seatunnel/pull/3117))
  - [BugFix] Solved the bug of can not parse '\t' as delimiter from config file ([3083](https://github.com/apache/incubator-seatunnel/pull/3083))

- [Sink] [IoTDB]
  - [Improve] Improve IoTDB Sink Connector ([2917](https://github.com/apache/incubator-seatunnel/pull/2917))
    - Support align by sql syntax
    - Support sql split ignore case
    - Support restore split offset to at-least-once
    - Support read timestamp from RowRecord
  - [BugFix] Fix IoTDB connector sink NPE ([3080](https://github.com/apache/incubator-seatunnel/pull/3080))

- [Sink] [JDBC]
  - [BugFix] Fix JDBC split exception ([2904](https://github.com/apache/incubator-seatunnel/pull/2904))
  - [Feature] Support Phoenix JDBC Source ([2499](https://github.com/apache/incubator-seatunnel/pull/2499))
  - [Feature] Support SQL Server JDBC Source ([2646](https://github.com/apache/incubator-seatunnel/pull/2646))
  - [Feature] Support Oracle JDBC Source ([2550](https://github.com/apache/incubator-seatunnel/pull/2550))
  - [Feature] Support StarRocks JDBC Source ([3060](https://github.com/apache/incubator-seatunnel/pull/3060))

- [Sink] [Kudu]
  - [Improve] Kudu Sink Connector Support to upsert row ([2881](https://github.com/apache/incubator-seatunnel/pull/2881))

## [Connector V1]

### [New Connector V1 Added]

### [Improve & Bug Fix]

- [Sink] [Spark-Hbase]
  - [BugFix] Handling null values ([3099](https://github.com/apache/incubator-seatunnel/pull/3099))

## [Starter & Core & API]

### [Feature & Improve]

- [Improve] [Sink] Support define parallelism for sink connector ([2941](https://github.com/apache/incubator-seatunnel/pull/2941))
- [Improve] [all] change Log to @slf4j ([3001](https://github.com/apache/incubator-seatunnel/pull/3001))
- [Improve] [format] [text] Support read & write SeaTunnelRow type ([2969](https://github.com/apache/incubator-seatunnel/pull/2969))
- [Improve] [api] [flink] extraction unified method ([2862](https://github.com/apache/incubator-seatunnel/pull/2862))
- [Feature] [deploy] Add Helm charts ([2903](https://github.com/apache/incubator-seatunnel/pull/2903))
- [Feature] [seatunnel-text-format] ([2884](https://github.com/apache/incubator-seatunnel/pull/2884))
- 
### [Bug Fix]

- [BugFix] Fix assert connector name error in config/plugin_config file ([3127](https://github.com/apache/incubator-seatunnel/pull/3127))
- [BugFix] [starter] Fix connector-v2 flink & spark dockerfile ([3007](https://github.com/apache/incubator-seatunnel/pull/3007))
- [BugFix] [core] Fix spark engine parallelism parameter does not working ([2965](https://github.com/apache/incubator-seatunnel/pull/2965))
- [BugFix] [build] Fix the invalidation of the suppression file of checkstyle in the win10 ([2986](https://github.com/apache/incubator-seatunnel/pull/2986))
- [BugFix] [format] [json] Fix jackson package conflict with spark ([2934](https://github.com/apache/incubator-seatunnel/pull/2934))
- [BugFix] [build] Fix the invalidation of the suppression file of checkstyle in the win10 ([2986](https://github.com/apache/incubator-seatunnel/pull/2986))
- [BugFix] [build] Fix the invalidation of the suppression file of checkstyle in the win10 ([2986](https://github.com/apache/incubator-seatunnel/pull/2986))
- [BugFix] [seatunnel-translation-base] Fix Source restore state NPE ([2878](https://github.com/apache/incubator-seatunnel/pull/2878))

## [Docs]

- Add coding guide ([2995](https://github.com/apache/incubator-seatunnel/pull/2995))

## [SeaTunnel Engine]

### [Feature & Improve]

#### [Cluster Manager]

- Support Run SeaTunnel Engine in stand-alone.
- Support Run SeaTunnel Engine cluster.
- Do not rely on third-party services(zookeeper etc) to realize the master-worker architecture.
- Autonomous cluster (non centralized).
- Automatic discovery of cluster members.

#### [Core]

- Support submit Job to SeaTunnel Engine in local mode.
- Support submit Job to SeaTunnel Engine in cluster mode.
- Support Batch Job.
- Support Stream Job.
- Supports batch stream integration, and the batch stream integration feature of all SeaTunnel V2 Connectors can be guaranteed in SeaTunnel Engine.
- Support Distributed Snapshot algorithm Chandy Ramport algorithm and Two-phase Commit. Exactly-Once semantics based on these implementations.
- Support pipeline granularity job scheduling, Ensure that the job can be started under limited resources.
- Support pipeline granularity job restore.
- Sharing threads between tasks to achieve real-time synchronization of a large number of small datasets.
