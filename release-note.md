# 2.1.2 Release

[Feature]
- Add Spark webhook source
- Support Flink application mode
- Split connector jar from core jar
- Add Replace transforms for Spark
- Add Uuid transform for Spark
- Support Flink dynamic configurations
- Flink JDBC source support Oracle database
- Add Flink connector Http
- Add Flink transform for register user define function
- Add Flink SQL Kafka, ElasticSearch connector

[Bugfix]
- Fixed ClickHouse sink data type convert error
- Fixed first execute Spark start shell can't run problem
- Fixed can not get config file when use Spark on yarn cluster mode
- Fixed Spark extraJavaOptions can't be empty
- Fixed the "plugins.tar.gz" decompression failure in Spark standalone cluster mode 
- Fixed Clickhouse sink can not work correctly when use multiple hosts
- Fixed Flink sql conf parse exception
- Fixed Flink JDBC Mysql datatype mapping incomplete
- Fixed variables cannot be set in Flink mode
- Fixed SeaTunnel Flink engine cannot check source config

[Improvement]
- Update Jackson version to 2.12.6
- Add guide on how to Set Up SeaTunnel with Kubernetes
- Optimize some generic type code
- Add Flink SQL e2e module
- Flink JDBC connector add pre sql and post sql
- Use @AutoService to generate SPI file
- Support Flink FakeSourceStream to mock data
- Support Read Hive by Flink JDBC source
- ClickhouseFile support ReplicatedMergeTree
- Support use Hive sink to save table as ORCFileFormat
- Support Spark Redis sink custom expire time
- Add Spark JDBC isolationLevel config
- Use Jackson replace Fastjson 