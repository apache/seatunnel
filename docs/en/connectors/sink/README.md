# Sink Connectors

Sink connectors are responsible for writing processed data from SeaTunnel to various external destinations such as databases, message queues, file systems, cloud services, and more. SeaTunnel provides a comprehensive set of sink connectors to handle diverse output requirements.

## Quick Start

### Basic Configuration Structure

A typical sink connector configuration in SeaTunnel follows this structure:

```yaml
env {
  execution.parallelism = 2
  job.name = "My Sink Job"
}

source {
  # Define your source connector
  FakeSource {}
}

transform {
  # Optional data transformations
}

sink {
  # Define your sink connector here
  MySinkConnector {
    # Connector-specific options
    option1 = "value1"
    option2 = "value2"
  }
}
```

### Common Sink Configuration Patterns

#### 1. JDBC-based Sinks (MySQL, PostgreSQL, Oracle, etc.)
```yaml
sink {
  Jdbc {
    url = "jdbc:mysql://localhost:3306/mydb"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "user"
    password = "password"
    table = "output_table"
    primary_keys = ["id"]
  }
}
```

#### 2. Message Queue Sinks (Kafka, Pulsar, RabbitMQ, etc.)
```yaml
sink {
  Kafka {
    bootstrap.servers = "localhost:9092"
    topic = "output_events"
    format = "json"
  }
}
```

#### 3. File-based Sinks (LocalFile, HdfsFile, S3File, etc.)
```yaml
sink {
  LocalFile {
    path = "/path/to/output"
    format = "json"
    file_name_pattern = "output_${time}"
  }
}
```

#### 4. Specialized Sinks (Email, Slack, Console, etc.)
```yaml
sink {
  Console {
    log.print.data = true
  }
}
```

## Supported Engines

Most sink connectors support multiple SeaTunnel engines:

- ✅ **Spark**: For batch processing workloads
- ✅ **Flink**: For both batch and stream processing
- ✅ **SeaTunnel Zeta**: The built-in engine for high-performance data processing

*Note: Check individual connector documentation for specific engine support.*

## Key Features

Sink connectors may support various features:

- 🎯 **Exactly-Once**: Guarantee data is written exactly once
- 📊 **Batch Mode**: Process data in discrete batches
- 🔄 **Stream Mode**: Process data continuously as it arrives
- ⚡ **Parallelism**: Write data in parallel for better performance
- 🔧 **Schema Evolution**: Handle schema changes dynamically
- 🏷️ **Partitioning**: Partition data by specific columns or time

## Connector Categories

### Database Sinks
- **Relational Databases**: MySQL, PostgreSQL, Oracle, SQL Server, DB2, etc.
- **NoSQL Databases**: MongoDB, Cassandra, HBase, Redis, etc.
- **Data Warehouses**: ClickHouse, Doris, StarRocks, Redshift, etc.
- **Graph Databases**: Neo4j, HugeGraph, etc.

### Message Queue Sinks
- Apache Kafka, Apache Pulsar, RabbitMQ, Amazon SQS, RocketMQ, ActiveMQ

### File System Sinks
- **Local Files**: LocalFile
- **Cloud Storage**: S3File, OSSFile, HdfsFile, CosFile, ObsFile
- **FTP/SFTP**: FtpFile, SftpFile

### Communication & Notification Sinks
- **Email**: Email
- **Messaging**: Slack, DingTalk, Enterprise WeChat, Feishu
- **Monitoring**: Sentry, SensorsData

### Specialized Sinks
- **Time Series**: InfluxDB, IoTDB, TDengine, Prometheus
- **Search Engines**: Elasticsearch, Easysearch, Typesense
- **Vector Databases**: Milvus, Qdrant
- **Data Lake**: Iceberg, Hudi, Paimon
- **Cloud Services**: Snowflake, MaxCompute, Datahub, Google Firestore

## A-Z Connector Index

### A
- [Activemq](Activemq.md) - Apache ActiveMQ message broker
- [Aerospike](Aerospike.md) - Aerospike NoSQL database
- [AmazonDynamoDB](AmazonDynamoDB.md) - AWS NoSQL database service
- [AmazonSqs](AmazonSqs.md) - AWS Simple Queue Service
- [Assert](Assert.md) - Data assertion and validation sink

### B
*(No connectors starting with B)*

### C
- [Cassandra](Cassandra.md) - Distributed NoSQL database
- [Clickhouse](Clickhouse.md) - Column-oriented database management system
- [ClickhouseFile](ClickhouseFile.md) - ClickHouse file format sink
- [Cloudberry](Cloudberry.md) - Cloud-native database
- [Console](Console.md) - Console output for debugging and testing
- [CosFile](CosFile.md) - Tencent Cloud COS storage

### D
- [Databend](Databend.md) - Cloud data warehouse
- [DB2](DB2.md) - IBM DB2 database
- [Datahub](Datahub.md) - LinkedIn DataHub metadata platform
- [DingTalk](DingTalk.md) - DingTalk notification sink
- [Doris](Doris.md) - Real-time analytical database
- [Druid](Druid.md) - Apache Druid analytical database

### E
- [Easysearch](Easysearch.md) - Search engine
- [Elasticsearch](Elasticsearch.md) - Distributed search and analytics engine
- [Email](Email.md) - Email notification sink
- [Enterprise-WeChat](Enterprise-WeChat.md) - Enterprise WeChat notification sink

### F
- [Feishu](Feishu.md) - Feishu (Lark) notification sink
- [Fluss](Fluss.md) - Apache Fluss streaming storage
- [FtpFile](FtpFile.md) - FTP file system sink

### G
- [GoogleFirestore](GoogleFirestore.md) - Google Cloud Firestore database
- [GraphQL](GraphQL.md) - GraphQL API sink
- [Greenplum](Greenplum.md) - PostgreSQL-based data warehouse

### H
- [Hbase](Hbase.md) - NoSQL HBase database
- [HdfsFile](HdfsFile.md) - HDFS distributed file system
- [Hive](Hive.md) - Apache Hive data warehouse
- [Http](Http.md) - HTTP/REST API sink
- [Hudi](Hudi.md) - Apache Hudi data lake platform
- [HugeGraph](HugeGraph.md) - HugeGraph database

### I
- [Iceberg](Iceberg.md) - Apache Iceberg table format
- [InfluxDB](InfluxDB.md) - Time series database
- [IoTDB](IoTDB.md) - Apache IoTDB time series database
- [IoTDBv2](IoTDBv2.md) - Apache IoTDB v2 time series database

### J
- [Jdbc](Jdbc.md) - Generic JDBC database sink

### K
- [Kafka](Kafka.md) - Apache Kafka distributed streaming platform
- [Kingbase](Kingbase.md) - Kingbase database
- [Kudu](Kudu.md) - Apache Kudu storage system

### L
- [LocalFile](LocalFile.md) - Local file system sink

### M
- [Maxcompute](Maxcompute.md) - Alibaba Cloud MaxCompute
- [Milvus](Milvus.md) - Vector database for AI applications
- [MongoDB](MongoDB.md) - MongoDB NoSQL database
- [Mysql](Mysql.md) - MySQL relational database

### N
- [Neo4j](Neo4j.md) - Neo4j graph database

### O
- [ObsFile](ObsFile.md) - Huawei Cloud OBS storage
- [OceanBase](OceanBase.md) - OceanBase distributed database
- [Oracle](Oracle.md) - Oracle relational database
- [OssFile](OssFile.md) - Alibaba Cloud OSS storage
- [OssJindoFile](OssJindoFile.md) - Alibaba Cloud OSS Jindo storage

### P
- [Paimon](Paimon.md) - Apache Paimon data lake format
- [Phoenix](Phoenix.md) - Apache Phoenix SQL layer for HBase
- [PostgreSql](PostgreSql.md) - PostgreSQL relational database
- [Prometheus](Prometheus.md) - Prometheus monitoring system
- [Pulsar](Pulsar.md) - Apache Pulsar messaging platform

### Q
- [Qdrant](Qdrant.md) - Qdrant vector database

### R
- [Rabbitmq](Rabbitmq.md) - RabbitMQ message broker
- [Redis](Redis.md) - Redis in-memory data structure store
- [Redshift](Redshift.md) - AWS Redshift data warehouse
- [RocketMQ](RocketMQ.md) - Apache RocketMQ messaging platform

### S
- [S3-Redshift](S3-Redshift.md) - S3 to Redshift data transfer
- [S3File](S3File.md) - Amazon S3 storage
- [SelectDB-Cloud](SelectDB-Cloud.md) - SelectDB Cloud analytical database
- [SensorsData](SensorsData.md) - SensorsData analytics platform
- [Sentry](Sentry.md) - Sentry error tracking platform
- [SftpFile](SftpFile.md) - SFTP file system sink
- [Slack](Slack.md) - Slack messaging platform
- [Sls](Sls.md) - Alibaba Cloud Simple Log Service
- [Snowflake](Snowflake.md) - Snowflake cloud data warehouse
- [Socket](Socket.md) - Socket-based data sink
- [SqlServer](SqlServer.md) - Microsoft SQL Server
- [StarRocks](StarRocks.md) - StarRocks analytical database

### T
- [Tablestore](Tablestore.md) - Alibaba Cloud Tablestore NoSQL database
- [TDengine](TDengine.md) - TDengine time series database
- [Typesense](Typesense.md) - Typesense search engine

### V
- [Vertica](Vertica.md) - Vertica analytical database

## Sink Common Options

Most sink connectors support common configuration options:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| parallelism | int | - | Number of parallel sink instances |
| max_retries | int | 3 | Maximum number of retry attempts |
| retry_wait_time | long | 1000 | Wait time between retries (milliseconds) |
| batch_size | int | 1000 | Number of records per batch |
| batch_interval_ms | long | 1000 | Maximum time to wait before flushing a batch |

## Getting Help

- **Documentation**: Check individual connector documentation for detailed configuration options
- **Community**: Join the [SeaTunnel Community](https://github.com/apache/seatunnel) for support
- **Issues**: Report bugs or request features on [GitHub Issues](https://github.com/apache/seatunnel/issues)

## Contributing

Want to add a new sink connector? Check our [Contributor Guide](../../contributing.md) for details on how to develop and contribute new connectors.

---

*This document covers all available sink connectors in SeaTunnel. For specific connector configuration details, please refer to the individual connector documentation pages.*