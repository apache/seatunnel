# Source Connectors

Source connectors are responsible for reading data from various external data sources and bringing it into SeaTunnel for processing. SeaTunnel supports a wide range of source connectors including databases, message queues, file systems, cloud services, and more.

## Quick Start

### Basic Configuration Structure

A typical source connector configuration in SeaTunnel follows this structure:

```yaml
env {
  execution.parallelism = 2
  job.name = "My Source Job"
}

source {
  # Define your source connector here
  MySourceConnector {
    # Connector-specific options
    option1 = "value1"
    option2 = "value2"
  }
}

transform {
  # Optional data transformations
}

sink {
  # Define your sink connector
  Console {}
}
```

### Common Source Configuration Patterns

#### 1. JDBC-based Sources (MySQL, PostgreSQL, Oracle, etc.)
```yaml
source {
  Jdbc {
    url = "jdbc:mysql://localhost:3306/mydb"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "user"
    password = "password"
    query = "SELECT id, name, email FROM users WHERE active = true"
  }
}
```

#### 2. Message Queue Sources (Kafka, Pulsar, RabbitMQ, etc.)
```yaml
source {
  Kafka {
    bootstrap.servers = "localhost:9092"
    topic = "user_events"
    consumer.group = "seatunnel_consumer"
    format = "json"
  }
}
```

#### 3. File-based Sources (LocalFile, HdfsFile, S3File, etc.)
```yaml
source {
  LocalFile {
    path = "/path/to/data/*.csv"
    format = "csv"
    schema = {
      fields {
        id = "int"
        name = "string"
        email = "string"
      }
    }
  }
}
```

#### 4. CDC Sources (MySQL-CDC, PostgreSQL-CDC, etc.)
```yaml
source {
  MySQL-CDC {
    username = "user"
    password = "password"
    database-names = ["mydb"]
    table-names = ["mydb.users", "mydb.orders"]
    base-url = "jdbc:mysql://localhost:3306"
  }
}
```

## Supported Engines

Most source connectors support multiple SeaTunnel engines:

- ✅ **Spark**: For batch processing workloads
- ✅ **Flink**: For both batch and stream processing
- ✅ **SeaTunnel Zeta**: The built-in engine for high-performance data processing

*Note: Check individual connector documentation for specific engine support.*

## Key Features

Source connectors may support various features:

- 📊 **Batch Mode**: Process data in discrete batches
- 🔄 **Stream Mode**: Process data continuously as it arrives
- 🎯 **Exactly-Once**: Guarantee data is processed exactly once
- 📋 **Column Projection**: Select only specific columns from source
- ⚡ **Parallelism**: Process data in parallel for better performance
- 🔧 **Custom Split**: Define custom data partitioning strategies

## Connector Categories

### Database Sources
- **Relational Databases**: MySQL, PostgreSQL, Oracle, SQL Server, DB2, etc.
- **NoSQL Databases**: MongoDB, Cassandra, HBase, Redis, etc.
- **Data Warehouses**: ClickHouse, Doris, StarRocks, etc.
- **CDC Sources**: MySQL-CDC, PostgreSQL-CDC, Oracle-CDC, etc.

### Message Queue Sources
- Apache Kafka, Apache Pulsar, RabbitMQ, Amazon SQS, RocketMQ

### File System Sources
- **Local Files**: LocalFile
- **Cloud Storage**: S3File, OSSFile, GCSFile, HdfsFile
- **FTP/SFTP**: FtpFile, SftpFile

### Specialized Sources
- **Time Series**: InfluxDB, IoTDB, Prometheus
- **Search Engines**: Elasticsearch, Easysearch, Typesense
- **Vector Databases**: Milvus, Qdrant
- **APIs**: HTTP, GitHub, GitLab, Jira, Notion
- **SaaS Platforms**: Google Sheets, Klaviyo, OneSignal, etc.

## A-Z Connector Index

### A
- [AmazonDynamoDB](AmazonDynamoDB.md) - AWS NoSQL database service
- [AmazonSqs](AmazonSqs.md) - AWS Simple Queue Service

### B
*(No connectors starting with B)*

### C
- [Cassandra](Cassandra.md) - Distributed NoSQL database
- [Clickhouse](Clickhouse.md) - Column-oriented database management system
- [Cloudberry](Cloudberry.md) - Cloud-native database

### D
- [Databend](Databend.md) - Cloud data warehouse
- [DB2](DB2.md) - IBM DB2 database
- [Doris](Doris.md) - Real-time analytical database

### E
- [Easysearch](Easysearch.md) - Search engine
- [Elasticsearch](Elasticsearch.md) - Distributed search and analytics engine

### F
- [FakeSource](FakeSource.md) - Built-in test data generator
- [FtpFile](FtpFile.md) - FTP file system source

### G
- [Github](Github.md) - GitHub API source
- [Gitlab](Gitlab.md) - GitLab API source
- [GoogleSheets](GoogleSheets.md) - Google Sheets API source
- [GraphQL](GraphQL.md) - GraphQL API source
- [Greenplum](Greenplum.md) - PostgreSQL-based data warehouse

### H
- [Hbase](Hbase.md) - NoSQL HBase database
- [HdfsFile](HdfsFile.md) - HDFS distributed file system
- [Hive](Hive.md) - Apache Hive data warehouse
- [HiveJdbc](HiveJdbc.md) - Hive JDBC source
- [Http](Http.md) - HTTP/REST API source

### I
- [Iceberg](Iceberg.md) - Apache Iceberg table format
- [InfluxDB](InfluxDB.md) - Time series database
- [IoTDB](IoTDB.md) - Apache IoTDB time series database
- [IoTDBv2](IoTDBv2.md) - Apache IoTDB v2 time series database

### J
- [Jdbc](Jdbc.md) - Generic JDBC database source
- [Jira](Jira.md) - Atlassian Jira API source

### K
- [Kafka](Kafka.md) - Apache Kafka distributed streaming platform
- [Kingbase](Kingbase.md) - Kingbase database
- [Klaviyo](Klaviyo.md) - Klaviyo marketing platform API
- [Kudu](Kudu.md) - Apache Kudu storage system

### L
- [Lemlist](Lemlist.md) - Lemlist API source
- [LocalFile](LocalFile.md) - Local file system source

### M
- [Maxcompute](Maxcompute.md) - Alibaba Cloud MaxCompute
- [Milvus](Milvus.md) - Vector database for AI applications
- [MongoDB-CDC](MongoDB-CDC.md) - MongoDB Change Data Capture
- [MongoDB](MongoDB.md) - MongoDB NoSQL database
- [MySQL-CDC](MySQL-CDC.md) - MySQL Change Data Capture
- [MyHours](MyHours.md) - MyHours time tracking API
- [Mysql](Mysql.md) - MySQL relational database

### N
- [Neo4j](Neo4j.md) - Neo4j graph database
- [Notion](Notion.md) - Notion API source

### O
- [ObsFile](ObsFile.md) - Huawei Cloud OBS storage
- [OceanBase](OceanBase.md) - OceanBase distributed database
- [OneSignal](OneSignal.md) - OneSignal push notification service
- [Opengauss-CDC](Opengauss-CDC.md) - OpenGauss Change Data Capture
- [OpenMldb](OpenMldb.md) - OpenMLDB database
- [Oracle-CDC](Oracle-CDC.md) - Oracle Change Data Capture
- [Oracle](Oracle.md) - Oracle relational database
- [OssFile](OssFile.md) - Alibaba Cloud OSS storage
- [OssJindoFile](OssJindoFile.md) - Alibaba Cloud OSS Jindo storage

### P
- [Paimon](Paimon.md) - Apache Paimon data lake format
- [Persistiq](Persistiq.md) - Persistiq API source
- [Phoenix](Phoenix.md) - Apache Phoenix SQL layer for HBase
- [PostgreSQL-CDC](PostgreSQL-CDC.md) - PostgreSQL Change Data Capture
- [PostgreSQL](PostgreSQL.md) - PostgreSQL relational database
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
- [S3File](S3File.md) - Amazon S3 storage
- [SftpFile](SftpFile.md) - SFTP file system source
- [Sls](Sls.md) - Alibaba Cloud Simple Log Service
- [Snowflake](Snowflake.md) - Snowflake cloud data warehouse
- [Socket](Socket.md) - Socket-based data source
- [SqlServer-CDC](SqlServer-CDC.md) - SQL Server Change Data Capture
- [SqlServer](SqlServer.md) - Microsoft SQL Server
- [StarRocks](StarRocks.md) - StarRocks analytical database

### T
- [Tablestore](Tablestore.md) - Alibaba Cloud Tablestore NoSQL database
- [TDengine](TDengine.md) - TDengine time series database
- [TiDB-CDC](TiDB-CDC.md) - TiDB Change Data Capture
- [Typesense](Typesense.md) - Typesense search engine

### V
- [Vertica](Vertica.md) - Vertica analytical database

### W
- [Web3j](Web3j.md) - Web3j blockchain data source

## Getting Help

- **Documentation**: Check individual connector documentation for detailed configuration options
- **Community**: Join the [SeaTunnel Community](https://github.com/apache/seatunnel) for support
- **Issues**: Report bugs or request features on [GitHub Issues](https://github.com/apache/seatunnel/issues)

## Contributing

Want to add a new source connector? Check our [Contributor Guide](../../contributing.md) for details on how to develop and contribute new connectors.

---

*This document covers all available source connectors in SeaTunnel. For specific connector configuration details, please refer to the individual connector documentation pages.*