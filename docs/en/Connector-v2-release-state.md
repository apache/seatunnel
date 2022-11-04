## Connector Release Status
SeaTunnel uses a grading system for connectors to help you understand what to expect from a connector:

|                      | Alpha                                                                                                                                                                                                            | Beta                                                                                                                                                                                                                                       | General Availability (GA)                                                                                                                                                                                      |
|----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Expectations         | An alpha connector signifies a connector under development and helps SeaTunnel gather early feedback and issues reported by early adopters. We strongly discourage using alpha releases for production use cases | A beta connector is considered stable and reliable with no backwards incompatible changes but has not been validated by a broader group of users. We expect to find and fix a few issues and bugs in the release before it’s ready for GA. | A generally available connector has been deemed ready for use in a production environment and is officially supported by SeaTunnel. Its documentation is considered sufficient to support widespread adoption. |
|                      |                                                                                                                                                                                                                  |                                                                                                                                                                                                                                            |                                                                                                                                                                                                                |
| Production Readiness | No                                                                                                                                                                                                               | Yes                                                                                                                                                                                                                                        | Yes                                                                                                                                                                                                            |

## Connector V2 Health

| Connector Name                                              | Type   | Status | Support Version |
|-------------------------------------------------------------|--------|--------|-----------------|
| [Asset](connector-v2/sink/Assert.md)                        | Sink   | Beta   | 2.2.0-beta      |
| [ClickHouse](connector-v2/source/Clickhouse.md)             | Source | Beta   | 2.2.0-beta      |
| [ClickHouse](connector-v2/sink/Clickhouse.md)               | Sink   | Beta   | 2.2.0-beta      |
| [ClickHouseFile](connector-v2/sink/ClickhouseFile.md)       | Sink   | Beta   | 2.2.0-beta      |
| [Console](connector-v2/sink/Console.md)                     | Sink   | Beta   | 2.2.0-beta      |
| [DataHub](connector-v2/sink/Datahub.md)                     | Sink   | Alpha  | 2.2.0-beta      |
| [DingTalk](connector-v2/sink/dingtalk.md)                   | Sink   | Alpha  | 2.2.0-beta      |
| [Elasticsearch](connector-v2/sink/Elasticsearch.md)         | Sink   | Beta   | 2.2.0-beta      |
| [Email](connector-v2/sink/Email.md)                         | Sink   | Alpha  | 2.2.0-beta      |
| [Enterprise WeChat](connector-v2/sink/Enterprise-WeChat.md) | Sink   | Alpha  | 2.2.0-beta      |
| [FeiShu](connector-v2/sink/Feishu.md)                       | Sink   | Alpha  | 2.2.0-beta      |
| [Fake](connector-v2/source/FakeSource.md)                   | Source | Beta   | 2.2.0-beta      |
| [FtpFile](connector-v2/sink/FtpFile.md)                     | Sink   | Alpha  | 2.2.0-beta      |
| [Greenplum](connector-v2/sink/Greenplum.md)                 | Sink   | Alpha  | 2.2.0-beta      |
| [Greenplum](connector-v2/source/Greenplum.md)               | Source | Alpha  | 2.2.0-beta      |
| [HdfsFile](connector-v2/sink/HdfsFile.md)                   | Sink   | Beta   | 2.2.0-beta      |
| [HdfsFile](connector-v2/source/HdfsFile.md)                 | Source | Beta   | 2.2.0-beta      |
| [Hive](connector-v2/sink/Hive.md)                           | Sink   | Beta   | 2.2.0-beta      |
| [Hive](connector-v2/source/Hive.md)                         | Source | Beta   | 2.2.0-beta      |
| [Http](connector-v2/sink/Http.md)                           | Sink   | Beta   | 2.2.0-beta      |
| [Http](connector-v2/source/Http.md)                         | Source | Beta   | 2.2.0-beta      |
| [Hudi](connector-v2/source/Hudi.md)                         | Source | Alpha  | 2.2.0-beta      |
| [Iceberg](connector-v2/source/Iceberg.md)                   | Source | Alpha  | 2.2.0-beta      |
| [IoTDB](connector-v2/source/IoTDB.md)                       | Source | Beta   | 2.2.0-beta      |
| [IoTDB](connector-v2/sink/IoTDB.md)                         | Sink   | Beta   | 2.2.0-beta      |
| [Jdbc](connector-v2/source/Jdbc.md)                         | Source | Beta   | 2.2.0-beta      |
| [Jdbc](connector-v2/sink/Jdbc.md)                           | Sink   | Beta   | 2.2.0-beta      |
| [Kudu](connector-v2/source/Kudu.md)                         | Source | Alpha  | 2.2.0-beta      |
| [Kudu](connector-v2/sink/Kudu.md)                           | Sink   | Alpha  | 2.2.0-beta      |
| [LocalFile](connector-v2/sink/LocalFile.md)                 | Sink   | Beta   | 2.2.0-beta      |
| [LocalFile](connector-v2/source/LocalFile.md)               | Source | Beta   | 2.2.0-beta      |
| [MongoDB](connector-v2/source/MongoDB.md)                   | Source | Beta   | 2.2.0-beta      |
| [MongoDB](connector-v2/sink/MongoDB.md)                     | Sink   | Beta   | 2.2.0-beta      |
| [Neo4j](connector-v2/sink/Neo4j.md)                         | Sink   | Alpha  | 2.2.0-beta      |
| [OssFile](connector-v2/sink/OssFile.md)                     | Sink   | Alpha  | 2.2.0-beta      |
| [OssFile](connector-v2/source/OssFile.md)                   | Source | Beta   | 2.2.0-beta      |
| [Phoenix](connector-v2/sink/Phoenix.md)                     | Sink   | Alpha  | 2.2.0-beta      |
| [Phoenix](connector-v2/source/Phoenix.md)                   | Source | Alpha  | 2.2.0-beta      |
| [Pulsar](connector-v2/source/pulsar.md)                     | Source | Beta   | 2.2.0-beta      |
| [Redis](connector-v2/sink/Redis.md)                         | Sink   | Beta   | 2.2.0-beta      |
| [Redis](connector-v2/source/Redis.md)                       | Source | Alpha  | 2.2.0-beta      |
| [Sentry](connector-v2/sink/Sentry.md)                       | Sink   | Alpha  | 2.2.0-beta      |
| [Socket](connector-v2/sink/Socket.md)                       | Sink   | Alpha  | 2.2.0-beta      |
| [Socket](connector-v2/source/Socket.md)                     | Source | Alpha  | 2.2.0-beta      |
| [Kafka](connector-v2/source/kafka.md)                       | Source | Alpha  | 2.3.0-beta      |
| [Kafka](connector-v2/sink/Kafka.md)                         | Sink   | Alpha  | 2.3.0-beta      |
| [S3File](connector-v2/source/S3File.md)                     | Source | Alpha  | 2.3.0-beta      |
| [S3File](connector-v2/sink/S3File.md)                       | Sink   | Alpha  | 2.3.0-beta      |