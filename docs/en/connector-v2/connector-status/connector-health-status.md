## Connector Release Status
SeaTunnel uses a grading system for connectors to help you understand what to expect from a connector:

|                      | Alpha                                                                                                                                                                                                            | Beta                                                                                                                                                                                                                                       | General Availability (GA)                                                                                                                                                                                      |
|----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Expectations         | An alpha connector signifies a connector under development and helps SeaTunnel gather early feedback and issues reported by early adopters. We strongly discourage using alpha releases for production use cases | A beta connector is considered stable and reliable with no backwards incompatible changes but has not been validated by a broader group of users. We expect to find and fix a few issues and bugs in the release before it’s ready for GA. | A generally available connector has been deemed ready for use in a production environment and is officially supported by SeaTunnel. Its documentation is considered sufficient to support widespread adoption. |
|                      |                                                                                                                                                                                                                  |                                                                                                                                                                                                                                            |                                                                                                                                                                                                                |
| Production Readiness | No                                                                                                                                                                                                               | Yes                                                                                                                                                                                                                                        | Yes                                                                                                                                                                                                            |

## Connector V2 Health

| Connector Name                                    | Type   | Status | Support Version |
|---------------------------------------------------|--------|--------|-----------------|
| [Asset](../sink/Assert.md)                        | Sink   | Beta   | 2.2.0-beta      |
| [ClickHouse](../source/Clickhouse.md)             | Source | Beta   | 2.2.0-beta      |
| [ClickHouse](../sink/Clickhouse.md)               | Sink   | Beta   | 2.2.0-beta      |
| [ClickHouseFile](../sink/ClickhouseFile.md)       | Sink   | Beta   | 2.2.0-beta      |
| [Console](../sink/Console.md)                     | Sink   | Beta   | 2.2.0-beta      |
| [DataHub](../sink/Datahub.md)                     | Sink   | Alpha  | 2.2.0-beta      |
| [DingTalk](../sink/dingtalk.md)                   | Sink   | Alpha  | 2.2.0-beta      |
| [Elasticsearch](../sink/Elasticsearch.md)         | Sink   | Beta   | 2.2.0-beta      |
| [Email](../sink/Email.md)                         | Sink   | Alpha  | 2.2.0-beta      |
| [Enterprise WeChat](../sink/Enterprise-WeChat.md) | Sink   | Alpha  | 2.2.0-beta      |
| [FeiShu](../sink/Feishu.md)                       | Sink   | Alpha  | 2.2.0-beta      |
| [Fake](../source/FakeSource.md)                   | Source | Alpha  | 2.2.0-beta      |
| [FtpFile](../sink/FtpFile.md)                     | Sink   | Alpha  | 2.2.0-beta      |
| [Greenplum](../sink/Greenplum.md)                 | Sink   | Alpha  | 2.2.0-beta      |
| [Greenplum](../source/Greenplum.md)               | Source | Alpha  | 2.2.0-beta      |
| [HdfsFile](../sink/HdfsFile.md)                   | Sink   | Beta   | 2.2.0-beta      |
| [HdfsFile](../source/HdfsFile.md)                 | Source | Beta   | 2.2.0-beta      |
| [Hive](../sink/Hive.md)                           | Sink   | Alpha  | 2.2.0-beta      |
| [Hive](../source/Hive.md)                         | Source | Beta   | 2.2.0-beta      |
| [Http](../sink/Http.md)                           | Sink   | Beta   | 2.2.0-beta      |
| [Http](../source/Http.md)                         | Source | Beta   | 2.2.0-beta      |
| [Hudi](../source/Hudi.md)                         | Source | Alpha  | 2.2.0-beta      |
| [Iceberg](../source/Iceberg.md)                   | Source | Alpha  | 2.2.0-beta      |
| [IoTDB](../source/IoTDB.md)                       | Source | Beta   | 2.2.0-beta      |
| [IoTDB](../sink/IoTDB.md)                         | Sink   | Beta   | 2.2.0-beta      |
| [Jdbc](../source/Jdbc.md)                         | Source | Beta   | 2.2.0-beta      |
| [Jdbc](../sink/Jdbc.md)                           | Sink   | Beta   | 2.2.0-beta      |
| [Kudu](../source/Kudu.md)                         | Source | Alpha  | 2.2.0-beta      |
| [Kudu](../sink/Kudu.md)                           | Sink   | Alpha  | 2.2.0-beta      |
| [LocalFile](../sink/LocalFile.md)                 | Sink   | Beta   | 2.2.0-beta      |
| [LocalFile](../source/LocalFile.md)               | Source | Beta   | 2.2.0-beta      |
| [MongoDB](../source/MongoDB.md)                   | Source | Beta   | 2.2.0-beta      |
| [MongoDB](../sink/MongoDB.md)                     | Sink   | Beta   | 2.2.0-beta      |
| [Neo4j](../sink/Neo4j.md)                         | Sink   | Alpha  | 2.2.0-beta      |
| [OssFile](../sink/OssFile.md)                     | Sink   | Alpha  | 2.2.0-beta      |
| [OssFile](../source/OssFile.md)                   | Source | Beta   | 2.2.0-beta      |
| [Phoenix](../sink/Phoenix.md)                     | Sink   | Alpha  | 2.2.0-beta      |
| [Phoenix](../source/Phoenix.md)                   | Source | Alpha  | 2.2.0-beta      |
| [Pulsar](../source/pulsar.md)                     | Source | Beta   | 2.2.0-beta      |
| [Redis](../sink/redis.md)                         | Sink   | Beta   | 2.2.0-beta      |
| [Redis](../source/redis.md)                       | Source | Alpha  | 2.2.0-beta      |
| [Sentry](../sink/Sentry.md)                       | Sink   | Alpha  | 2.2.0-beta      |
| [Socket](../sink/Socket.md)                       | Sink   | Alpha  | 2.2.0-beta      |
| [Socket](../source/Socket.md)                     | Source | Alpha  | 2.2.0-beta      |
