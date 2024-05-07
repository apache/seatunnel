# 连接器发布状态

SeaTunnel 使用连接器分级系统来帮助您了解连接器的期望：

|                      |                                    Alpha                                     |                                    Beta                                    |                  General Availability (GA)                   |
|----------------------|------------------------------------------------------------------------------|----------------------------------------------------------------------------|--------------------------------------------------------------|
| Expectations         | alpha 连接器表示正在开发的连接器，可帮助 SeaTunnel 收集早期采用者报告的早期反馈和问题。 我们强烈反对在生产用例中使用 alpha 版本 | Beta 连接器被认为稳定可靠，没有向后不兼容的更改，但尚未得到更广泛的用户群体的验证。 我们希望在正式发布之前找到并修复该版本中的一些问题和错误。 | 普遍可用的连接器已被认为可以在生产环境中使用，并得到 SeaTunnel 的正式支持。 它的文档被认为足以支持广泛采用。 |
|                      |                                                                              |                                                                            |                                                              |
| Production Readiness | No                                                                           | Yes                                                                        | Yes                                                          |

## Connector V2 Health

|                          Connector Name                           |  Type  | Status | Support Version |
|-------------------------------------------------------------------|--------|--------|-----------------|
| [AmazonDynamoDB](../en/connector-v2/sink/AmazonDynamoDB.md)       | Sink   | Beta   | 2.3.0           |
| [AmazonDynamoDB](../en/connector-v2/source/AmazonDynamoDB.md)     | Source | Beta   | 2.3.0           |
| [Asset](../en/connector-v2/sink/Assert.md)                        | Sink   | Beta   | 2.2.0-beta      |
| [Cassandra](../en/connector-v2/sink/Cassandra.md)                 | Sink   | Beta   | 2.3.0           |
| [Cassandra](../en/connector-v2/source/Cassandra.md)               | Source | Beta   | 2.3.0           |
| [ClickHouse](../en/connector-v2/source/Clickhouse.md)             | Source | GA     | 2.2.0-beta      |
| [ClickHouse](../en/connector-v2/sink/Clickhouse.md)               | Sink   | GA     | 2.2.0-beta      |
| [ClickHouseFile](../en/connector-v2/sink/ClickhouseFile.md)       | Sink   | GA     | 2.2.0-beta      |
| [Console](connector-v2/sink/Console.md)                           | Sink   | GA     | 2.2.0-beta      |
| [DataHub](../en/connector-v2/sink/Datahub.md)                     | Sink   | Alpha  | 2.2.0-beta      |
| [Doris](../en/connector-v2/sink/Doris.md)                         | Sink   | Beta   | 2.3.0           |
| [DingTalk](../en/connector-v2/sink/DingTalk.md)                   | Sink   | Alpha  | 2.2.0-beta      |
| [Elasticsearch](connector-v2/sink/Elasticsearch.md)               | Sink   | GA     | 2.2.0-beta      |
| [Email](connector-v2/sink/Email.md)                               | Sink   | Alpha  | 2.2.0-beta      |
| [Enterprise WeChat](../en/connector-v2/sink/Enterprise-WeChat.md) | Sink   | Alpha  | 2.2.0-beta      |
| [FeiShu](connector-v2/sink/Feishu.md)                             | Sink   | Alpha  | 2.2.0-beta      |
| [Fake](../en/connector-v2/source/FakeSource.md)                   | Source | GA     | 2.2.0-beta      |
| [FtpFile](../en/connector-v2/sink/FtpFile.md)                     | Sink   | Beta   | 2.2.0-beta      |
| [Greenplum](../en/connector-v2/sink/Greenplum.md)                 | Sink   | Beta   | 2.2.0-beta      |
| [Greenplum](../en/connector-v2/source/Greenplum.md)               | Source | Beta   | 2.2.0-beta      |
| [HdfsFile](connector-v2/sink/HdfsFile.md)                         | Sink   | GA     | 2.2.0-beta      |
| [HdfsFile](connector-v2/source/HdfsFile.md)                       | Source | GA     | 2.2.0-beta      |
| [Hive](../en/connector-v2/sink/Hive.md)                           | Sink   | GA     | 2.2.0-beta      |
| [Hive](../en/connector-v2/source/Hive.md)                         | Source | GA     | 2.2.0-beta      |
| [Http](connector-v2/sink/Http.md)                                 | Sink   | Beta   | 2.2.0-beta      |
| [Http](../en/connector-v2/source/Http.md)                         | Source | Beta   | 2.2.0-beta      |
| [Hudi](../en/connector-v2/source/Hudi.md)                         | Source | Beta   | 2.2.0-beta      |
| [Iceberg](../en/connector-v2/source/Iceberg.md)                   | Source | Beta   | 2.2.0-beta      |
| [InfluxDB](../en/connector-v2/sink/InfluxDB.md)                   | Sink   | Beta   | 2.3.0           |
| [InfluxDB](../en/connector-v2/source/InfluxDB.md)                 | Source | Beta   | 2.3.0-beta      |
| [IoTDB](../en/connector-v2/source/IoTDB.md)                       | Source | GA     | 2.2.0-beta      |
| [IoTDB](../en/connector-v2/sink/IoTDB.md)                         | Sink   | GA     | 2.2.0-beta      |
| [Jdbc](../en/connector-v2/source/Jdbc.md)                         | Source | GA     | 2.2.0-beta      |
| [Jdbc](connector-v2/sink/Jdbc.md)                                 | Sink   | GA     | 2.2.0-beta      |
| [Kafka](../en/connector-v2/source/kafka.md)                       | Source | GA     | 2.3.0           |
| [Kafka](connector-v2/sink/Kafka.md)                               | Sink   | GA     | 2.2.0-beta      |
| [Kudu](../en/connector-v2/source/Kudu.md)                         | Source | Beta   | 2.2.0-beta      |
| [Kudu](../en/connector-v2/sink/Kudu.md)                           | Sink   | Beta   | 2.2.0-beta      |
| [Lemlist](../en/connector-v2/source/Lemlist.md)                   | Source | Beta   | 2.3.0           |
| [LocalFile](../en/connector-v2/sink/LocalFile.md)                 | Sink   | GA     | 2.2.0-beta      |
| [LocalFile](../en/connector-v2/source/LocalFile.md)               | Source | GA     | 2.2.0-beta      |
| [Maxcompute]../en/(connector-v2/source/Maxcompute.md)             | Source | Alpha  | 2.3.0           |
| [Maxcompute](../en/connector-v2/sink/Maxcompute.md)               | Sink   | Alpha  | 2.3.0           |
| [MongoDB](../en/connector-v2/source/MongoDB.md)                   | Source | Beta   | 2.2.0-beta      |
| [MongoDB](../en/connector-v2/sink/MongoDB.md)                     | Sink   | Beta   | 2.2.0-beta      |
| [MyHours](../en/connector-v2/source/MyHours.md)                   | Source | Alpha  | 2.2.0-beta      |
| [MySqlCDC](../en/connector-v2/source/MySQL-CDC.md)                | Source | GA     | 2.3.0           |
| [Neo4j](../en/connector-v2/sink/Neo4j.md)                         | Sink   | Beta   | 2.2.0-beta      |
| [Notion](../en/connector-v2/source/Notion.md)                     | Source | Alpha  | 2.3.0           |
| [OneSignal](../en/connector-v2/source/OneSignal.md)               | Source | Beta   | 2.3.0           |
| [OpenMldb](../en/connector-v2/source/OpenMldb.md)                 | Source | Beta   | 2.3.0           |
| [OssFile](../en/connector-v2/sink/OssFile.md)                     | Sink   | Beta   | 2.2.0-beta      |
| [OssFile](../en/connector-v2/source/OssFile.md)                   | Source | Beta   | 2.2.0-beta      |
| [Phoenix](../en/connector-v2/sink/Phoenix.md)                     | Sink   | Beta   | 2.2.0-beta      |
| [Phoenix](../en/connector-v2/source/Phoenix.md)                   | Source | Beta   | 2.2.0-beta      |
| [Pulsar](../en/connector-v2/source/Pulsar.md)                     | Source | Beta   | 2.2.0-beta      |
| [RabbitMQ](../en/connector-v2/sink/Rabbitmq.md)                   | Sink   | Beta   | 2.3.0           |
| [RabbitMQ](../en/connector-v2/source/Rabbitmq.md)                 | Source | Beta   | 2.3.0           |
| [Redis](../en/connector-v2/sink/Redis.md)                         | Sink   | Beta   | 2.2.0-beta      |
| [Redis](../en/connector-v2/source/Redis.md)                       | Source | Beta   | 2.2.0-beta      |
| [S3Redshift](../en/connector-v2/sink/S3-Redshift.md)              | Sink   | GA     | 2.3.0-beta      |
| [S3File](../en/connector-v2/source/S3File.md)                     | Source | GA     | 2.3.0-beta      |
| [S3File](../en/connector-v2/sink/S3File.md)                       | Sink   | GA     | 2.3.0-beta      |
| [Sentry](../en/connector-v2/sink/Sentry.md)                       | Sink   | Alpha  | 2.2.0-beta      |
| [SFtpFile](../en/connector-v2/sink/SftpFile.md)                   | Sink   | Beta   | 2.3.0           |
| [SFtpFile](../en/connector-v2/source/SftpFile.md)                 | Source | Beta   | 2.3.0           |
| [Slack](../en/connector-v2/sink/Slack.md)                         | Sink   | Beta   | 2.3.0           |
| [Socket](../en/connector-v2/sink/Socket.md)                       | Sink   | Beta   | 2.2.0-beta      |
| [Socket](../en/connector-v2/source/Socket.md)                     | Source | Beta   | 2.2.0-beta      |
| [StarRocks](../en/connector-v2/sink/StarRocks.md)                 | Sink   | Alpha  | 2.3.0           |
| [Tablestore](../en/connector-v2/sink/Tablestore.md)               | Sink   | Alpha  | 2.3.0           |

