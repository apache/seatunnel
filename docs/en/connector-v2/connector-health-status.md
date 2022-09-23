## Connector Release Status
SeaTunnel uses a grading system for connectors to help you understand what to expect from a connector:

|                      | Alpha                                                                                                                                                                                                            | Beta                                                                                                                                                                                                                                       | General Availability (GA)                                                                                                                                                                                      |
|----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Expectations         | An alpha connector signifies a connector under development and helps SeaTunnel gather early feedback and issues reported by early adopters. We strongly discourage using alpha releases for production use cases | A beta connector is considered stable and reliable with no backwards incompatible changes but has not been validated by a broader group of users. We expect to find and fix a few issues and bugs in the release before it’s ready for GA. | A generally available connector has been deemed ready for use in a production environment and is officially supported by SeaTunnel. Its documentation is considered sufficient to support widespread adoption. |
|                      |                                                                                                                                                                                                                  |                                                                                                                                                                                                                                            |                                                                                                                                                                                                                |
| Production Readiness | No                                                                                                                                                                                                               | Yes                                                                                                                                                                                                                                        | Yes                                                                                                                                                                                                            |

## Connector V2 Health

| Connector Name    | Type   | Status | Support Version |
|-------------------|--------|--------|----------------|
| Asset             | Sink   | Beta   | 2.2.0-beta     |
| ClickHouse        | Source | Beta   | 2.2.0-beta     |
| ClickHouse        | Sink   | Beta   | 2.2.0-beta     |
| ClickHouseFile    | Sink   | Beta   | 2.2.0-beta     |
| Console           | Source | Beta   | 2.2.0-beta     |
| DataHub           | Sink   | Alpha  | 2.2.0-beta     |
| Elasticsearch     | Sink   | Alpha  | 2.2.0-beta     |
| Email             | Sink   | Alpha  | 2.2.0-beta     |
| Enterprise WeChat | Sink   | Alpha  | 2.2.0-beta     |
| FeiShu            | Sink   | Alpha  | 2.2.0-beta     |
| Fake              | Source | Alpha  | 2.2.0-beta     |
| FtpFile           | Sink   | Alpha  | 2.2.0-beta     |
| FtpFile           | Source | Alpha  | 2.2.0-beta     |
| Greenplum         | Sink   | Alpha  | 2.2.0-beta     |
| Greenplum         | Source | Alpha  | 2.2.0-beta     |
| HdfsFile          | Sink   | Beta   | 2.2.0-beta     |
| HdfsFile          | Source | Beta   | 2.2.0-beta     |
| Hive              | Sink   | Beta   | 2.2.0-beta     |
| Hive              | Source | Beta   | 2.2.0-beta     |
| Http              | Sink   | Alpha  | 2.2.0-beta     |
| Http              | Source | Alpha  | 2.2.0-beta     |
| Hudi              | Source | Alpha   | 2.2.0-beta     |
| Iceberg           | Source | Alpha   | 2.2.0-beta     |
| IotDB             | Source | Beta   | 2.2.0-beta     |
| IotDB             | Sink   | Beta   | 2.2.0-beta     |
| Jdbc              | Source | Beta   | 2.2.0-beta     |
| Jdbc              | Sink   | Beta   | 2.2.0-beta     |
| Kudu              | Source | Alpha   | 2.2.0-beta     |
| Kudu              | Sink   | Alpha   | 2.2.0-beta     |
| LocalFile         | Sink   | Beta   | 2.2.0-beta     |
| LocalFile         | Source | Beta   | 2.2.0-beta     |
| MongoDB           | Source | Alpha  | 2.2.0-beta     |
| MongoDB           | Sink   | Alpha  | 2.2.0-beta     |
| Neo4j             | Sink   | Alpha  | 2.2.0-beta     |
| OssFile           | Sink   | Alpha   | 2.2.0-beta     |
| OssFile           | Source | Alpha   | 2.2.0-beta     |
| Phoneix           | Sink   | Alpha   | 2.2.0-beta     |
| Phoneix           | Source | Alpha   | 2.2.0-beta     |
| Pulsar            | Source | Beta   | 2.2.0-beta     |
| Redis             | Sink   | Alpha   | 2.2.0-beta     |
| Redis             | Source | Alpha   | 2.2.0-beta     |
| Sentry            | Sink   | Alpha   | 2.2.0-beta     |
| Socket            | Sink   | Alpha   | 2.2.0-beta     |
| Socket            | Source | Alpha   | 2.2.0-beta     |
| DingTalk          | Sink   | Alpha   | 2.2.0-beta     |