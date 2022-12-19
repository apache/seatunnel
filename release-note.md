# 2.3.0-release

## Bug fix

### Core

- [Core] [Starter] Fix the bug of ST log print failed in some jdk versions #3160
- [Core] Fix bug that shell script about downloading plugins does not work #3462

### Connector-V2

- [Connector-V2] [Jdbc] Fix the bug that jdbc source can not be stopped in batch mode #3220
- [Connector-V2] [Jdbc] Fix the bug that jdbc connector reset in jdbc connector #3670
- [Connector-V2] [Hive] Fix the following bugs of hive connector: 1. write parquet NullPointerException 2. when restore write from states getting error file path #3258
- [Connector-V2] [File] Fix the bug that when getting file system throw NullPointerException #3506
- [Connector-V2] [Hudi] Fix the bug that the split owner of Hudi connector may be negative #3184
- [Connector-V2] [File] Fix the bug that when user does not config the `fileNameExpression` it will throw NullPointerException #3706

### ST-Engine

- [ST-Engine] Fix bug data file name will duplicate when use SeaTunnel Engine #3717
- [ST-Engine] Fix job restart of all nodes down #3722
- [ST-Engine] Fix the bug that checkpoint stuck in ST-Engine #3213

### E2E

- [E2E] [Spark] Corrected spark version in e2e container #3225

## Improve

### Core

- [Core] [Starter] [Flink] Upgrade the method of loading extra jars in flink starter #2982

### Connector-V1

- [Connector-V1] Remove connector v1 related codes from dev branch #3450

### Connector-V2

- [Connector-V2] Add split templates for all connectors #3335
- [Connector-V2] [Redis] Support redis cluster mode & user authentication #3188
- [Connector-V2] [Clickhouse] Support nest type and array type in clickhouse connector #3047
- [Connector-V2] [Clickhouse] Support geo type in clickhouse connector #3141
- [Connector-V2] [Clickhouse] Improve double convert that in clickhouse connector #3441
- [Connector-V2] [Clickhouse] Improve float long convert that in clickhouse connector #3471
- [Connector-V2] [Kafka] Support setting read start offset or message time in kafka connector #3157
- [Connector-V2] [Kafka] Support specify multiple partition keys in kafka connector #3230
- [Connector-V2] [Kafka] Support dynamic discover topic & partition in kafka connector #3125
- [Connector-V2] [IotDB] Add the parameter check logic for iotDB sink connector #3412
- [Connector-V2] [Jdbc] Support setting fetch size in jdbc connector #3478
- [Connector-V2] [Jdbc] Support upsert config in jdbc connector #3708
- [Connector-V2] [Jdbc] Optimize the commit process of jdbc connector #3451
- [Connector-V2] [Jdbc] Release jdbc resource when after using #3358
- [Connector-V2] [Oracle] Improve data type mapping of Oracle connector #3486
- [Connector-V2] [Http] Support extract complex json string in http connector #3510
- [Connector-V2] [File] [S3] Support s3a protocol in S3 file connector #3632
- [Connector-V2] [File] Support file split in file connectors #3625
- [Connector-V2] [CDC] Support write cdc changelog event in elsticsearch sink connector #3673
- [Connector-V2] [CDC] Support write cdc changelog event in clickhouse sink connector #3653
- [Conncetor-V2] [CDC] Support write cdc changelog event in jdbc connector #3444
- [Connector-V2] [Kafka] Support text format for kafka connector #3711

### ST-Engine

- [ST-Engine] Improve statistic information print format that in ST-Engine #3492
- [ST-Engine] Improve ST-Engine performance #3216
- [ST-Engine] Support user-defined jvm parameters in ST-Engine #3307

### CI

- [CI] Improve CI process #3179 #3194

### E2E

- [E2E] [Flink] Support execute extra commands on task-manager container #3224
- [E2E] [Jdbc] Increased Jdbc e2e stability #3234

## Feature

### Core

- [Core] [Log] Integrate slf4j and log4j2 for unified management logs #3025
- [Core] [Connector-V2] [Exception] Unified exception API & Unified connector error tip message #3045

### Connector-V2

- [Connector-V2] [Elasticsearch] Add elasticsearch source connector #2821
- [Connector-V2] [AmazondynamoDB] Add AmazondynamoDB source & sink connector #3166
- [Connector-V2] [StarRocks] Add StarRocks sink connector #3164
- [Connector-V2] [DB2] Add DB2 source & sink connector #2410
- [Connector-V2] [Transform] Add transform-v2 api #3145
- [Connector-V2] [InfluxDB] Add influxDB sink connector #3174
- [Connector-V2] [Cassandra] Add Cassandra Source & Sink connector #3229
- [Connector-V2] [MyHours] Add MyHours source connector #3228
- [Connector-V2] [Lemlist] Add Lemlist source connector #3346
- [Connector-V2] [CDC] [MySql] Add mysql cdc source connector #3455
- [Connector-V2] [Klaviyo] Add Klaviyo source connector #3443
- [Connector-V2] [OneSingal] Add OneSingal source connector #3454
- [Connector-V2] [Slack] Add slack sink connector #3226
- [Connector-V2] [Jira] Add Jira source connector #3473
- [Connector-V2] [Sqlite] Add Sqlite source & sink connector #3089
- [Connector-V2] [OpenMldb] Add openmldb source connector #3313
- [Connector-V2] [Teradata] Add teradata source & sink connector #3362
- [Connector-V2] [Doris] Add doris source & sink connector #3586
- [Connector-V2] [MaxCompute] Add MaxCompute source & sink connector #3640
- [Connector-V2] [Doris] [Streamload] Add doris streamload sink connector #3631
- [Connector-V2] [Redshift] Add redshift source & sink connector #3615
- [Connector-V2] [Notion] Add notion source connector #3470
- [Connector-V2] [File] [Oss-Jindo] Add oss jindo source & sink connector #3456

### ST-Engine

- [ST-Engine] Support print job metrics when job finished #3691
- [ST-Engine] Add metrics statistic in ST-Engine #3621
- [ST-Engine] Support IMap file storage in ST-Engine #3418
- [ST-Engine] Support S3 file system for IMap file storage #3675

### E2E

- [E2E] [Http] Add http type connector e2e test cases #3340
- [E2E] [File] [Local] Add local file connector e2e test cases #3221

## Docs

- [Docs] [Connector-V2] [Factory] Add TableSourceFactory & TableSinkFactor docs #3343
- [Docs] [Connector-V2] [Schema] Add connector-v2 schema docs #3296
- [Docs] [Connector-V2] [Quick-Manaul] Add error quick reference manual #3437
- [Docs] [README] Improve README and refactored other docs #3619