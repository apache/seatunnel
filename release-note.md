# 2.3.1-release

## Bug fix

### Core

- [Core] [Shade] [Hadoop] Fix hadoop shade dependency can not be used (#3835)
- [Core] Fix Handover using linked blocking queue cause the oom (#3469)
- [Core] Fix a bug of Spark Translation when has a timestamp/date type filed in source (#4226)
- [Core] Remove unnecessary row conversion (#4335)

### Connector-V2

- [Connecor-V2] [JDBC] Fix the problem that can not throw exception correctly (#3796)
- [Connector-V2] [JDBC] Fix xa transaction commit failure on pipeline restore (#3809)
- [Connector-V2] [Clickhouse] Fix the serializable problems of `Committer` (#3803)
- [Connector-V2] [Clickhouse] Fix the performance bug (#3910)
- [Connector-v2] [Clickhouse] Fix clickhouse write cdc changelog update event (#3951)
- [Connector-V2] [Clickhouse] Clickhouse File Connector failed to sink to table with settings like storage_policy (#4172)
- [Connector-V2] [Clickhouse] Clickhouse File Connector not support split mode for write data to all shards of distributed table (#4035)
- [Connector-V2] [File] Fix the error type of option rule `path` (#3804)
- [Connector-V2] [File] Text And Json WriteStrategy lost the sinkColumnsIndexInRow (#3863)
- [Connector-V2] [Kafka] Fix the bug that can not parse offset format (#3810)
- [Connecor-V2] [Kafka] Fix the default value of commit_on_checkpoint (#3831)
- [Connector-V2] [Kafka] Json deserialize exception log no content (#3874)
- [Connector-V2] [Kafka] Fix commit kafka offset bug. (#3933)
- [Connector-V2] [Kafka] Fix the bug that kafka consumer is not close (#3836)
- [Connector-V2] [Kafka] Fix config option error (#4244)
- [Connector-V2] [CDC] Guaranteed to be exactly-once in the process of switching from SnapshotTask to IncrementalTask (#3837)
- [Connector-V2] [CDC] Fix concurrent modify of splits (#3937)
- [Connector-V2] [CDC] Fix jdbc sink generate update sql (#3940)
- [Connector-V2] [CDC] Fix cdc option rule error (#4018)
- [Connector-V2] [CDC] Fix cdc base shutdown thread not cleared (#4327)
- [Connector-V2] [ALL] Fix ConcurrentModificationException when snapshotState based on SourceReaderBase (#4011)
- [Connector-V2] Fix connector source snapshot state NPE (#4027)
- [Connector-v2] [Pulsar] Fix pulsar option topic-pattern bug (#3989)
- [Connector-V2] [ElasticSearch] Fix es source no data (#4076)
- [Connector-V2] [Hive] Fix hive unknownhost (#4141)
- [Connector-V2] [Pulsar] Fix Pulsar source consumer ack exception (#4237)
- [Connector-V2] [Maxcompute] Fix failed to parse some maxcompute type (#3894)
- [Connector-V2] [Doris] Fix Content Length header already present (#4277)
- [Connector-V2] [ElasticSearch] Fix es field type not support binary (#4274)
- [Connector-V2] [JDBC] Field aliases are not supported in the query of jdbc source (#4210)
- [Json-format] [Canal] Fix json deserialize NPE (#4195)

### Zeta(ST-Engine)

- [Zeta] Fix the bug that checkpoint will be continued trigger after job CANCELED (#3808)
- [Zeta] Fix the bug that the source split can not be deserialized in local mode (#3817)
- [Zeta] Fix Engine Metrics will lose when Job be canceled. (#3797)
- [Zeta] Fix CheckpointIDCounter thread not safe problem (#3875)
- [Zeta] Fix SeatunnelChildFirstClassLoader load jackson error (#3884)
- [Zeta] Fix local mode can not generate logs (#3917)
- [Zeta] Fix actionSubtaskState can be null error (#3902)
- [Zeta] A checkpoint exception may be thrown when the active cancel task terminates with an exception (#3915)
- [Zeta] Fix NPE when scheduling sub plan fails. (#3909)
- [Zeta] Fix NPE when starting from checkpoint. (#3904)
- [Zeta] Fix metrics lose on pipeline restart (#3977)
- [Zeta] Fix Job will lost control when JobMaster init failed (#4045)
- [Zeta] Fix ResourceManager Assign Task Not Random (#4078)
- [Zeta] Fix NullPointerException when registering Reader with the enumerator (#4048)
- [Zeta] Fix clean TaskGroupContext Error when target node is offline (#4086)
- [Zeta] Fix Slot Status Not Sync After Restart When All Node Down (#4047)
- [Zeta] Fix parse job mode bug and improve doc (#4091)
- [Zeta] Fix Client Have Error Can't be Shutdown (#4099)
- [Zeta] Fix zeta bugs
- [Zeta] Fix checkpoint storage namespace (#4260)
- [Zeta] Fix read checkpoint file data is incomplete (#4263)
- [Zeta] The pipeline needs to wait for the CheckpointCoordinator to end (#4272)
- [Zeta] Fix CheckpointCoordinator Can't Trigger Timeout Task (#4276)
- [Zeta] Fix job crash when run it on Seatunnel Engine cluster (#4299)
- [Zeta] Fix Default Cluster Not Working In Config File (#3770)
- [Zeta] Adapt StarRocks With Multi-Table And Single-Table Mode (#4324)
- [Zeta] Fix TaskExecutionService Deploy Failed The Job Can't Stop (#4265)
- [Zeta] Fix cancelJob and checkpoint complete error (#4330)

### E2E

- [Connector-V2] [SQLServer-CDC] Fix the bug of SQLServer-CDC it case (#3807)
- [Connector-V2] [Clickhouse] Fix the bug of clickhouse e2e case (#3985)

## Improve

### Core

- [Core] [API] Add parallelism and column projection interface (#3829)
- [Core] [Connector-V2] Add get source method to all source connector (#3846)
- [Core] [Shade] [Hadoop] Improve hadoop shade by including classes in package com.google.common.cache.* (#3858)
- [Core] Use ReadonlyConfig to avoid option being changed (#4056)
- [Core] Give the maven module a human readable name (#4114)
- [Core] Unified the checkpoint setting key of Flink (#4296)

### Connector-V2

- [Connector-V2] [MySQL-CDC] Add mysql-cdc source factory (#3791)
- [Connector-V2] [MySQL-CDC] Ennable binlog watermark compare (#4293)
- [Connector-V2] [Kafka] Support user-defined client id (#3783)
- [Connector-V2] [Kafka] Support extract topic from SeaTunnelRow field (#3742)
- [Connector-V2] [Kafka] Add Kafka catalog (#4106)
- [Connector-V2] [JDBC] Remove unused options that in jdbc source factory (#3794)
- [Connector-V2] [JDBC] Support exactly-once semantics for JDBC source connector (#3750)
- [Connector-V2] [JDBC] Improve option rule (#3802) (#3864)
- [Connector-V2] [MongoDB] Support use source query in connector (#3697)
- [Connector-V2] [File] Add the exception stack detail for log output (#3805)
- [Connector-V2] [File] Improve file connector option rule and document (#3812)
- [Connector-V2] [File] Support skip number when reading text csv files (#3900)
- [Connector-V2] [File] Allow the user to set the row delimiter as an empty string (#3854)
- [Connector-V2] [File] Support compress (#3899)
- [Connector-V2] [File & Hive] Support kerberos in hive and hdfs file connector (#3840)
- [Connector-V2] [Hive] Improve config check logic (#3886)
- [Connector-V2] [Hive] Support assign partitions (#3842)
- [Connector-V2] [Hive] Support read text table & Column projection (#4105)
- [Connector-V2] [Clickhouse & Kafka & Rabbitmq & StarRocks] Change connector custom config prefix to map (#3719)
- [Connector-V2] [Clickhouse] Special characters in column names are supported (#3881)
- [Connector-V2] [Clickhouse] Remove Clickhouse Fields Config (#3826)
- [Connector-V2] [Email] Unified exception for email connector (#3898)
- [Connector-V2] [Iceberg] Unified exception for iceberg source connector (#3677)
- [Connector-v2] [StarRocks] Support write cdc changelog event(INSERT/UPDATE/DELETE) (#3865)
- [Connector-V2] [Fake] Improve fake connector (#3932)
- [Connector-V2] [Fake] Optimizing Data Generation Strategies (#4061)
- [Connector-V2] [InfluxDB] Unifie InfluxDB source fields to schema (#3897)
- [Connector-V2] [IoTDB] Unifie IoTDB source fields to schema (#3896)
- [Transform-V2] Add transform factory test (#3887)
- [Connector-V2] [SQLServer-CDC] Add sqlserver cdc optionRule (#4019)
- [Connector-V2] [Elasticsearch] Support https protocol (#3997)
- [Connector-V2] [Elasticsearch] Add ElasticSearch catalog (#4108)
- [Connector-V2] [Elasticsearch] Support dsl filter (#4130)
- [Connector-V2] [S3] Add S3Catalog (#4121)
- [Connector-V2] [Doris] Refactor some Doris Sink code as well as support 2pc and cdc (#4235)
- [Connector-V2] [CDC] Optimize options & add docs for compatible_debezium_json (#4351)

### CI

- [CI] Imprve CI/CD process, split all connector it cases to 4 (#3832)
- [CI] Imprve CI/CD process, split all connector it cases to 5 (#4065)
- [E2E] Improve CI stability (#4068)
- [CI] Improve ci steps (#4314) (#4342)

### Zeta(ST-Engine)

- [Zeta] Remove unnecessary dependencies in pom (#3795)
- [Zeta] Add the restart job logic when all nodes down (#3784)
- [Zeta] Add the logic that cancel CheckpointCoordinator first before cancel task (#3838)
- [Zeta] Remove `seatunnel-api` from engine storage (#3834)
- [Zeta] Suppress delete data log level (#4001)
- [Zeta] Set the write data timeout to be configurable (#4059)
- [Zeta] Job clean before JobMaster future complete (#4087)
- [Zeta] Add Slot Sequence To Avoid Active Check Error (#4097)
- [Zeta] Improve Client Job Info Message
- [Zeta] Client Job Info Message Add Order By Submit Time
- [Zeta] JVM parameters distinguish between client and server (#4297)

### E2E

- [Connector-V2] [Iceberg] Refactor iceberg connector e2e test cases (#3820)
- [Connector-V2] [Datahub] Refactor DataHub connector e2e test cases (#3866)
- [Connector-V2] [MongoDB] Refactor MongoDB connector e2e test (#3819)
- [Connector-V2] [ES & InfluxDB & Redis] Add a 'nonNull' check to avoid npe when executing 'tearDown'. (#3967)
- [Connector-V2] [Http] Refactor the e2e test of http (#3969)
- [Connector-V2] Remove the use of scala.Tuple in the e2e module (#3974)
- [Zeta] Change E2E To support ClusterFaultToleranceIT (#3976)
- [Zeta] Statistics server job and system resource usage (#3982)
- [Transform-v2] Merge e2e tests and config files of SQL transform plugin (#4278)
- [Connector-V2] [Jdbc] Reactor jdbc e2e with new api, then remove the useless e2e case. For better performance, we split the jdbc e2e module into three modules. (#4165)

## Feature

### Core

- [Core] [Transform-V2] Support transform-v2 for flink engine  (#3396)
- [Core] [Transform-V2] Support transform-v2 for spark (#3409)
- [Core] [Starter] Refactor starter module (#3798)
- [Core] [Transform-V1] Remove old version transform module (#3911)
- [Core] [API] Remove old version apis (#3920)
- [Core] [API] Add savemode feature API (#3885)
- [Core] [Shade] Add seatunnel-jackson module (#3947)
- [Core] Upgrade flink version to 1.14.6 (#3963)
- [Core] Support flink 1.16 (#3979)
- [Core] [Spark] Support spark 3.3 (#3984)
- [Core] [API] Support Other Config Type Adapter Extends SPI (#4016)
- [Core] [Catalog] Support create/drop table, create/drop database in catalog (#4075)
- [Core] Add the spotless plugin for unified code style (#3939) (#4101)
- [Core] [API] Add Metrics for Connector-V2 (#4017)
- [Core] Support config encryption (#4134)

### Connector-V2

- [Connector-V2] [JDBC] Add SAP HANA connector (#3017)
- [Connector-V2] [Persistiq] Add Persistiq source connector (#3460)
- [Connector-V2] [TDEngine] Add tdengine source (#2832)
- [Connector-V2] [SelectDB Cloud] Support SelectDB Cloud Sink Connector (#3958)
- [Transform-V2] Add FieldMapper transform (#3781)
- [Transform-V2] Add SimpleSQL transform plugin (#4148)
- [Json-format] [canal] Support read canal format message (#3950)
- [Connector-V2] [CDC] Support export debezium-json format to kafka (#4339)
- [Connector-V2] [CDC] MySQL CDC supports deserialization of multi-tables (#4067)
- [Connector-V2] [Hbase] Introduce hbase sink connector (#4049)
- [Connector-V2] [MySQL-CDC] Support all database sync to StarRocks 
- [Connector-V2] [StarRocks] Support auto create table when sink to it
- [Connector-V2] [MySQL-CDC] Support read database list (#4255)
- [Connector-V2] [CDC] Support add & dorp tables when restore cdc jobs (#4254)

### Zeta(ST-Engine)

- [Zeta] Add close engine instance shell script (#3776)
- [Zeta] Zeta support print metrics information (#3913)
- [Zeta] Add savepoint and restore with savepoint (#3930)
- [Zeta] Support Get Error Message From Client When Job Failed (#3928)
- [Zeta] Add oss support for checkpoint storing (#3732)
- [Zeta] Add Zeta Client ShutdownHook To Cancel Job (#3946)
- [Zeta] Add a jvm.properties file to define the SeaTunnel Zeta JVM Options (#3771)
- [Zeta] Add rest api for runningjob detail (#4072)
- [Zeta] Support get cluster metrics from seatunnel zeta client (#4139)

## Docs

- [Docs] Add transform v2 doc & remove transform v1 doc (#3786)
- [Docs] Add some tips in jdbc sink doc (#3916)
- [Docs] Add imap store doc (#4008)
- [Docs] SeaTunnel run with flink operator (#4024)
- [Docs] Add SQLServer cdc connector doc (#3993)
- [Docs] Update usage docs (#4030)
- [Docs] Modify the error of some documents (#4037)
- [Docs] Improve kafka sink docs (#4243)
- [Docs] Add the lost error codes in docs (#4275)
- [Docs] Improve StarRocks Sink Doc With Template Feature. (#4290)
- [Docs] Improve MySQL-CDC Doc For connect.timeout.ms field (#4292)

In this version, we have fixed numerous bugs in the Zeta engine, improving its stability and reliability. We have also improved the stability of the CI/CD process in the engineering aspect, optimizing the contributor's experience. In terms of connectors, we have added several new connectors, fixed hidden bugs in commonly used connectors, and refactored some of them to improve the stability of data transmission and enhance the user experience. In this release, we have also added support for MySQL CDC full-table synchronization to StarRocks, and automatic table creation is now possible on the StarRocks end.