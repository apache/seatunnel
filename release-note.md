# next-release

## Bug fix

### Core

- [Core] [API] Fixed generic class loss for lists (#4421)
- [Core] [API] Fix parse nested row data type key changed upper (#4459)
 
### Connector-V2

- [Json-format] [Canal-Json] Fix json deserialize NPE (#4195)
- [Connector-V2] [Jdbc] Field aliases are not supported in the query of jdbc source. (#4210)
- [Connector-V2] [Jdbc] Fix connection failure caused by connection timeout. (#4322)
- [Connector-V2] [Jdbc] Set default value to false of JdbcOption: generate_sink_sql (#4471)
- [Connector-V2] [Pulsar] Fix the bug that can't consume messages all the time. (#4125)
- [Connector-V2] [Eleasticsearch] Document description error (#4390)
- [Connector-V2] [Eleasticsearch] Source deserializer error and inappropriate (#4233)
- [Connector-V2] [Kafka] Fix KafkaProducer resources have never been released. (#4302)
- [Connector-V2] [Kafka] Fix the permission problem caused by client.id. (#4246)
- [Connector-V2] [Kafka] Fix KafkaConsumerThread exit caused by commit offset error. (#4379)

### Zeta(ST-Engine)

- [Zeta] Fix LogicalDagGeneratorTest testcase (#4401)
- [Zeta] Fix MultipleTableJobConfigParser parse only one transform (#4412)
- [Zeta] Fix missing common plugin jars (#4448)
- [Zeta] Fix handleCheckpointError be called while checkpoint already complete (#4442)
- [Zeta] Fix job error message is not right bug (#4463)
- [Zeta] Fix finding TaskGroup deployment node bug (#4449)
- [Zeta] Fix the bug of conf (#4488)
- [Zeta] Fix Connector load logic from zeta (#4510)
- [Zeta] Fix conflict dependency of hadoop-hdfs (#4509)

### E2E

- [E2E] [Kafka] Fix kafka e2e testcase (#4520)
- [Container Version] Fix risk of unreproducible test cases #4591

## Improve

### Core

- [Core] [Spark] Push transform operation from Spark Driver to Executors (#4503)
- [Core] [Starter] Optimize code structure & remove redundant code (#4525)
- [Core] [Translation] [Flink] Optimize code structure & remove redundant code (#4527)

### Connector-V2

- [Connector-V2] [CDC] Improve startup.mode/stop.mode options (#4360)
- [Connector-V2] [CDC] Optimize jdbc fetch-size options (#4352)
- [Connector-V2] [SQLServer] Fix sqlserver catalog (#4441)
- [Connector-V2] [StarRocks] Improve StarRocks Serialize Error Message (#4458)
- [Connector-V2] [Jdbc] add the log for sql and update some style (#4475)
- [Connector-V2] [Jdbc] Fix the table name is not automatically obtained when multiple tables (#4514)
- [Connector-V2] [S3 & Kafka] Delete unavailable S3 & Kafka Catalogs (#4477)
- [Connector-V2] [Pulsar] Support Canal Format

### CI

### Zeta(ST-Engine)

- [Zeta] Support run the server through daemon mode (#4161)
- [Zeta] Change ClassLoader To Improve the SDK compatibility of the client (#4447)
- [Zeta] Client Support Async Submit Job (#4456)
- [Zeta] Add more detailed log output. (#4446)
- [Zeta] Improve seatunnel-cluster.sh (#4435)
- [Zeta] Reduce CPU Cost When Task Not Ready (#4479)
- [Zeta] Add parser log (#4485)
- [Zeta] Remove redundant code (#4489)
- [Zeta] Remove redundancy code in validateSQL (#4506)
- [Zeta] Improve JobMetrics fetch performance (#4467)

## Feature

### Core

- [Core] [API] Support convert strings as List<T> option (#4362)
- [Core] [API] Add copy method to Catalog codes (#4414)
- [Core] [API] Add options check before create source and sink and transform in FactoryUtil (#4424)
- [Core] [Shade] Add guava shade module (#4358)

### Connector-V2

- [Connector-V2] [CDC] [SQLServer] Support multi-table read (#4377)
- [Connector-V2] [Kafka] Kafka source supports data deserialization failure skipping (#4364)
- [Connector-V2] [Jdbc] [TiDB] Add TiDB catalog (#4438)
- [Connector-V2] [File] Add file excel sink and source (#4164)
- [Transform-V2] Add UDF SPI and an example implement for SQL Transform plugin (#4392)
- [Transform-V2] Support copy field list (#4404)
- [Transform-V2] Add support CatalogTable for FieldMapperTransform (#4423)
- [Transform-V2] Add CatalogTable support for ReplaceTransform (#4411)
- [Transform-V2] Add Catalog support for FilterRowKindTransform (#4420)
- [Transform-V2] Add support CatalogTable for FilterFieldTransform (#4422)

### Zeta(ST-Engine)

- [Zeta] Support for mixing Factory and Plugin SPI (#4359)
- [Zeta] Add get running job info by jobId rest api (#4140)
- [Zeta] Add REST API To Get System Monitoring Information (#4315)
- [Transform V2 & Zeta] Make SplitTransform Support CatalogTable And CatalogTable Evolution (#4396)

## Docs 

- [Docs] Optimizes part of the Doris and SelectDB connector documentation (#4365)
- [Docs] Fix docs code style (#4368)
- [Docs] Update jdbc doc and kafka doc (#4380)
- [Docs] Fix max_retries default value is 0. (#4383)
- [Docs] Fix markdown syntax (#4426)
- [Docs] Fix Kafka Doc Error Config Key "kafka." (#4427)
- [Docs] Add Transform to Quick Start v2 (#4436)