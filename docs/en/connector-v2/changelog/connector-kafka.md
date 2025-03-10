<details><summary>dev</summary>

| Change | Commit |
| --- | --- |
|[Feature][Kafka] Support native format read/write kafka record (#8724)|https://github.com/apache/seatunnel/commit/86e2d6fcfa|
|[improve] update kafka source default schema from content<ROW<content STRING>> to content<STRING> (#8642)|https://github.com/apache/seatunnel/commit/db6e2994d4|
|[Improve] restruct connector common options (#8634)|https://github.com/apache/seatunnel/commit/f3499a6eeb|
|[improve] kafka connector options (#8616)|https://github.com/apache/seatunnel/commit/aadfe99f88|
|[Fix] [Kafka Source] kafka source use topic as table name instead of fullName (#8401)|https://github.com/apache/seatunnel/commit/3d4f4bb33a|

</details>
<details><summary>2.3.9</summary>

| Change | Commit |
| --- | --- |
|[Feature][Kafka] Add `debezium_record_table_filter` and fix error (#8391)|https://github.com/apache/seatunnel/commit/b27a30a5aa|
|[Bug][Kafka] kafka reads repeatedly (#8465)|https://github.com/apache/seatunnel/commit/f67f27279a|
|[Hotfix][Connector-V2][kafka] fix kafka sink config exactly-once  exception (#7857)|https://github.com/apache/seatunnel/commit/92b3253a5b|
|[Improve][dist]add shade check rule (#8136)|https://github.com/apache/seatunnel/commit/51ef800016|
|[Improve][Kafka] Support custom topic for debezium compatible format (#8145)|https://github.com/apache/seatunnel/commit/deefe8762a|
|[Improve][API] Unified tables_configs and table_list (#8100)|https://github.com/apache/seatunnel/commit/84c0b8d660|
|[Fix][Kafka] Fix in kafka streaming mode can not read incremental data (#7871)|https://github.com/apache/seatunnel/commit/a0eeeb9b62|
|[Feature][Core] Support cdc task ddl restore for zeta (#7463)|https://github.com/apache/seatunnel/commit/8e322281ed|
|[Fix][Connector-V2] Fix kafka `format_error_handle_way` not work (#7838)|https://github.com/apache/seatunnel/commit/63c7b4e9cc|
|[Feature][Restapi] Allow metrics information to be associated to logical plan nodes (#7786)|https://github.com/apache/seatunnel/commit/6b7c53d03c|

</details>
<details><summary>2.3.8</summary>

| Change | Commit |
| --- | --- |
|[Feature][kafka] Add arg  poll.timeout  for interval poll messages (#7606)|https://github.com/apache/seatunnel/commit/09d12fc40e|
|[Improve][Kafka] kafka source refactored some reader read logic (#6408)|https://github.com/apache/seatunnel/commit/10598b6aec|
|[Feature][connector-v2]Add Kafka Protobuf Data Parsing Support (#7361)|https://github.com/apache/seatunnel/commit/51c8e1a834|

</details>
<details><summary>2.3.7</summary>

| Change | Commit |
| --- | --- |
|[Hotfix][Connector] Fix kafka consumer log next startup offset (#7312)|https://github.com/apache/seatunnel/commit/891652399e|

</details>
<details><summary>2.3.6</summary>

| Change | Commit |
| --- | --- |
|[Fix][Connector kafka]Fix Kafka consumer stop fetching after TM node restarted (#7233)|https://github.com/apache/seatunnel/commit/7dc3fa8a13|
|[Fix][Connector-V2] Fix kafka batch mode can not read all message (#7135)|https://github.com/apache/seatunnel/commit/1784c01a35|
|[Feature][connector][kafka] Support read Maxwell format message from kafka #4415 (#4428)|https://github.com/apache/seatunnel/commit/4281b867ac|
|[Hotfix][Connector-V2][kafka]Kafka consumer group automatically commits offset logic error fix (#6961)|https://github.com/apache/seatunnel/commit/181f01ee52|
|[Improve][CDC] Bump the version of debezium to 1.9.8.Final (#6740)|https://github.com/apache/seatunnel/commit/c3ac953524|
|[Feature][Kafka] Support multi-table source read  (#5992)|https://github.com/apache/seatunnel/commit/60104602d1|

</details>
<details><summary>2.3.5</summary>

| Change | Commit |
| --- | --- |
|[Fix][Kafka-Sink] fix kafka sink factory option rule (#6657)|https://github.com/apache/seatunnel/commit/37578e103f|

</details>
<details><summary>2.3.4</summary>

| Change | Commit |
| --- | --- |
|[Feature][Connector-V2] Remove useless code for kafka connector (#6157)|https://github.com/apache/seatunnel/commit/0f286d1627|
|[Feature] support avro format (#5084)|https://github.com/apache/seatunnel/commit/93a006156d|
|[Improve][Common] Introduce new error define rule (#5793)|https://github.com/apache/seatunnel/commit/9d1b2582b2|
|[Improve] Remove use `SeaTunnelSink::getConsumedType` method and mark it as deprecated (#5755)|https://github.com/apache/seatunnel/commit/8de7408100|
|[Feature][formats][ogg] Support read ogg format message #4201 (#4225)|https://github.com/apache/seatunnel/commit/7728e241e8|
|[Improve] Remove all useless `prepare`, `getProducedType` method (#5741)|https://github.com/apache/seatunnel/commit/ed94fffbb9|
|[Improve] Add default implement for `SeaTunnelSink::setTypeInfo` (#5682)|https://github.com/apache/seatunnel/commit/86cba87450|
|KafkaSource use Factory to create source (#5635)|https://github.com/apache/seatunnel/commit/1c6176e518|
|[Improve] Refactor CatalogTable and add `SeaTunnelSource::getProducedCatalogTables` (#5562)|https://github.com/apache/seatunnel/commit/41173357f8|
|[Improve][CheckStyle] Remove useless 'SuppressWarnings' annotation of checkstyle. (#5260)|https://github.com/apache/seatunnel/commit/51c0d709ba|

</details>
<details><summary>2.3.3</summary>

| Change | Commit |
| --- | --- |
|[Feature][Connector-V2] connector-kafka source support data conversion extracted by kafka connect source (#4516)|https://github.com/apache/seatunnel/commit/bd74989099|
|[Feature][connector][kafka] Support read debezium format message from kafka (#5066)|https://github.com/apache/seatunnel/commit/53a1f0c6c1|

</details>
<details><summary>2.3.2</summary>

| Change | Commit |
| --- | --- |
|[hotfix][kafka] Fix the problem that the partition information cannot be obtained when kafka is restored (#4764)|https://github.com/apache/seatunnel/commit/c203ef5f8d|
|Fix the processing bug of abnormal parsing method of kafkaSource format. (#4687)|https://github.com/apache/seatunnel/commit/228257b2e2|
|[hotfix][e2e][kafka] Fix the job not stopping (#4600)|https://github.com/apache/seatunnel/commit/93471c9ade|
|[Improve][connector][kafka] Set default value for partition option (#4524)|https://github.com/apache/seatunnel/commit/884f733c3d|
|[chore] delete unavailable S3 & Kafka Catalogs (#4477)|https://github.com/apache/seatunnel/commit/e0aec5ecec|
|[Feature][API] Add options check before create source and sink and transform in FactoryUtil (#4424)|https://github.com/apache/seatunnel/commit/38f1903be2|
|[Feature][Connector-V2][Kafka] Kafka source supports data deserialization failure skipping (#4364)|https://github.com/apache/seatunnel/commit/e1ed22b153|
|[Bug][Connector-v2][KafkaSource]Fix KafkaConsumerThread exit caused by commit offset error. (#4379)|https://github.com/apache/seatunnel/commit/71f4d0c784|
|[Bug][Connector-v2][KafkaSink]Fix the permission problem caused by client.id. (#4246)|https://github.com/apache/seatunnel/commit/3cdb7cfa4d|
|Fix KafkaProducer resources have never been released. (#4302)|https://github.com/apache/seatunnel/commit/f99f02caa2|

</details>
<details><summary>2.3.1</summary>

| Change | Commit |
| --- | --- |
|[Improve][CDC] Optimize options & add docs for compatible_debezium_json (#4351)|https://github.com/apache/seatunnel/commit/336f590498|
|[Hotfix][Zeta] Fix TaskExecutionService Deploy Failed The Job Can't Stop (#4265)|https://github.com/apache/seatunnel/commit/cf55b070bb|
|[Feature][CDC] Support export debezium-json format to kafka (#4339)|https://github.com/apache/seatunnel/commit/5817ec07bf|
|[Improve]]Connector-V2\[Kafka] Set kafka consumer default group (#4271)|https://github.com/apache/seatunnel/commit/82c784a3ef|
|[chore] Fix the words of `canal` & `kafka` (#4261)|https://github.com/apache/seatunnel/commit/077a8d27a7|
|Merge branch 'dev' into merge/cdc|https://github.com/apache/seatunnel/commit/4324ee1912|
|[Improve][Project] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/423b583038|
|[Improve] [Connector-V2] [StarRocks] Starrocks Support Auto Create Table (#4177)|https://github.com/apache/seatunnel/commit/7e0008e6fb|
|[improve][api] Refactoring schema parse (#4157)|https://github.com/apache/seatunnel/commit/b2f573a13e|
|[Imprve][Connector-V2][Hive] Support read text table & Column projection (#4105)|https://github.com/apache/seatunnel/commit/717620f542|
|[Improve][build] Give the maven module a human readable name (#4114)|https://github.com/apache/seatunnel/commit/d7cd601051|
|Add convertor factory (#4119)|https://github.com/apache/seatunnel/commit/cbdea45d95|
|Add ElasticSearch catalog (#4108)|https://github.com/apache/seatunnel/commit/9ee4d8394c|
|Add Kafka catalog (#4106)|https://github.com/apache/seatunnel/commit/34f1f21e48|
|[Improve][Project] Code format with spotless plugin. (#4101)|https://github.com/apache/seatunnel/commit/a2ab166561|
| [Feature][Json-format][canal] Support read canal format message (#3950)|https://github.com/apache/seatunnel/commit/b80be72c85|
|[Improve][Connector-V2][Kafka] Support extract topic from SeaTunnelRow field (#3742)|https://github.com/apache/seatunnel/commit/8aff807305|
|[Feature][shade][Jackson] Add seatunnel-jackson module (#3947)|https://github.com/apache/seatunnel/commit/5d8862ec9c|
|[Hotfix][Connector-V2][Kafka] Fix the bug that kafka consumer is not close. (#3836)|https://github.com/apache/seatunnel/commit/3447266427|
|fix commit kafka offset bug. (#3933)|https://github.com/apache/seatunnel/commit/e60ad938be|
|[Feature][Connector] add get source method to all source connector (#3846)|https://github.com/apache/seatunnel/commit/417178fb84|
|[Improve] [Connector-V2] Change Connector Custom Config Prefix To Map (#3719)|https://github.com/apache/seatunnel/commit/ef1b8b1bb5|
|[Feature][API & Connector & Doc] add parallelism and column projection interface (#3829)|https://github.com/apache/seatunnel/commit/b9164b8ba1|
|[Bug][KafkaSource]Fix the default value of commit_on_checkpoint. (#3831)|https://github.com/apache/seatunnel/commit/df969849f6|
|[Bug][KafkaSource]Failed to parse offset format (#3810)|https://github.com/apache/seatunnel/commit/8e1196accf|
|[Improve] [Connector-V2] Kafka client user configured clientid is preferred (#3783)|https://github.com/apache/seatunnel/commit/aacf0abc04|

</details>
<details><summary>2.3.0</summary>

| Change | Commit |
| --- | --- |
|[Improve] [Connector-V2] Fix Kafka sink can't run EXACTLY_ONCE semantics (#3724)|https://github.com/apache/seatunnel/commit/5e3f196e29|
|[Improve] [Connector-V2] fix kafka admin client can't get property config (#3721)|https://github.com/apache/seatunnel/commit/74c3351700|
|[Improve][Connector-V2][Kafka] Add text format for kafka sink connector (#3711)|https://github.com/apache/seatunnel/commit/74bbd76b65|
|[Hotfix][OptionRule] Fix option rule about all connectors (#3592)|https://github.com/apache/seatunnel/commit/226dc6a119|
|[Improve][Connector-V2][Kafka]Unified exception for Kafka source and sink connector (#3574)|https://github.com/apache/seatunnel/commit/3b573798db|
|options in conditional need add to required or optional options (#3501)|https://github.com/apache/seatunnel/commit/51d5bcba10|
|[Improve][Connector-V2-kafka] Support for dynamic discover topic & partition in streaming mode (#3125)|https://github.com/apache/seatunnel/commit/999cfd6069|
|[Improve][Connector-V2][Kafka] Support to specify multiple partition keys (#3230)|https://github.com/apache/seatunnel/commit/f65f44f44c|
|[Feature][Connector-V2][Kafka] Add Kafka option rules (#3388)|https://github.com/apache/seatunnel/commit/cc0cb8cdb8|
|[Improve][Connector-V2][Kafka]Improve kafka metadata code format (#3397)|https://github.com/apache/seatunnel/commit/379da3097f|
|[Improve][Connector-V2-kafka] Support setting read starting offset or time at startup config (#3157)|https://github.com/apache/seatunnel/commit/3da19d4444|

</details>
<details><summary>2.3.0-beta</summary>

| Change | Commit |
| --- | --- |
|update (#3150)|https://github.com/apache/seatunnel/commit/2b44992750|
|[Feature][connectors-v2][kafka] Kafka supports custom schema #2371 (#2783)|https://github.com/apache/seatunnel/commit/6506e306eb|
|[feature][connector][kafka] Support extract partition from SeaTunnelRow fields (#3085)|https://github.com/apache/seatunnel/commit/385e1f42c0|
|[Improve][connector][kafka] sink support custom partition (#3041)|https://github.com/apache/seatunnel/commit/ebddc18c41|
|[Improve][all] change Log to @Slf4j (#3001)|https://github.com/apache/seatunnel/commit/6016100f12|
|[Imporve][Connector-V2]Parameter verification for connector V2 kafka sink (#2866)|https://github.com/apache/seatunnel/commit/254223fdb9|

</details>
<details><summary>2.2.0-beta</summary>

| Change | Commit |
| --- | --- |
|[Connector-V2] [Kafka] Fix Kafka Streaming problem (#2759)|https://github.com/apache/seatunnel/commit/e92e7b7283|
|[Improve][Connector-V2] Fix kafka connector (#2745)|https://github.com/apache/seatunnel/commit/90ce3851db|
|[DEV][Api] Replace SeaTunnelContext with JobContext and remove singleton pattern (#2706)|https://github.com/apache/seatunnel/commit/cbf82f755c|
|[#2606]Dependency management split (#2630)|https://github.com/apache/seatunnel/commit/fc047be69b|
|StateT of SeaTunnelSource should extend `Serializable` (#2214)|https://github.com/apache/seatunnel/commit/8c426ef850|
|[api-draft][Optimize] Optimize module name (#2062)|https://github.com/apache/seatunnel/commit/f79e3112b1|

</details>
