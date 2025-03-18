<details><summary> Change Log </summary>

| Change | Commit | Version |
| --- | --- | --- |
|[Feature][Connector-V2] Add `filename_extension` parameter for read/write file (#8769)|https://github.com/apache/seatunnel/commit/78b23c0ef5| dev |
|[Fix][Connector-v2] Add DateMilliConvertor to Convert DateMilliVector into Default Timezone (#8736)|https://github.com/apache/seatunnel/commit/7b8298a8a4| dev |
|[Fix][Connector-V2] fix starRocks automatically creates tables with comment (#8568)|https://github.com/apache/seatunnel/commit/c4cb1fc4a3| dev |
|[Feature][Connector-V2] Support single file mode in file sink (#8518)|https://github.com/apache/seatunnel/commit/e893deed50| dev |
|[Fix][Connector-V2] Fixed adding table comments (#8514)|https://github.com/apache/seatunnel/commit/edca75b0d6| dev |
|[Feature][Core] Support read arrow data (#8137)|https://github.com/apache/seatunnel/commit/4710ea0f8d|2.3.9|
|[Feature][Clickhouse] Support sink savemode  (#8086)|https://github.com/apache/seatunnel/commit/e6f92fd79b|2.3.9|
|[Improve][dist]add shade check rule (#8136)|https://github.com/apache/seatunnel/commit/51ef800016|2.3.9|
|[Fix][Connector-V2] Fix AbstractSingleSplitReader lock useless when do checkpoint (#7764)|https://github.com/apache/seatunnel/commit/a941b91628|2.3.9|
|[Improve][Core] Move MultiTableSink to seatunnel-api module (#7243)|https://github.com/apache/seatunnel/commit/cc5949988b|2.3.6|
|[Feature][Connector-V2] Support jdbc hana catalog and type convertor (#6950)|https://github.com/apache/seatunnel/commit/d663398739|2.3.6|
|[Improve][CDC] Close idle subtasks gorup(reader/writer) in increment phase (#6526)|https://github.com/apache/seatunnel/commit/454c339b9c|2.3.6|
|[Fix] Fix MultiTableWriterRunnable can not catch Throwable error (#6734)|https://github.com/apache/seatunnel/commit/d826cf9ece|2.3.6|
|[Fix][Connector-v2] Fix the sql statement error of create table for doris and starrocks (#6679)|https://github.com/apache/seatunnel/commit/88263cd69f|2.3.6|
|[Improve][CDC] Optimize split state memory allocation in increment phase (#6554)|https://github.com/apache/seatunnel/commit/fe33422161|2.3.5|
|[Feature][Core] Support event listener for job (#6419)|https://github.com/apache/seatunnel/commit/831d0022eb|2.3.5|
|[Improve] Improve MultiTableSinkWriter prepare commit performance (#6495)|https://github.com/apache/seatunnel/commit/2086b0e8a6|2.3.5|
|[Improve][API] Unify type system api(data &amp; type) (#5872)|https://github.com/apache/seatunnel/commit/b38c7edcc9|2.3.5|
|[Hotfix][Zeta] Fix job can not restore when last checkpoint failed (#6193)|https://github.com/apache/seatunnel/commit/59f60b9f73|2.3.4|
|[Improve] Extend `SupportResourceShare` to spark/flink (#5847)|https://github.com/apache/seatunnel/commit/c69da93b87|2.3.4|
|[Feature][Core] Upgrade flink source translation (#5100)|https://github.com/apache/seatunnel/commit/5aabb14a94|2.3.4|
|[Fix] Fix MultiTableSinkWriter thread index always 1 (#5832)|https://github.com/apache/seatunnel/commit/a6523ba368|2.3.4|
|[Improve][Connector-V2][Common] Remove assert key word. (#5915)|https://github.com/apache/seatunnel/commit/d757dcd1fc|2.3.4|
|[Improve] Remove use `SeaTunnelSink::getConsumedType` method and mark it as deprecated (#5755)|https://github.com/apache/seatunnel/commit/8de7408100|2.3.4|
|[Fix] Fix MultiTableSink restore failed when add new table (#5746)|https://github.com/apache/seatunnel/commit/21503bd771|2.3.4|
|[feature][connector-jdbc]Add Save Mode function and Connector-JDBC (MySQL) connector has been realized (#5663)|https://github.com/apache/seatunnel/commit/eff17ccbe5|2.3.4|
|[Improve] Add default implement for `SeaTunnelSink::setTypeInfo` (#5682)|https://github.com/apache/seatunnel/commit/86cba87450|2.3.4|
|[Fix] Fix MultiTableSink return committer but sink do not support (#5710)|https://github.com/apache/seatunnel/commit/c413040a6e|2.3.4|
|[Fix] Fix log error when multi-table sink close (#5683)|https://github.com/apache/seatunnel/commit/fea4b6f268|2.3.4|
|[Feature] Support multi-table sink (#5620)|https://github.com/apache/seatunnel/commit/81ac173189|2.3.4|
|[Hotfix] Fix com.google.common.base.Preconditions to seatunnel shade one (#5284)|https://github.com/apache/seatunnel/commit/ed5eadcf73|2.3.3|
|[Bugfix][zeta] Fix cdc connection does not close (#4922)|https://github.com/apache/seatunnel/commit/a2d2f2dda8|2.3.3|
|[improve][zeta] fix zeta bugs|https://github.com/apache/seatunnel/commit/3a82e8b39f|2.3.1|
|Merge branch &#x27;dev&#x27; into merge/cdc|https://github.com/apache/seatunnel/commit/4324ee1912|2.3.1|
|[Improve][Project] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/423b583038|2.3.1|
|[Improve][SeaTunnelSchema] Complete data type prompt. (#4181)|https://github.com/apache/seatunnel/commit/9e92593709|2.3.1|
|[improve][api] Refactoring schema parse (#4157)|https://github.com/apache/seatunnel/commit/b2f573a13e|2.3.1|
|[Improve][build] Give the maven module a human readable name (#4114)|https://github.com/apache/seatunnel/commit/d7cd601051|2.3.1|
|Add Kafka catalog (#4106)|https://github.com/apache/seatunnel/commit/34f1f21e48|2.3.1|
|[Improve][Project] Code format with spotless plugin. (#4101)|https://github.com/apache/seatunnel/commit/a2ab166561|2.3.1|
|[Hotfix][Connector-V2] Fix ConcurrentModificationException when snapshotState based on SourceReaderBase (#4011)|https://github.com/apache/seatunnel/commit/cd2bd6a408|2.3.1|
|[Feature][shade][Jackson] Add seatunnel-jackson module (#3947)|https://github.com/apache/seatunnel/commit/5d8862ec9c|2.3.1|
|[Improve][CDC][base] Guaranteed to be exactly-once in the process of switching from SnapshotTask to IncrementalTask (#3837)|https://github.com/apache/seatunnel/commit/8379aaf876|2.3.1|
|[feature][cdc] Fixed error in mysql cdc under real-time job (#3666)|https://github.com/apache/seatunnel/commit/2238fda300|2.3.0|
|[Feature][Connector-V2][AmazonDynamoDB] Add Factory for AmazonDynamoDB (#3348)|https://github.com/apache/seatunnel/commit/a0068efdbf|2.3.0|
|[Feature][Connector-V2][SeaTunnelSchema] Improve code structure (#3384)|https://github.com/apache/seatunnel/commit/98b9168d5a|2.3.0|
|[feature][connector][common] Add  `SingleThreadMultiplexSourceReaderBase (#3335)|https://github.com/apache/seatunnel/commit/f4e33b5912|2.3.0|
|[Connector-V2] [ElasticSearch] Add ElasticSearch Source/Sink Factory (#3325)|https://github.com/apache/seatunnel/commit/38254e3f26|2.3.0|
|[Feature][Connector-V2] [Amazondynamodb Connector]add amazondynamodb source &amp; sink connnector (#3166)|https://github.com/apache/seatunnel/commit/183bac02f0|2.3.0|
|unify `flatten-maven-plugin` version (#3078)|https://github.com/apache/seatunnel/commit/ed743fddcc|2.3.0-beta|
|Merge remote-tracking branch &#x27;upstream/dev&#x27; into st-engine|https://github.com/apache/seatunnel/commit/73a699d47b|2.3.0-beta|
|[Imporve][Connector-V2] Imporve iotdb connector (#2917)|https://github.com/apache/seatunnel/commit/3da11ce19b|2.3.0-beta|
|Merge remote-tracking branch &#x27;upstream/dev&#x27; into st-engine|https://github.com/apache/seatunnel/commit/ca80df779a|2.3.0-beta|
|[Connector-V2] [ElasticSearch] Fix ElasticSearch Connector V2 Bug (#2817)|https://github.com/apache/seatunnel/commit/2fcbbf464a|2.2.0-beta|
|[Improve][SeaTunnel-Schema] Support parse row type from config file (#2771)|https://github.com/apache/seatunnel/commit/9f59fc1874|2.2.0-beta|
|[Bug][Core] Fix the bug that can not convert array and map (#2750)|https://github.com/apache/seatunnel/commit/6db4d7595d|2.2.0-beta|
|[Improve][build] Improved scope of maven-shade-plugin (#2665)|https://github.com/apache/seatunnel/commit/93bc8bd116|2.2.0-beta|
|[#2606]Dependency management split (#2630)|https://github.com/apache/seatunnel/commit/fc047be69b|2.2.0-beta|
|[chore][connector-common] Rename SeatunnelSchema to SeaTunnelSchema (#2538)|https://github.com/apache/seatunnel/commit/7dc2a27388|2.2.0-beta|
|[hotfix][engine][dag] Loss of parallelism when recreating actions. (#2519)|https://github.com/apache/seatunnel/commit/7953ac149f|2.3.0-beta|
|[hotfix] fix user-defined schema for bytes type translattion (#2530)|https://github.com/apache/seatunnel/commit/0491a33edc|2.2.0-beta|
|[Imporve][Fake-Connector-V2]support user-defined-schmea and random data for fake-table  (#2406)|https://github.com/apache/seatunnel/commit/a5447528c3|2.2.0-beta|
|[Feature][Connector-V2] Local file json support (#2465)|https://github.com/apache/seatunnel/commit/65a92f2496|2.2.0-beta|
|[Improve][Connector-V2] Http source support user-defined schema (#2439)|https://github.com/apache/seatunnel/commit/793933b6b8|2.2.0-beta|
|[Engine][Task] Add task runtime logic (#2386)|https://github.com/apache/seatunnel/commit/14d3b92a54|2.3.0-beta|
|[Feature][Connector-V2] Support user-defined schema for source connectors (#2392)|https://github.com/apache/seatunnel/commit/6b650bef07|2.2.0-beta|
|Merge from dev to st-engine (#2243)|https://github.com/apache/seatunnel/commit/41e530afd5|2.3.0-beta|
|StateT of SeaTunnelSource should extend `Serializable` (#2214)|https://github.com/apache/seatunnel/commit/8c426ef850|2.2.0-beta|
|[Improvement][new api] refer to https://github.com/apache/incubator-seatunnel/issues/2127 (#2144)|https://github.com/apache/seatunnel/commit/e19660a049|2.2.0-beta|
|[api-draft][Optimize] Optimize module name (#2062)|https://github.com/apache/seatunnel/commit/f79e3112b1|2.2.0-beta|

</details>
