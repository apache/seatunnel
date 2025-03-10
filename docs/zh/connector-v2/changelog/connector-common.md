<details><summary>dev</summary>

| Change | Commit |
| --- | --- |
|[Feature][Connector-V2] Add `filename_extension` parameter for read/write file (#8769)|https://github.com/apache/seatunnel/commit/78b23c0ef5|
|[Fix][Connector-v2] Add DateMilliConvertor to Convert DateMilliVector into Default Timezone (#8736)|https://github.com/apache/seatunnel/commit/7b8298a8a4|
|[Fix][Connector-V2] fix starRocks automatically creates tables with comment (#8568)|https://github.com/apache/seatunnel/commit/c4cb1fc4a3|
|[Feature][Connector-V2] Support single file mode in file sink (#8518)|https://github.com/apache/seatunnel/commit/e893deed50|
|[Fix][Connector-V2] Fixed adding table comments (#8514)|https://github.com/apache/seatunnel/commit/edca75b0d6|

</details>
<details><summary>2.3.9</summary>

| Change | Commit |
| --- | --- |
|[Feature][Core] Support read arrow data (#8137)|https://github.com/apache/seatunnel/commit/4710ea0f8d|
|[Feature][Clickhouse] Support sink savemode  (#8086)|https://github.com/apache/seatunnel/commit/e6f92fd79b|
|[Improve][dist]add shade check rule (#8136)|https://github.com/apache/seatunnel/commit/51ef800016|
|[Fix][Connector-V2] Fix AbstractSingleSplitReader lock useless when do checkpoint (#7764)|https://github.com/apache/seatunnel/commit/a941b91628|

</details>
<details><summary>2.3.6</summary>

| Change | Commit |
| --- | --- |
|[Improve][Core] Move MultiTableSink to seatunnel-api module (#7243)|https://github.com/apache/seatunnel/commit/cc5949988b|
|[Feature][Connector-V2] Support jdbc hana catalog and type convertor (#6950)|https://github.com/apache/seatunnel/commit/d663398739|
|[Improve][CDC] Close idle subtasks gorup(reader/writer) in increment phase (#6526)|https://github.com/apache/seatunnel/commit/454c339b9c|
|[Fix] Fix MultiTableWriterRunnable can not catch Throwable error (#6734)|https://github.com/apache/seatunnel/commit/d826cf9ece|
|[Fix][Connector-v2] Fix the sql statement error of create table for doris and starrocks (#6679)|https://github.com/apache/seatunnel/commit/88263cd69f|

</details>
<details><summary>2.3.5</summary>

| Change | Commit |
| --- | --- |
|[Improve][CDC] Optimize split state memory allocation in increment phase (#6554)|https://github.com/apache/seatunnel/commit/fe33422161|
|[Feature][Core] Support event listener for job (#6419)|https://github.com/apache/seatunnel/commit/831d0022eb|
|[Improve] Improve MultiTableSinkWriter prepare commit performance (#6495)|https://github.com/apache/seatunnel/commit/2086b0e8a6|
|[Improve][API] Unify type system api(data &amp; type) (#5872)|https://github.com/apache/seatunnel/commit/b38c7edcc9|

</details>
<details><summary>2.3.4</summary>

| Change | Commit |
| --- | --- |
|[Hotfix][Zeta] Fix job can not restore when last checkpoint failed (#6193)|https://github.com/apache/seatunnel/commit/59f60b9f73|
|[Improve] Extend `SupportResourceShare` to spark/flink (#5847)|https://github.com/apache/seatunnel/commit/c69da93b87|
|[Feature][Core] Upgrade flink source translation (#5100)|https://github.com/apache/seatunnel/commit/5aabb14a94|
|[Fix] Fix MultiTableSinkWriter thread index always 1 (#5832)|https://github.com/apache/seatunnel/commit/a6523ba368|
|[Improve][Connector-V2][Common] Remove assert key word. (#5915)|https://github.com/apache/seatunnel/commit/d757dcd1fc|
|[Improve] Remove use `SeaTunnelSink::getConsumedType` method and mark it as deprecated (#5755)|https://github.com/apache/seatunnel/commit/8de7408100|
|[Fix] Fix MultiTableSink restore failed when add new table (#5746)|https://github.com/apache/seatunnel/commit/21503bd771|
|[feature][connector-jdbc]Add Save Mode function and Connector-JDBC (MySQL) connector has been realized (#5663)|https://github.com/apache/seatunnel/commit/eff17ccbe5|
|[Improve] Add default implement for `SeaTunnelSink::setTypeInfo` (#5682)|https://github.com/apache/seatunnel/commit/86cba87450|
|[Fix] Fix MultiTableSink return committer but sink do not support (#5710)|https://github.com/apache/seatunnel/commit/c413040a6e|
|[Fix] Fix log error when multi-table sink close (#5683)|https://github.com/apache/seatunnel/commit/fea4b6f268|
|[Feature] Support multi-table sink (#5620)|https://github.com/apache/seatunnel/commit/81ac173189|

</details>
<details><summary>2.3.3</summary>

| Change | Commit |
| --- | --- |
|[Hotfix] Fix com.google.common.base.Preconditions to seatunnel shade one (#5284)|https://github.com/apache/seatunnel/commit/ed5eadcf73|
|[Bugfix][zeta] Fix cdc connection does not close (#4922)|https://github.com/apache/seatunnel/commit/a2d2f2dda8|

</details>
<details><summary>2.3.1</summary>

| Change | Commit |
| --- | --- |
|[improve][zeta] fix zeta bugs|https://github.com/apache/seatunnel/commit/3a82e8b39f|
|Merge branch &#x27;dev&#x27; into merge/cdc|https://github.com/apache/seatunnel/commit/4324ee1912|
|[Improve][Project] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/423b583038|
|[Improve][SeaTunnelSchema] Complete data type prompt. (#4181)|https://github.com/apache/seatunnel/commit/9e92593709|
|[improve][api] Refactoring schema parse (#4157)|https://github.com/apache/seatunnel/commit/b2f573a13e|
|[Improve][build] Give the maven module a human readable name (#4114)|https://github.com/apache/seatunnel/commit/d7cd601051|
|Add Kafka catalog (#4106)|https://github.com/apache/seatunnel/commit/34f1f21e48|
|[Improve][Project] Code format with spotless plugin. (#4101)|https://github.com/apache/seatunnel/commit/a2ab166561|
|[Hotfix][Connector-V2] Fix ConcurrentModificationException when snapshotState based on SourceReaderBase (#4011)|https://github.com/apache/seatunnel/commit/cd2bd6a408|
|[Feature][shade][Jackson] Add seatunnel-jackson module (#3947)|https://github.com/apache/seatunnel/commit/5d8862ec9c|
|[Improve][CDC][base] Guaranteed to be exactly-once in the process of switching from SnapshotTask to IncrementalTask (#3837)|https://github.com/apache/seatunnel/commit/8379aaf876|

</details>
<details><summary>2.3.0</summary>

| Change | Commit |
| --- | --- |
|[feature][cdc] Fixed error in mysql cdc under real-time job (#3666)|https://github.com/apache/seatunnel/commit/2238fda300|
|[Feature][Connector-V2][AmazonDynamoDB] Add Factory for AmazonDynamoDB (#3348)|https://github.com/apache/seatunnel/commit/a0068efdbf|
|[Feature][Connector-V2][SeaTunnelSchema] Improve code structure (#3384)|https://github.com/apache/seatunnel/commit/98b9168d5a|
|[feature][connector][common] Add  `SingleThreadMultiplexSourceReaderBase (#3335)|https://github.com/apache/seatunnel/commit/f4e33b5912|
|[Connector-V2] [ElasticSearch] Add ElasticSearch Source/Sink Factory (#3325)|https://github.com/apache/seatunnel/commit/38254e3f26|
|[Feature][Connector-V2] [Amazondynamodb Connector]add amazondynamodb source &amp; sink connnector (#3166)|https://github.com/apache/seatunnel/commit/183bac02f0|

</details>
<details><summary>2.3.0-beta</summary>

| Change | Commit |
| --- | --- |
|unify `flatten-maven-plugin` version (#3078)|https://github.com/apache/seatunnel/commit/ed743fddcc|
|Merge remote-tracking branch &#x27;upstream/dev&#x27; into st-engine|https://github.com/apache/seatunnel/commit/73a699d47b|
|[Imporve][Connector-V2] Imporve iotdb connector (#2917)|https://github.com/apache/seatunnel/commit/3da11ce19b|
|Merge remote-tracking branch &#x27;upstream/dev&#x27; into st-engine|https://github.com/apache/seatunnel/commit/ca80df779a|

</details>
<details><summary>2.2.0-beta</summary>

| Change | Commit |
| --- | --- |
|[Connector-V2] [ElasticSearch] Fix ElasticSearch Connector V2 Bug (#2817)|https://github.com/apache/seatunnel/commit/2fcbbf464a|
|[Improve][SeaTunnel-Schema] Support parse row type from config file (#2771)|https://github.com/apache/seatunnel/commit/9f59fc1874|
|[Bug][Core] Fix the bug that can not convert array and map (#2750)|https://github.com/apache/seatunnel/commit/6db4d7595d|
|[Improve][build] Improved scope of maven-shade-plugin (#2665)|https://github.com/apache/seatunnel/commit/93bc8bd116|
|[#2606]Dependency management split (#2630)|https://github.com/apache/seatunnel/commit/fc047be69b|
|[chore][connector-common] Rename SeatunnelSchema to SeaTunnelSchema (#2538)|https://github.com/apache/seatunnel/commit/7dc2a27388|

</details>
<details><summary>2.3.0-beta</summary>

| Change | Commit |
| --- | --- |
|[hotfix][engine][dag] Loss of parallelism when recreating actions. (#2519)|https://github.com/apache/seatunnel/commit/7953ac149f|

</details>
<details><summary>2.2.0-beta</summary>

| Change | Commit |
| --- | --- |
|[hotfix] fix user-defined schema for bytes type translattion (#2530)|https://github.com/apache/seatunnel/commit/0491a33edc|
|[Imporve][Fake-Connector-V2]support user-defined-schmea and random data for fake-table  (#2406)|https://github.com/apache/seatunnel/commit/a5447528c3|
|[Feature][Connector-V2] Local file json support (#2465)|https://github.com/apache/seatunnel/commit/65a92f2496|
|[Improve][Connector-V2] Http source support user-defined schema (#2439)|https://github.com/apache/seatunnel/commit/793933b6b8|

</details>
<details><summary>2.3.0-beta</summary>

| Change | Commit |
| --- | --- |
|[Engine][Task] Add task runtime logic (#2386)|https://github.com/apache/seatunnel/commit/14d3b92a54|

</details>
<details><summary>2.2.0-beta</summary>

| Change | Commit |
| --- | --- |
|[Feature][Connector-V2] Support user-defined schema for source connectors (#2392)|https://github.com/apache/seatunnel/commit/6b650bef07|

</details>
<details><summary>2.3.0-beta</summary>

| Change | Commit |
| --- | --- |
|Merge from dev to st-engine (#2243)|https://github.com/apache/seatunnel/commit/41e530afd5|

</details>
<details><summary>2.2.0-beta</summary>

| Change | Commit |
| --- | --- |
|StateT of SeaTunnelSource should extend `Serializable` (#2214)|https://github.com/apache/seatunnel/commit/8c426ef850|
|[Improvement][new api] refer to https://github.com/apache/incubator-seatunnel/issues/2127 (#2144)|https://github.com/apache/seatunnel/commit/e19660a049|
|[api-draft][Optimize] Optimize module name (#2062)|https://github.com/apache/seatunnel/commit/f79e3112b1|

</details>
