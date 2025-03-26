<details><summary> Change Log </summary>

| Change | Commit | Version |
| --- | --- | --- |
|[improve] fake source options (#8950)|https://github.com/apache/seatunnel/commit/f8c47fb5f|2.3.10|
|[Improve] restruct connector common options (#8634)|https://github.com/apache/seatunnel/commit/f3499a6ee|2.3.10|
|[Feature][API] Support timestamp with timezone offset (#8367)|https://github.com/apache/seatunnel/commit/e18bfeabd|2.3.9|
|[Improve][dist]add shade check rule (#8136)|https://github.com/apache/seatunnel/commit/51ef80001|2.3.9|
|[Improve][API] Unified tables_configs and table_list (#8100)|https://github.com/apache/seatunnel/commit/84c0b8d66|2.3.9|
|[Feature][Core] Rename `result_table_name`/`source_table_name` to `plugin_input/plugin_output` (#8072)|https://github.com/apache/seatunnel/commit/c7bbd322d|2.3.9|
|[Improve][Fake] Improve memory usage when split size is large (#7821)|https://github.com/apache/seatunnel/commit/2d41b024c|2.3.9|
|[Improve][Connector-V2] Time supports default value (#7639)|https://github.com/apache/seatunnel/commit/33978689f|2.3.8|
|[Improve][Connector-V2] Fake supports column configuration (#7503)|https://github.com/apache/seatunnel/commit/39162a4e0|2.3.8|
|[Feature][Core] Add event notify for all connector (#7501)|https://github.com/apache/seatunnel/commit/d71337b0e|2.3.8|
|[Improve][Connector-V2] update vectorType (#7446)|https://github.com/apache/seatunnel/commit/1bba72385|2.3.8|
|[Feature][Connector-V2] Fake Source support produce vector data (#7401)|https://github.com/apache/seatunnel/commit/6937d10ac|2.3.8|
|[Feature][Kafka] Support multi-table source read  (#5992)|https://github.com/apache/seatunnel/commit/60104602d|2.3.6|
|[Feature][Doris] Add Doris type converter (#6354)|https://github.com/apache/seatunnel/commit/518999184|2.3.6|
|[Feature][Core] Support event listener for job (#6419)|https://github.com/apache/seatunnel/commit/831d0022e|2.3.5|
|[Fix][FakeSource] fix random from template not include the latest value issue (#6438)|https://github.com/apache/seatunnel/commit/6ec16ac46|2.3.5|
|[Improve][Catalog] Use default tablepath when can not get the tablepath from source config (#6276)|https://github.com/apache/seatunnel/commit/f8158bb80|2.3.4|
|[Improve][Connector-V2] Replace CommonErrorCodeDeprecated.JSON_OPERATION_FAILED (#5978)|https://github.com/apache/seatunnel/commit/456cd1771|2.3.4|
|FakeSource support generate different CatalogTable for MultipleTable (#5766)|https://github.com/apache/seatunnel/commit/a8b93805e|2.3.4|
|[Improve][Common] Introduce new error define rule (#5793)|https://github.com/apache/seatunnel/commit/9d1b2582b|2.3.4|
|[Improve] Add default implement for `SeaTunnelSource::getProducedType` (#5670)|https://github.com/apache/seatunnel/commit/a04add699|2.3.4|
|Support config tableIdentifier for schema (#5628)|https://github.com/apache/seatunnel/commit/652921fb7|2.3.4|
|[Feature] Add `table-names` from FakeSource/Assert to produce/assert multi-table (#5604)|https://github.com/apache/seatunnel/commit/2c67cd8f3|2.3.4|
|Support config column/primaryKey/constraintKey in schema (#5564)|https://github.com/apache/seatunnel/commit/eac76b4e5|2.3.4|
|[Improve][CheckStyle] Remove useless &#x27;SuppressWarnings&#x27; annotation of checkstyle. (#5260)|https://github.com/apache/seatunnel/commit/51c0d709b|2.3.4|
|[improve][zeta] fix zeta bugs|https://github.com/apache/seatunnel/commit/3a82e8b39|2.3.1|
|[chore] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/291214ad6|2.3.1|
|Merge branch &#x27;dev&#x27; into merge/cdc|https://github.com/apache/seatunnel/commit/4324ee191|2.3.1|
|[Improve][Project] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/423b58303|2.3.1|
|[improve][api] Refactoring schema parse (#4157)|https://github.com/apache/seatunnel/commit/b2f573a13|2.3.1|
|[Improve][build] Give the maven module a human readable name (#4114)|https://github.com/apache/seatunnel/commit/d7cd60105|2.3.1|
|[Improve][Project] Code format with spotless plugin. (#4101)|https://github.com/apache/seatunnel/commit/a2ab16656|2.3.1|
|[Improve][Connector-fake] Optimizing Data Generation Strategies refer to #4004 (#4061)|https://github.com/apache/seatunnel/commit/c7c596a6d|2.3.1|
|[Improve][Connector-V2][Fake] Improve fake connector (#3932)|https://github.com/apache/seatunnel/commit/31f12431d|2.3.1|
|[Feature][Connector-v2][StarRocks] Support write cdc changelog event(INSERT/UPDATE/DELETE) (#3865)|https://github.com/apache/seatunnel/commit/8e3d158c0|2.3.1|
|[Feature][Connector] add get source method to all source connector (#3846)|https://github.com/apache/seatunnel/commit/417178fb8|2.3.1|
|[Feature][API &amp; Connector &amp; Doc] add parallelism and column projection interface (#3829)|https://github.com/apache/seatunnel/commit/b9164b8ba|2.3.1|
|[Hotfix][OptionRule] Fix option rule about all connectors (#3592)|https://github.com/apache/seatunnel/commit/226dc6a11|2.3.0|
|[Improve][Connector-V2][Fake] Unified exception for fake source connector (#3520)|https://github.com/apache/seatunnel/commit/f371ad582|2.3.0|
|[Connector-V2] [Fake] Add Fake TableSourceFactory (#3345)|https://github.com/apache/seatunnel/commit/74b61c33a|2.3.0|
|[Connector-V2] [ElasticSearch] Add ElasticSearch Source/Sink Factory (#3325)|https://github.com/apache/seatunnel/commit/38254e3f2|2.3.0|
|[Improve] [Engine] Improve Engine performance. (#3216)|https://github.com/apache/seatunnel/commit/7393c4732|2.3.0|
|[hotfix][connector][fake] fix FakeSourceSplitEnumerator assigning duplicate splits when restoring (#3112)|https://github.com/apache/seatunnel/commit/98b1feda8|2.3.0-beta|
|[improve][connector][fake] supports setting the number of split rows and reading interval (#3098)|https://github.com/apache/seatunnel/commit/efabe6af7|2.3.0-beta|
|[feature][connector][fake] Support mutil splits for fake source connector (#2974)|https://github.com/apache/seatunnel/commit/c28c44b7c|2.3.0-beta|
|[E2E][ST-Engine] Add test data consistency in 3 node cluster and fix bug (#3038)|https://github.com/apache/seatunnel/commit/97400a6f1|2.3.0-beta|
|[Improve][all] change Log to @Slf4j (#3001)|https://github.com/apache/seatunnel/commit/6016100f1|2.3.0-beta|
|[Improve][Connector-V2] Improve fake source connector (#2944)|https://github.com/apache/seatunnel/commit/044f62ef3|2.3.0-beta|
|[Improve][Connector-v2-Fake]Supports direct definition of data values(row) (#2839)|https://github.com/apache/seatunnel/commit/b7d9dde6c|2.3.0-beta|
|[Connector-V2] [ElasticSearch] Fix ElasticSearch Connector V2 Bug (#2817)|https://github.com/apache/seatunnel/commit/2fcbbf464|2.2.0-beta|
|[DEV][Api] Replace SeaTunnelContext with JobContext and remove singleton pattern (#2706)|https://github.com/apache/seatunnel/commit/cbf82f755|2.2.0-beta|
|[Bug] [connector-fake] Fake date calculation error(#2573)|https://github.com/apache/seatunnel/commit/9ea01298f|2.2.0-beta|
|[Bug][ConsoleSinkV2]fix fieldToString StackOverflow and add Unit-Test (#2545)|https://github.com/apache/seatunnel/commit/6f8709456|2.2.0-beta|
|[chore][connector-common] Rename SeatunnelSchema to SeaTunnelSchema (#2538)|https://github.com/apache/seatunnel/commit/7dc2a2738|2.2.0-beta|
|[Imporve][Fake-Connector-V2]support user-defined-schmea and random data for fake-table  (#2406)|https://github.com/apache/seatunnel/commit/a5447528c|2.2.0-beta|
|[api-draft][Optimize] Optimize module name (#2062)|https://github.com/apache/seatunnel/commit/f79e3112b|2.2.0-beta|

</details>
