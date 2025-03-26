<details><summary> Change Log </summary>

| Change | Commit | Version |
| --- | --- | --- |
|[Feature][elasticsearch-connector] support elasticsearch sql source (#8895)|https://github.com/apache/seatunnel/commit/814086279|2.3.10|
|[Fix] Fix error log name for SourceSplitEnumerator implements class (#8817)|https://github.com/apache/seatunnel/commit/55ed90eca|2.3.10|
|[Improve] restruct connector common options (#8634)|https://github.com/apache/seatunnel/commit/f3499a6ee|2.3.10|
|[improve] add Elasticsearch options (#8623)|https://github.com/apache/seatunnel/commit/d307ab44f|2.3.10|
|[Fix][connector-elasticsearch] support elasticsearch nest type &amp;&amp; spark with Array&lt;map&gt; (#8492)|https://github.com/apache/seatunnel/commit/92d2a4a10|2.3.10|
|Revert &quot;[Feature][connector-elasticsearch] elasticsearch support nested type (#8462)&quot; (#8485)|https://github.com/apache/seatunnel/commit/c68944893|2.3.9|
|[Feature][connector-elasticsearch] elasticsearch support nested type (#8462)|https://github.com/apache/seatunnel/commit/eaa15e4c8|2.3.9|
|[Feature][Elasticsearch] Support sink ddl  (#8412)|https://github.com/apache/seatunnel/commit/a4a38ccff|2.3.9|
|[hotfix][connector-elasticsearch-sink] Convert index to lowercase  (#8429)|https://github.com/apache/seatunnel/commit/46fcb237c|2.3.9|
|[Improve][Elasticsearch] Truncate the exception message body for request errors (#8263)|https://github.com/apache/seatunnel/commit/b9d850e61|2.3.9|
|[Improve][dist]add shade check rule (#8136)|https://github.com/apache/seatunnel/commit/51ef80001|2.3.9|
|[Feature][Restapi] Allow metrics information to be associated to logical plan nodes (#7786)|https://github.com/apache/seatunnel/commit/6b7c53d03|2.3.9|
|[Fix][Connector-V2] Fix known directory create and delete ignore issues (#7700)|https://github.com/apache/seatunnel/commit/e2fb67957|2.3.8|
|[Feature][Elastic search] Support multi-table source feature (#7502)|https://github.com/apache/seatunnel/commit/29fbeb254|2.3.8|
|[Hotfix][Connector-V2] Fix null not inserted in es (#7493)|https://github.com/apache/seatunnel/commit/a4ba6a171|2.3.8|
|[Improve][API] Move catalog open to SaveModeHandler (#7439)|https://github.com/apache/seatunnel/commit/8c2c5c79a|2.3.8|
|[Improve][Connector] Add multi-table sink option check (#7360)|https://github.com/apache/seatunnel/commit/2489f6446|2.3.7|
|[Feature][Core] Support using upstream table placeholders in sink options and auto replacement (#7131)|https://github.com/apache/seatunnel/commit/c4ca74122|2.3.6|
|[Fix][Connector-V2][Elasticsearch]Fix sink configuration for DROP_DATA (#7124)|https://github.com/apache/seatunnel/commit/bb9fd516e|2.3.6|
|[Feature][Elasticsearch] Support multi-table sink write #7041 (#7052)|https://github.com/apache/seatunnel/commit/45653e1d2|2.3.6|
|[Feature][Doris] Add Doris type converter (#6354)|https://github.com/apache/seatunnel/commit/518999184|2.3.6|
|[Fix][Connector-V2] Remove Some Incorrect Comments and Properties in ElasticsearchCommitInfo|https://github.com/apache/seatunnel/commit/720298775|2.3.6|
|[Bug][Improve][Connector-v2][ElasticsearchSource] Fix behavior when source empty，Support SourceConfig.SOURCE field empty. (#6425)|https://github.com/apache/seatunnel/commit/4e98eb863|2.3.6|
|[Improve][Connector-V2] Add ElasticSearch type converter (#6546)|https://github.com/apache/seatunnel/commit/505c1252b|2.3.5|
|[Fix][Connector-V2] Fix connector support SPI but without no args constructor (#6551)|https://github.com/apache/seatunnel/commit/5f3c9c36a|2.3.5|
|[Improve] Add SaveMode log of process detail (#6375)|https://github.com/apache/seatunnel/commit/b0d70ce22|2.3.5|
|[Improve][API] Unify type system api(data &amp; type) (#5872)|https://github.com/apache/seatunnel/commit/b38c7edcc|2.3.5|
|[Improve] Implement ElasticSearch connector factory (#6181)|https://github.com/apache/seatunnel/commit/1fd854de6|2.3.4|
|[Feature][Connector] add elasticsearch save_mode  (#6046)|https://github.com/apache/seatunnel/commit/716a36ac3|2.3.4|
|[Improve][Connector-V2] Replace CommonErrorCodeDeprecated.JSON_OPERATION_FAILED (#5978)|https://github.com/apache/seatunnel/commit/456cd1771|2.3.4|
|[Feature] Add unsupported datatype check for all catalog (#5890)|https://github.com/apache/seatunnel/commit/b9791285a|2.3.4|
|[BUG][Connector-V2] Fixed conversion exception of elasticsearch array format (#5825)|https://github.com/apache/seatunnel/commit/64f19f25d|2.3.4|
|[Improve][Common] Introduce new error define rule (#5793)|https://github.com/apache/seatunnel/commit/9d1b2582b|2.3.4|
|[Improve] Remove use `SeaTunnelSink::getConsumedType` method and mark it as deprecated (#5755)|https://github.com/apache/seatunnel/commit/8de740810|2.3.4|
|[Improve][Connector] Add field name to `DataTypeConvertor` to improve error message (#5782)|https://github.com/apache/seatunnel/commit/ab60790f0|2.3.4|
|Support config column/primaryKey/constraintKey in schema (#5564)|https://github.com/apache/seatunnel/commit/eac76b4e5|2.3.4|
|[Improve][CheckStyle] Remove useless &#x27;SuppressWarnings&#x27; annotation of checkstyle. (#5260)|https://github.com/apache/seatunnel/commit/51c0d709b|2.3.4|
|[Chore] Update the es version in the docs. (#4499)|https://github.com/apache/seatunnel/commit/415150635|2.3.2|
|[Improve][ElasticsearchSink]remove useless code. (#4500)|https://github.com/apache/seatunnel/commit/ef44c0d44|2.3.2|
|[Hotfix][Connector-V2][ES] Source deserializer error and inappropriate (#4233)|https://github.com/apache/seatunnel/commit/15530d278|2.3.2|
|[Feature][Connector-V2][ES] Support dsl filter (#4130)|https://github.com/apache/seatunnel/commit/79ca87833|2.3.1|
|[Bug][Connector-V2][ES]Fix es field type not support binary(#4240) (#4274)|https://github.com/apache/seatunnel/commit/84f10f201|2.3.1|
|Merge branch &#x27;dev&#x27; into merge/cdc|https://github.com/apache/seatunnel/commit/4324ee191|2.3.1|
|[Improve][Project] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/423b58303|2.3.1|
|Shade google common in hadoop (#4222)|https://github.com/apache/seatunnel/commit/537690507|2.3.1|
|Set es text type to string (#4192)|https://github.com/apache/seatunnel/commit/473971b94|2.3.1|
|[improve][api] Refactoring schema parse (#4157)|https://github.com/apache/seatunnel/commit/b2f573a13|2.3.1|
|Support ES catalog get field mapping (#4167)|https://github.com/apache/seatunnel/commit/72f241871|2.3.1|
|[Improve][build] Give the maven module a human readable name (#4114)|https://github.com/apache/seatunnel/commit/d7cd60105|2.3.1|
|[Bug][Connector-V2][ES]Fix es source no data (#4076)|https://github.com/apache/seatunnel/commit/a573b8dbe|2.3.1|
|Add convertor factory (#4119)|https://github.com/apache/seatunnel/commit/cbdea45d9|2.3.1|
|Add ElasticSearch catalog (#4108)|https://github.com/apache/seatunnel/commit/9ee4d8394|2.3.1|
|[Improve][Project] Code format with spotless plugin. (#4101)|https://github.com/apache/seatunnel/commit/a2ab16656|2.3.1|
|[Feature][Connector-V2][Elasticsearch] Support https protocol (#3997)|https://github.com/apache/seatunnel/commit/79b5cdd9c|2.3.1|
|[Feature][shade][Jackson] Add seatunnel-jackson module (#3947)|https://github.com/apache/seatunnel/commit/5d8862ec9|2.3.1|
|[Feature][Connector] add get source method to all source connector (#3846)|https://github.com/apache/seatunnel/commit/417178fb8|2.3.1|
|[Feature][API &amp; Connector &amp; Doc] add parallelism and column projection interface (#3829)|https://github.com/apache/seatunnel/commit/b9164b8ba|2.3.1|
|[hotfix][connector-v2][elasticsearch] Fix bulk refresh operation not locked (#3738)|https://github.com/apache/seatunnel/commit/b6cab90d2|2.3.0|
|[feature][connector-v2][elasticsearch] Support write cdc changelog event in elasticsearch sink (#3673)|https://github.com/apache/seatunnel/commit/3ec47c684|2.3.0|
|[Hotfix][OptionRule] Fix option rule about all connectors (#3592)|https://github.com/apache/seatunnel/commit/226dc6a11|2.3.0|
|[Improve][Connector-V2][ElasticSearch] Unified exception for ElasticSearch source &amp; sink connector (#3569)|https://github.com/apache/seatunnel/commit/b73944d1d|2.3.0|
|[Improve] [Connector-V2] Bad smell ToArrayCallWithZeroLengthArrayArgument: (#3577)|https://github.com/apache/seatunnel/commit/cc448d98c|2.3.0|
|[Improve][Connector-V2][ElasticSearch] Improve es bulk sink retriable mechanism (#3148)|https://github.com/apache/seatunnel/commit/02ef38eb7|2.3.0|
|[Connector-V2] [E2E] Add missed ElasticSearch E2E module. (#3338)|https://github.com/apache/seatunnel/commit/b2dad4d47|2.3.0|
|[Connector-V2] [ElasticSearch] Add ElasticSearch Source/Sink Factory (#3325)|https://github.com/apache/seatunnel/commit/38254e3f2|2.3.0|
|[Feature][Connector-V2][Elasticsearch] Support Elasticsearch source (#2821)|https://github.com/apache/seatunnel/commit/ded5481d9|2.3.0|
|update (#3149)|https://github.com/apache/seatunnel/commit/59abe4ad6|2.3.0|
|[Improve][all] change Log to @Slf4j (#3001)|https://github.com/apache/seatunnel/commit/6016100f1|2.3.0-beta|
|[Connector-V2] [ElasticSearch] Fix ElasticSearch Connector V2 Bug (#2817)|https://github.com/apache/seatunnel/commit/2fcbbf464|2.2.0-beta|
|[DEV][Api] Replace SeaTunnelContext with JobContext and remove singleton pattern (#2706)|https://github.com/apache/seatunnel/commit/cbf82f755|2.2.0-beta|
|[#2606]Dependency management split (#2630)|https://github.com/apache/seatunnel/commit/fc047be69|2.2.0-beta|
|[Feature][Connector-V2] new connecotor of Elasticsearch sink(#2326) (#2330)|https://github.com/apache/seatunnel/commit/2a1fd5027|2.2.0-beta|

</details>
