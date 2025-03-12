<details><summary> Change Log </summary>

| Change | Commit | Version |
| --- | --- | --- |
|[Improve][Redis] Optimized Redis connection params (#8841)|https://github.com/apache/seatunnel/commit/e56f06cdf0| dev |
|[Improve] restruct connector common options (#8634)|https://github.com/apache/seatunnel/commit/f3499a6eeb| dev |
|[improve] update Redis connector config option (#8631)|https://github.com/apache/seatunnel/commit/f1c313eea6| dev |
|[Feature][Redis] Flush data when the time reaches checkpoint.interval and update test case (#8308)|https://github.com/apache/seatunnel/commit/e15757bcd7|2.3.9|
|Revert &quot;[Feature][Redis] Flush data when the time reaches checkpoint interval&quot; and &quot;[Feature][CDC] Add &#x27;schema-changes.enabled&#x27; options&quot; (#8278)|https://github.com/apache/seatunnel/commit/fcb2938286|2.3.9|
|[Feature][Redis] Flush data when the time reaches checkpoint.interval (#8198)|https://github.com/apache/seatunnel/commit/2e24941e6a|2.3.9|
|[Hotfix] Fix redis sink NPE (#8171)|https://github.com/apache/seatunnel/commit/6b9074e769|2.3.9|
|[Improve][dist]add shade check rule (#8136)|https://github.com/apache/seatunnel/commit/51ef800016|2.3.9|
|[Feature] [Connector-Redis] Redis connector support delete data (#7994)|https://github.com/apache/seatunnel/commit/02a35c3979|2.3.9|
|[Improve][Connector-V2] Redis support custom key and value (#7888)|https://github.com/apache/seatunnel/commit/ef2c3c7283|2.3.9|
|[Feature][Restapi] Allow metrics information to be associated to logical plan nodes (#7786)|https://github.com/apache/seatunnel/commit/6b7c53d03c|2.3.9|
|[improve][Redis]Redis scan command supports versions 5, 6, 7 (#7666)|https://github.com/apache/seatunnel/commit/6e70cbe334|2.3.8|
|[Improve][Connector] Add multi-table sink option check (#7360)|https://github.com/apache/seatunnel/commit/2489f6446b|2.3.7|
|[Feature][Core] Support using upstream table placeholders in sink options and auto replacement (#7131)|https://github.com/apache/seatunnel/commit/c4ca74122c|2.3.6|
|[Improve][Redis] Redis reader use scan cammnd instead of keys, single mode reader/writer support batch (#7087)|https://github.com/apache/seatunnel/commit/be37f05c07|2.3.6|
|[Feature][Kafka] Support multi-table source read  (#5992)|https://github.com/apache/seatunnel/commit/60104602d1|2.3.6|
|[Improve][Connector-V2]Support multi-table sink feature for redis (#6314)|https://github.com/apache/seatunnel/commit/fed89ae3fc|2.3.5|
|[Feature][Core] Upgrade flink source translation (#5100)|https://github.com/apache/seatunnel/commit/5aabb14a94|2.3.4|
|[Feature][Connector-V2] Support TableSourceFactory/TableSinkFactory on redis  (#5901)|https://github.com/apache/seatunnel/commit/e84dcb8c10|2.3.4|
|[Improve][Common] Introduce new error define rule (#5793)|https://github.com/apache/seatunnel/commit/9d1b2582b2|2.3.4|
|[Improve] Remove use `SeaTunnelSink::getConsumedType` method and mark it as deprecated (#5755)|https://github.com/apache/seatunnel/commit/8de7408100|2.3.4|
|[Improve][Connector-v2][Redis] Redis support select db (#5570)|https://github.com/apache/seatunnel/commit/77fbbbd0ee|2.3.4|
|Support config column/primaryKey/constraintKey in schema (#5564)|https://github.com/apache/seatunnel/commit/eac76b4e50|2.3.4|
|[Feature][Connector-v2][RedisSink]Support redis to set expiration time. (#4975)|https://github.com/apache/seatunnel/commit/b5321ff1d2|2.3.3|
|Merge branch &#x27;dev&#x27; into merge/cdc|https://github.com/apache/seatunnel/commit/4324ee1912|2.3.1|
|[Improve][Project] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/423b583038|2.3.1|
|[improve][api] Refactoring schema parse (#4157)|https://github.com/apache/seatunnel/commit/b2f573a13e|2.3.1|
|[Improve][build] Give the maven module a human readable name (#4114)|https://github.com/apache/seatunnel/commit/d7cd601051|2.3.1|
|[Improve][Project] Code format with spotless plugin. (#4101)|https://github.com/apache/seatunnel/commit/a2ab166561|2.3.1|
|[Feature][Connector] add get source method to all source connector (#3846)|https://github.com/apache/seatunnel/commit/417178fb84|2.3.1|
|[Hotfix][OptionRule] Fix option rule about all connectors (#3592)|https://github.com/apache/seatunnel/commit/226dc6a119|2.3.0|
|[Improve][Connector-V2][Redis] Unified exception for redis source &amp; sink exception (#3517)|https://github.com/apache/seatunnel/commit/205f782585|2.3.0|
|options in conditional need add to required or optional options (#3501)|https://github.com/apache/seatunnel/commit/51d5bcba10|2.3.0|
|[feature][api] add option validation for the ReadonlyConfig (#3417)|https://github.com/apache/seatunnel/commit/4f824fea36|2.3.0|
|[Feature][Redis Connector V2] Add Redis Connector Option Rules &amp; Improve Redis Connector doc (#3320)|https://github.com/apache/seatunnel/commit/1c10aacb30|2.3.0|
|[Connector-V2] [ElasticSearch] Add ElasticSearch Source/Sink Factory (#3325)|https://github.com/apache/seatunnel/commit/38254e3f26|2.3.0|
|[Improve][Connector-V2][Redis] Support redis cluster connection &amp; user authentication (#3188)|https://github.com/apache/seatunnel/commit/c7275a49cc|2.3.0|
|[DEV][Api] Replace SeaTunnelContext with JobContext and remove singleton pattern (#2706)|https://github.com/apache/seatunnel/commit/cbf82f755c|2.2.0-beta|
|[Feature][Connector-V2] Add redis sink connector (#2647)|https://github.com/apache/seatunnel/commit/71a9e4b019|2.2.0-beta|
|[#2606]Dependency management split (#2630)|https://github.com/apache/seatunnel/commit/fc047be69b|2.2.0-beta|
|[Feature][Connector-V2] Add redis source connector (#2569)|https://github.com/apache/seatunnel/commit/405f7d6f99|2.2.0-beta|

</details>
