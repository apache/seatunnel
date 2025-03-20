<details><summary> Change Log </summary>

| Change | Commit | Version |
| --- | --- | --- |
|[Improve] restruct connector common options (#8634)|https://github.com/apache/seatunnel/commit/f3499a6eeb| dev |
|[improve] add assert options (#8620)|https://github.com/apache/seatunnel/commit/b159cc0c75| dev |
|[Feature][API] Support timestamp with timezone offset (#8367)|https://github.com/apache/seatunnel/commit/e18bfeabd2|2.3.9|
|[fix][connector-v2][connector-assert] Optimize Assert Sink verification method (#8356)|https://github.com/apache/seatunnel/commit/5c9159d7cd|2.3.9|
|[Improve][dist]add shade check rule (#8136)|https://github.com/apache/seatunnel/commit/51ef800016|2.3.9|
|[Feature][File] Support config null format for text file read (#8109)|https://github.com/apache/seatunnel/commit/2dbf02df47|2.3.9|
|[Feature][Transform-V2] Support transform with multi-table (#7628)|https://github.com/apache/seatunnel/commit/72c9c4576d|2.3.9|
|[Improve][API] Unified tables_configs and table_list (#8100)|https://github.com/apache/seatunnel/commit/84c0b8d660|2.3.9|
|[Fix][API] Fix column length can not be long (#8039)|https://github.com/apache/seatunnel/commit/16cf632d3e|2.3.9|
|[Feature][Restapi] Allow metrics information to be associated to logical plan nodes (#7786)|https://github.com/apache/seatunnel/commit/6b7c53d03c|2.3.9|
|[Feature][Connector-V2] Assert support multi-table check (#7687)|https://github.com/apache/seatunnel/commit/c4778a2497|2.3.8|
|[Feature][Transform] Add embedding transform (#7534)|https://github.com/apache/seatunnel/commit/3310cfcd34|2.3.8|
|[Improve][Connector] Add multi-table sink option check (#7360)|https://github.com/apache/seatunnel/commit/2489f6446b|2.3.7|
|[Feature][Core] Support using upstream table placeholders in sink options and auto replacement (#7131)|https://github.com/apache/seatunnel/commit/c4ca74122c|2.3.6|
|[Hotfix] fix http source can not read yyyy-MM-dd HH:mm:ss format bug &amp; Improve DateTime Utils (#6601)|https://github.com/apache/seatunnel/commit/19888e7969|2.3.5|
|[Feature][Connector-V2][Assert] Support field type assert and field value equality assert for full data types (#6275)|https://github.com/apache/seatunnel/commit/576919bfab|2.3.4|
|[Feature][Connector-V2][Assert] Support check the precision and scale of Decimal type. (#6110)|https://github.com/apache/seatunnel/commit/dd64ed52d4|2.3.4|
|[Hotfix][SQL Transform] Fix cast to timestamp, date, time bug (#5812)|https://github.com/apache/seatunnel/commit/de181de02a|2.3.4|
|[Improve][Common] Introduce new error define rule (#5793)|https://github.com/apache/seatunnel/commit/9d1b2582b2|2.3.4|
|[Improve] Remove use `SeaTunnelSink::getConsumedType` method and mark it as deprecated (#5755)|https://github.com/apache/seatunnel/commit/8de7408100|2.3.4|
|[Improve] Add default implement for `SeaTunnelSink::setTypeInfo` (#5682)|https://github.com/apache/seatunnel/commit/86cba87450|2.3.4|
|[Fix] Fix log error when multi-table sink close (#5683)|https://github.com/apache/seatunnel/commit/fea4b6f268|2.3.4|
|Support config tableIdentifier for schema (#5628)|https://github.com/apache/seatunnel/commit/652921fb75|2.3.4|
|[Feature] Add `table-names` from FakeSource/Assert to produce/assert multi-table (#5604)|https://github.com/apache/seatunnel/commit/2c67cd8f3e|2.3.4|
|[Improve] Remove useless ReadonlyConfig flatten feature (#5612)|https://github.com/apache/seatunnel/commit/243edfef3d|2.3.4|
|Support config column/primaryKey/constraintKey in schema (#5564)|https://github.com/apache/seatunnel/commit/eac76b4e50|2.3.4|
|[Improve][connector-assert]support &#x27;DECIMAL&#x27; type and fix &#x27;Number&#x27; type precision issue (#5479)|https://github.com/apache/seatunnel/commit/d308e27733|2.3.4|
|[Improve][CheckStyle] Remove useless &#x27;SuppressWarnings&#x27; annotation of checkstyle. (#5260)|https://github.com/apache/seatunnel/commit/51c0d709ba|2.3.4|
|[Feature][Transform] Add SimpleSQL transform plugin (#4148)|https://github.com/apache/seatunnel/commit/b914d49abf|2.3.1|
|[Improve][build] Give the maven module a human readable name (#4114)|https://github.com/apache/seatunnel/commit/d7cd601051|2.3.1|
|[Improve][Project] Code format with spotless plugin. (#4101)|https://github.com/apache/seatunnel/commit/a2ab166561|2.3.1|
|[Hotfix][OptionRule] Fix option rule about all connectors (#3592)|https://github.com/apache/seatunnel/commit/226dc6a119|2.3.0|
|[Improve][Connector-V2][Assert] Unified exception for assert connector (#3331)|https://github.com/apache/seatunnel/commit/e74c9bc6fd|2.3.0|
|[improve][connector] The Factory#factoryIdentifier must be consistent with PluginIdentifierInterface#getPluginName (#3328)|https://github.com/apache/seatunnel/commit/d9519d696a|2.3.0|
|[Improve][Connector-V2] Add Clickhouse and Assert Source/Sink Factory (#3306)|https://github.com/apache/seatunnel/commit/9e4a128381|2.3.0|
|[Feature][Connector-v2] improve assert sink connector (#2844)|https://github.com/apache/seatunnel/commit/967fec0e93|2.3.0-beta|
|[DEV][Api] Replace SeaTunnelContext with JobContext and remove singleton pattern (#2706)|https://github.com/apache/seatunnel/commit/cbf82f755c|2.2.0-beta|
|[improve][UT] Upgrade junit to 5.+ (#2305)|https://github.com/apache/seatunnel/commit/362319ff3e|2.2.0-beta|
|[checkstyle] Improved validation scope of MagicNumber (#2194)|https://github.com/apache/seatunnel/commit/6d08b5f369|2.2.0-beta|
|[API-DRAFT] [MERGE] update license and pom.xml|https://github.com/apache/seatunnel/commit/5ae8865b7c|2.2.0-beta|
|add assert sink to Api draft (#2071)|https://github.com/apache/seatunnel/commit/fc640b52bd|2.2.0-beta|

</details>
