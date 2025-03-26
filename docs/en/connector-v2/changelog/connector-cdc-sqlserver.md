<details><summary> Change Log </summary>

| Change | Commit | Version |
| --- | --- | --- |
|[Improve][CDC] Extract duplicate code (#8906)|https://github.com/apache/seatunnel/commit/b922bb90e|2.3.10|
|[Improve] restruct connector common options (#8634)|https://github.com/apache/seatunnel/commit/f3499a6ee|2.3.10|
|[Improve][dist]add shade check rule (#8136)|https://github.com/apache/seatunnel/commit/51ef80001|2.3.9|
|[Improve][Connector-V2] Add pre-check for table enable cdc (#8152)|https://github.com/apache/seatunnel/commit/9a5da7817|2.3.9|
|[Improve][Connector-V2] Fix SqlServer cdc memory leak (#8083)|https://github.com/apache/seatunnel/commit/69cd4ae1a|2.3.9|
|[Feature][Connector-V2]Jdbc chunk split add  snapshotSplitColumn config #7794 (#7840)|https://github.com/apache/seatunnel/commit/b6c6dc043|2.3.9|
|[Feature][Core] Support cdc task ddl restore for zeta (#7463)|https://github.com/apache/seatunnel/commit/8e322281e|2.3.9|
|[Feature][Connector-V2] SqlServer support user-defined type (#7706)|https://github.com/apache/seatunnel/commit/fb8903327|2.3.8|
|[Improve][Connector-V2] Optimize sqlserver package structure (#7715)|https://github.com/apache/seatunnel/commit/9720f118e|2.3.8|
|[Hotfix][CDC] Fix package name spelling mistake (#7415)|https://github.com/apache/seatunnel/commit/469112fa6|2.3.8|
|[Improve][CDC] Bump the version of debezium to 1.9.8.Final (#6740)|https://github.com/apache/seatunnel/commit/c3ac95352|2.3.6|
|[Improve][CDC] Close idle subtasks gorup(reader/writer) in increment phase (#6526)|https://github.com/apache/seatunnel/commit/454c339b9|2.3.6|
|[Improve][JDBC Source] Fix Split can not be cancel (#6825)|https://github.com/apache/seatunnel/commit/ee3b7c372|2.3.6|
|[Hotfix][Jdbc/CDC] Fix postgresql uuid type in jdbc read (#6684)|https://github.com/apache/seatunnel/commit/868ba4d7c|2.3.6|
|[Improve] Improve read table schema in cdc connector (#6702)|https://github.com/apache/seatunnel/commit/a8c6cc6e0|2.3.6|
|[Improve][Jdbc] Add quote identifier for sql (#6669)|https://github.com/apache/seatunnel/commit/849d748d3|2.3.5|
|[Improve][CDC] Optimize split state memory allocation in increment phase (#6554)|https://github.com/apache/seatunnel/commit/fe3342216|2.3.5|
|[Fix][Connector-V2] Fix connector support SPI but without no args constructor (#6551)|https://github.com/apache/seatunnel/commit/5f3c9c36a|2.3.5|
|[Improve][CDC-Connector]Fix CDC option rule. (#6454)|https://github.com/apache/seatunnel/commit/1ea27afa8|2.3.5|
|[Improve][CDC] Optimize memory allocation for snapshot split reading (#6281)|https://github.com/apache/seatunnel/commit/485664583|2.3.5|
|[Improve][API] Unify type system api(data &amp; type) (#5872)|https://github.com/apache/seatunnel/commit/b38c7edcc|2.3.5|
|[Improve] Support `int identity` type in sql server (#6186)|https://github.com/apache/seatunnel/commit/1a8da1c84|2.3.4|
|[Feature][CDC] Support custom table primary key (#6106)|https://github.com/apache/seatunnel/commit/1312a1dd2|2.3.4|
|[Feature][CDC] Support read no primary key table (#6098)|https://github.com/apache/seatunnel/commit/b42d78de3|2.3.4|
|[Hotfix][Jdbc] Fix jdbc setFetchSize error (#6005)|https://github.com/apache/seatunnel/commit/d41af8a6e|2.3.4|
|[Bug][CDC] Fix state recovery error when switching a single table to multiple tables (#5784)|https://github.com/apache/seatunnel/commit/37fcff347|2.3.4|
|[Improve][CDC] Clean unused code (#5785)|https://github.com/apache/seatunnel/commit/b5a66d3db|2.3.4|
|[Improve][Jdbc] Fix database identifier (#5756)|https://github.com/apache/seatunnel/commit/dbfc8a670|2.3.4|
|[improve][connector-v2][sqlserver-cdc]Unified sqlserver TypeUtils type conversion mode (#5668)|https://github.com/apache/seatunnel/commit/75b814bc3|2.3.4|
|[feature][connector-cdc-sqlserver] add dataType datetimeoffset (#5548)|https://github.com/apache/seatunnel/commit/0cf63eed6|2.3.4|
|[Improve] Remove catalog tag for config file (#5645)|https://github.com/apache/seatunnel/commit/dc509aa08|2.3.4|
|[Improve] Refactor CatalogTable and add `SeaTunnelSource::getProducedCatalogTables` (#5562)|https://github.com/apache/seatunnel/commit/41173357f|2.3.4|
|[Imporve] [CDC Base] Add a fast sampling method that supports character types (#5179)|https://github.com/apache/seatunnel/commit/c0422dbfe|2.3.3|
|[improve] [CDC Base] Add some split parameters to the optionRule (#5161)|https://github.com/apache/seatunnel/commit/94fd6755e|2.3.3|
|[Feature][Connector-V2][CDC] Support string type shard fields. (#5147)|https://github.com/apache/seatunnel/commit/e1be9d7f8|2.3.3|
|[Feature][CDC] Support tables without primary keys (with unique keys) (#163) (#5150)|https://github.com/apache/seatunnel/commit/32b7f2b69|2.3.3|
|[Bugfix][zeta] Fix cdc connection does not close (#4922)|https://github.com/apache/seatunnel/commit/a2d2f2dda|2.3.3|
|[Feature][CDC] Support disable/enable exactly once for INITIAL (#4921)|https://github.com/apache/seatunnel/commit/6d9a3e595|2.3.3|
|[Improve][CDC]change driver scope to provider (#5002)|https://github.com/apache/seatunnel/commit/745c0b9e9|2.3.3|
|[Improve][CDC]Remove  driver for cdc connector (#4952)|https://github.com/apache/seatunnel/commit/b65f40c3c|2.3.3|
|[Bugfix][zeta] Fix the deadlock issue with JDBC driver loading (#4878)|https://github.com/apache/seatunnel/commit/c30a2a1b1|2.3.2|
|[improve][CDC base] Implement Sample-based Sharding Strategy with Configurable Sampling Rate (#4856)|https://github.com/apache/seatunnel/commit/d827c700f|2.3.2|
|[Bugfix][CDC Base] Solving the ConcurrentModificationException caused by snapshotState being modified concurrently. (#4877)|https://github.com/apache/seatunnel/commit/9a2efa51c|2.3.2|
|[Hotfix][CDC] Fix chunk start/end parameter type error (#4777)|https://github.com/apache/seatunnel/commit/c13c03199|2.3.2|
|[Feature][CDC][SqlServer] Support multi-table read (#4377)|https://github.com/apache/seatunnel/commit/c4e3f2dc0|2.3.2|
|[Improve][CDC] Optimize jdbc fetch-size options (#4352)|https://github.com/apache/seatunnel/commit/fbb60ce1b|2.3.1|
|[Improve][CDC] Improve startup.mode/stop.mode options (#4360)|https://github.com/apache/seatunnel/commit/b71d8739d|2.3.1|
|[Improve][CDC] Optimize options &amp; add docs for compatible_debezium_json (#4351)|https://github.com/apache/seatunnel/commit/336f59049|2.3.1|
|Update CDC StartupMode and StopMode option to SingleChoiceOption (#4357)|https://github.com/apache/seatunnel/commit/f60ac1a5e|2.3.1|
|[bugfix][cdc-base] Fix cdc base shutdown thread not cleared (#4327)|https://github.com/apache/seatunnel/commit/ac61409bd|2.3.1|
|[improve][zeta] fix zeta bugs|https://github.com/apache/seatunnel/commit/3a82e8b39|2.3.1|
|[Improve] Support MySqlCatalog Use JDBC URL With Custom Suffix|https://github.com/apache/seatunnel/commit/210d0ff1f|2.3.1|
|Merge branch &#x27;dev&#x27; into merge/cdc|https://github.com/apache/seatunnel/commit/4324ee191|2.3.1|
|[Improve][Project] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/423b58303|2.3.1|
|[improve][cdc] support sharding-tables (#4207)|https://github.com/apache/seatunnel/commit/5c3f0c9b0|2.3.1|
|[Hotfix][CDC] Fix multiple-table data read (#4200)|https://github.com/apache/seatunnel/commit/7f5671d2c|2.3.1|
|[Improve][build] Give the maven module a human readable name (#4114)|https://github.com/apache/seatunnel/commit/d7cd60105|2.3.1|
|[Improve][Project] Code format with spotless plugin. (#4101)|https://github.com/apache/seatunnel/commit/a2ab16656|2.3.1|
|[Improve][Connector-V2][SQLServer-CDC] Add sqlserver cdc optionRule (#4019)|https://github.com/apache/seatunnel/commit/78df50339|2.3.1|
|[Improve][CDC][base] Guaranteed to be exactly-once in the process of switching from SnapshotTask to IncrementalTask (#3837)|https://github.com/apache/seatunnel/commit/8379aaf87|2.3.1|
|[Feature][API &amp; Connector &amp; Doc] add parallelism and column projection interface (#3829)|https://github.com/apache/seatunnel/commit/b9164b8ba|2.3.1|
|[Improve][CDC] Add mysql-cdc source factory (#3791)|https://github.com/apache/seatunnel/commit/356538de8|2.3.1|
|[feature][connector-v2] add sqlServer CDC (#3686)|https://github.com/apache/seatunnel/commit/0f0afb58a|2.3.0|

</details>
