<details><summary> Change Log </summary>

| Change | Commit | Version |
| --- | --- | --- |
|[Fix][Connector-V2] Fix StarRocksCatalogTest#testCatalog() NPE (#8987)|https://github.com/apache/seatunnel/commit/53f0a9eb5|2.3.10|
|[Improve][Connector-V2] Random pick the starrocks fe address which can be connected (#8898)|https://github.com/apache/seatunnel/commit/bef76078f|2.3.10|
|[Feature][Connector-v2] Support multi starrocks source (#8789)|https://github.com/apache/seatunnel/commit/26b5529aa|2.3.10|
|[Fix][Connector-V2] Fix possible data loss in scenarios of request_tablet_size is less than the number of BUCKETS (#8768)|https://github.com/apache/seatunnel/commit/3c6f21613|2.3.10|
|[Fix][Connector-V2]Fix Descriptions for CUSTOM_SQL in Connector (#8778)|https://github.com/apache/seatunnel/commit/96b610eb7|2.3.10|
|[Improve] restruct connector common options (#8634)|https://github.com/apache/seatunnel/commit/f3499a6ee|2.3.10|
|[improve] add StarRocks options (#8639)|https://github.com/apache/seatunnel/commit/da8d9cbd3|2.3.10|
|[Fix][Connector-V2] fix starRocks automatically creates tables with comment (#8568)|https://github.com/apache/seatunnel/commit/c4cb1fc4a|2.3.10|
|[Fix][Connector-V2] Fixed adding table comments (#8514)|https://github.com/apache/seatunnel/commit/edca75b0d|2.3.10|
|[Feature][Connector-V2] Starrocks implements multi table sink (#8467)|https://github.com/apache/seatunnel/commit/55eebfa8a|2.3.9|
|[Improve][Connector-V2] Add pre-check starrocks version before exeucte alter table field name (#8237)|https://github.com/apache/seatunnel/commit/c24e3b12b|2.3.9|
|[Fix][Connector-starrocks] Fix drop column bug for starrocks (#8216)|https://github.com/apache/seatunnel/commit/082814da1|2.3.9|
|[Feature][Core] Support read arrow data (#8137)|https://github.com/apache/seatunnel/commit/4710ea0f8|2.3.9|
|[Feature][Clickhouse] Support sink savemode  (#8086)|https://github.com/apache/seatunnel/commit/e6f92fd79|2.3.9|
|[Feature][Connector-V2] StarRocks-sink support schema evolution (#8082)|https://github.com/apache/seatunnel/commit/d33b0da8a|2.3.9|
|[Improve][dist]add shade check rule (#8136)|https://github.com/apache/seatunnel/commit/51ef80001|2.3.9|
|[Improve][Connector-V2] Add doris/starrocks create table with comment (#7847)|https://github.com/apache/seatunnel/commit/207b8c16f|2.3.9|
|[Feature][Restapi] Allow metrics information to be associated to logical plan nodes (#7786)|https://github.com/apache/seatunnel/commit/6b7c53d03|2.3.9|
|[Improve][API] Move catalog open to SaveModeHandler (#7439)|https://github.com/apache/seatunnel/commit/8c2c5c79a|2.3.8|
|[Improve][Connector-V2] Reuse connection in StarRocksCatalog (#7342)|https://github.com/apache/seatunnel/commit/8ee129d20|2.3.8|
|[Improve][Connector-V2] Remove system table limit (#7391)|https://github.com/apache/seatunnel/commit/adf888e00|2.3.8|
|[Improve][Connector-V2] Close all ResultSet after used (#7389)|https://github.com/apache/seatunnel/commit/853e97321|2.3.8|
|[Feature][Core] Support using upstream table placeholders in sink options and auto replacement (#7131)|https://github.com/apache/seatunnel/commit/c4ca74122|2.3.6|
|[Fix][Connector-V2] Fix starrocks Content-Length header already present error (#7034)|https://github.com/apache/seatunnel/commit/a485a74ef|2.3.6|
|[Feature][Connector-V2]Support StarRocks Fe Node HA|https://github.com/apache/seatunnel/commit/9c36c4581|2.3.6|
|[Fix][Connector-v2] Fix the sql statement error of create table for doris and starrocks (#6679)|https://github.com/apache/seatunnel/commit/88263cd69|2.3.6|
|[Fix][StarRocks] Fix NPE when upstream catalogtable table path only have table name part (#6540)|https://github.com/apache/seatunnel/commit/5795b265c|2.3.5|
|[Fix][Connector-V2] Fixed doris/starrocks create table sql parse error (#6580)|https://github.com/apache/seatunnel/commit/f2ed1fbde|2.3.5|
|[Fix][Connector-V2] Fix connector support SPI but without no args constructor (#6551)|https://github.com/apache/seatunnel/commit/5f3c9c36a|2.3.5|
|[Improve] Add SaveMode log of process detail (#6375)|https://github.com/apache/seatunnel/commit/b0d70ce22|2.3.5|
|[Improve][Connector-V2] Support TableSourceFactory on StarRocks (#6498)|https://github.com/apache/seatunnel/commit/aded56299|2.3.5|
|[Improve] StarRocksSourceReader  use the existing client  (#6480)|https://github.com/apache/seatunnel/commit/1a02c571a|2.3.5|
|[Improve][API] Unify type system api(data &amp; type) (#5872)|https://github.com/apache/seatunnel/commit/b38c7edcc|2.3.5|
|[Feature][Connector] add starrocks save_mode (#6029)|https://github.com/apache/seatunnel/commit/66b0f1e1d|2.3.4|
|[Feature] Add unsupported datatype check for all catalog (#5890)|https://github.com/apache/seatunnel/commit/b9791285a|2.3.4|
|[Improve] StarRocks support create table template with unique key (#5905)|https://github.com/apache/seatunnel/commit/25b01125e|2.3.4|
|[Improve][StarRocksSink] add http socket timeout. (#5918)|https://github.com/apache/seatunnel/commit/febdb262b|2.3.4|
|[Improve] Support create varchar field type in StarRocks (#5911)|https://github.com/apache/seatunnel/commit/602589516|2.3.4|
|[Improve]Change System.out.println to log output. (#5912)|https://github.com/apache/seatunnel/commit/bbedb07a9|2.3.4|
|[Improve][Common] Introduce new error define rule (#5793)|https://github.com/apache/seatunnel/commit/9d1b2582b|2.3.4|
|[Improve] Remove use `SeaTunnelSink::getConsumedType` method and mark it as deprecated (#5755)|https://github.com/apache/seatunnel/commit/8de740810|2.3.4|
|[Improve][Connector] Add field name to `DataTypeConvertor` to improve error message (#5782)|https://github.com/apache/seatunnel/commit/ab60790f0|2.3.4|
|[feature][connector-jdbc]Add Save Mode function and Connector-JDBC (MySQL) connector has been realized (#5663)|https://github.com/apache/seatunnel/commit/eff17ccbe|2.3.4|
|[Improve] Add default implement for `SeaTunnelSink::setTypeInfo` (#5682)|https://github.com/apache/seatunnel/commit/86cba8745|2.3.4|
|Support config column/primaryKey/constraintKey in schema (#5564)|https://github.com/apache/seatunnel/commit/eac76b4e5|2.3.4|
|[Improve] Refactor CatalogTable and add `SeaTunnelSource::getProducedCatalogTables` (#5562)|https://github.com/apache/seatunnel/commit/41173357f|2.3.4|
|[Hotfix][Connector-V2][StarRocks] fix starrocks template sql parser #5071 (#5332)|https://github.com/apache/seatunnel/commit/23d79b0d1|2.3.4|
|[Improve] [Connector-V2] Remove scheduler in StarRocks sink (#5269)|https://github.com/apache/seatunnel/commit/cb7b79491|2.3.4|
|[Improve][CheckStyle] Remove useless &#x27;SuppressWarnings&#x27; annotation of checkstyle. (#5260)|https://github.com/apache/seatunnel/commit/51c0d709b|2.3.4|
|[Hotfix] Fix com.google.common.base.Preconditions to seatunnel shade one (#5284)|https://github.com/apache/seatunnel/commit/ed5eadcf7|2.3.3|
|Fix StarRocksJsonSerializer will transform array/map/row to string (#5281)|https://github.com/apache/seatunnel/commit/f94195377|2.3.3|
|[Improve] Improve savemode api (#4767)|https://github.com/apache/seatunnel/commit/4acd370d4|2.3.3|
|[Improve] [Connector-V2] Improve StarRocks Auto Create Table To Support Use Primary Key Template In Field (#4487)|https://github.com/apache/seatunnel/commit/e601cd4c3|2.3.2|
|Revert &quot;[Improve][Catalog] refactor catalog (#4540)&quot; (#4628)|https://github.com/apache/seatunnel/commit/2d1933195|2.3.2|
|[hotfix][starrocks] fix error on get starrocks source typeInfo (#4619)|https://github.com/apache/seatunnel/commit/f7b094f9e|2.3.2|
|[Improve][Catalog] refactor catalog (#4540)|https://github.com/apache/seatunnel/commit/b0a701cb8|2.3.2|
|[Improve] [Connector-V2] Throw StarRocks Serialize Error To Client (#4484)|https://github.com/apache/seatunnel/commit/e2c107323|2.3.2|
|[Improve] [Connector-V2] Improve StarRocks Serialize Error Message (#4458)|https://github.com/apache/seatunnel/commit/465e75cbf|2.3.2|
|[Hotfix][Zeta] Adapt StarRocks With Multi-Table And Single-Table Mode (#4324)|https://github.com/apache/seatunnel/commit/c11c171d3|2.3.1|
|[improve][zeta] fix zeta bugs|https://github.com/apache/seatunnel/commit/3a82e8b39|2.3.1|
|[Improve] [Zeta] Improve Client Job Info Message|https://github.com/apache/seatunnel/commit/56febf011|2.3.1|
|[Fix] [Connector-V2] Fix StarRocksSink Without Format Field In Header|https://github.com/apache/seatunnel/commit/463ae6437|2.3.1|
|[Improve] Support StarRocksCatalog Use JDBC URL With Custom Suffix|https://github.com/apache/seatunnel/commit/d00ced6ec|2.3.1|
|[Improve] Support MySqlCatalog Use JDBC URL With Custom Suffix|https://github.com/apache/seatunnel/commit/210d0ff1f|2.3.1|
|[Improve] Change StarRocks Sink Default Format To Json|https://github.com/apache/seatunnel/commit/870335783|2.3.1|
|[Fix] Fix StarRocks Default Url Can&#x27;t Use|https://github.com/apache/seatunnel/commit/67c45d353|2.3.1|
|[hotfix] fixed schema options import error|https://github.com/apache/seatunnel/commit/656805f2d|2.3.1|
|[chore] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/291214ad6|2.3.1|
|Merge branch &#x27;dev&#x27; into merge/cdc|https://github.com/apache/seatunnel/commit/4324ee191|2.3.1|
|[Improve][Project] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/423b58303|2.3.1|
|[Fix] Fix StarRocks Default Url Can&#x27;t Use (#4229)|https://github.com/apache/seatunnel/commit/ed74d1109|2.3.1|
|[Bug] Remove StarRocks Auto Creat Table Default Value (#4220)|https://github.com/apache/seatunnel/commit/80b5cd40a|2.3.1|
|[Feature] Add SaveMode For StarRocks (#4217)|https://github.com/apache/seatunnel/commit/0674f10a5|2.3.1|
|[Improve] Improve StarRocks Catalog Base Url (#4215)|https://github.com/apache/seatunnel/commit/6632a4047|2.3.1|
|[Improve] Improve StarRocks Sink Config (#4212)|https://github.com/apache/seatunnel/commit/8d5712c1d|2.3.1|
|[Hotfix][Zeta] keep deleteCheckpoint method synchronized (#4209)|https://github.com/apache/seatunnel/commit/061f9b587|2.3.1|
|[Improve] Improve StarRocks Auto Create Table (#4208)|https://github.com/apache/seatunnel/commit/bc9cd6bf6|2.3.1|
|[hotfix][zeta] fix zeta multi-table parser error (#4193)|https://github.com/apache/seatunnel/commit/98f2ad0c1|2.3.1|
|[feature][starrocks] add StarRocks factories (#4191)|https://github.com/apache/seatunnel/commit/c485d887e|2.3.1|
|[Feature] Change StarRocks CreatTable Template (#4184)|https://github.com/apache/seatunnel/commit/4cf07f3be|2.3.1|
|[Feature][Connector-V2] StarRocks source connector (#3679)|https://github.com/apache/seatunnel/commit/9681173b1|2.3.1|
|[Improve] [Connector-V2] [StarRocks] Starrocks Support Auto Create Table (#4177)|https://github.com/apache/seatunnel/commit/7e0008e6f|2.3.1|
|[Improve][build] Give the maven module a human readable name (#4114)|https://github.com/apache/seatunnel/commit/d7cd60105|2.3.1|
|[Improve][Project] Code format with spotless plugin. (#4101)|https://github.com/apache/seatunnel/commit/a2ab16656|2.3.1|
|[Feature][Connector-v2][StarRocks] Support write cdc changelog event(INSERT/UPDATE/DELETE) (#3865)|https://github.com/apache/seatunnel/commit/8e3d158c0|2.3.1|
|[Improve] [Connector-V2] Change Connector Custom Config Prefix To Map (#3719)|https://github.com/apache/seatunnel/commit/ef1b8b1bb|2.3.1|
|[Improve][Connector-V2][StarRocks] Unified exception for StarRocks source and sink (#3593)|https://github.com/apache/seatunnel/commit/612d0297a|2.3.0|
|[Improve][Connector-V2][StarRocks] Delete the Mapper may not be used (#3579)|https://github.com/apache/seatunnel/commit/1e868ecf2|2.3.0|
|[Hotfix][OptionRule] Fix option rule about all connectors (#3592)|https://github.com/apache/seatunnel/commit/226dc6a11|2.3.0|
|[Improve][Connector-V2][StarRocks]Add StarRocks connector option rules (#3402)|https://github.com/apache/seatunnel/commit/5d187f69b|2.3.0|
|[Bugfix][Connector-V2][StarRocks]Fix StarRocks StreamLoad retry bug and fix doc (#3406)|https://github.com/apache/seatunnel/commit/071f9aa05|2.3.0|
|[Feature][Connector-V2] Starrocks sink connector (#3164)|https://github.com/apache/seatunnel/commit/3e6caf705|2.3.0|

</details>
