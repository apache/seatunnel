| Change | Commit |
| --- | --- |
|[Feature][Connector-v2] Support multi starrocks source (#8789)|https://github.com/apache/seatunnel/commit/26b5529aaf|
|[Fix][Connector-V2] Fix possible data loss in scenarios of request_tablet_size is less than the number of BUCKETS (#8768)|https://github.com/apache/seatunnel/commit/3c6f216135|
|[Fix][Connector-V2]Fix Descriptions for CUSTOM_SQL in Connector (#8778)|https://github.com/apache/seatunnel/commit/96b610eb7e|
|[Improve] restruct connector common options (#8634)|https://github.com/apache/seatunnel/commit/f3499a6eeb|
|[improve] add StarRocks options (#8639)|https://github.com/apache/seatunnel/commit/da8d9cbd35|
|[Fix][Connector-V2] fix starRocks automatically creates tables with comment (#8568)|https://github.com/apache/seatunnel/commit/c4cb1fc4a3|
|[Fix][Connector-V2] Fixed adding table comments (#8514)|https://github.com/apache/seatunnel/commit/edca75b0d6|
|[Feature][Connector-V2] Starrocks implements multi table sink (#8467)|https://github.com/apache/seatunnel/commit/55eebfa8af|
|[Improve][Connector-V2] Add pre-check starrocks version before exeucte alter table field name (#8237)|https://github.com/apache/seatunnel/commit/c24e3b12ba|
|[Fix][Connector-starrocks] Fix drop column bug for starrocks (#8216)|https://github.com/apache/seatunnel/commit/082814da1f|
|[Feature][Core] Support read arrow data (#8137)|https://github.com/apache/seatunnel/commit/4710ea0f8d|
|[Feature][Clickhouse] Support sink savemode  (#8086)|https://github.com/apache/seatunnel/commit/e6f92fd79b|
|[Feature][Connector-V2] StarRocks-sink support schema evolution (#8082)|https://github.com/apache/seatunnel/commit/d33b0da8ab|
|[Improve][dist]add shade check rule (#8136)|https://github.com/apache/seatunnel/commit/51ef800016|
|[Improve][Connector-V2] Add doris/starrocks create table with comment (#7847)|https://github.com/apache/seatunnel/commit/207b8c16fd|
|[Feature][Restapi] Allow metrics information to be associated to logical plan nodes (#7786)|https://github.com/apache/seatunnel/commit/6b7c53d03c|
|[Improve][API] Move catalog open to SaveModeHandler (#7439)|https://github.com/apache/seatunnel/commit/8c2c5c79a1|
|[Improve][Connector-V2] Reuse connection in StarRocksCatalog (#7342)|https://github.com/apache/seatunnel/commit/8ee129d20f|
|[Improve][Connector-V2] Remove system table limit (#7391)|https://github.com/apache/seatunnel/commit/adf888e008|
|[Improve][Connector-V2] Close all ResultSet after used (#7389)|https://github.com/apache/seatunnel/commit/853e973212|
|[Feature][Core] Support using upstream table placeholders in sink options and auto replacement (#7131)|https://github.com/apache/seatunnel/commit/c4ca74122c|
|[Fix][Connector-V2] Fix starrocks Content-Length header already present error (#7034)|https://github.com/apache/seatunnel/commit/a485a74eff|
|[Feature][Connector-V2]Support StarRocks Fe Node HA|https://github.com/apache/seatunnel/commit/9c36c45819|
|[Fix][Connector-v2] Fix the sql statement error of create table for doris and starrocks (#6679)|https://github.com/apache/seatunnel/commit/88263cd69f|
|[Fix][StarRocks] Fix NPE when upstream catalogtable table path only have table name part (#6540)|https://github.com/apache/seatunnel/commit/5795b265cc|
|[Fix][Connector-V2] Fixed doris/starrocks create table sql parse error (#6580)|https://github.com/apache/seatunnel/commit/f2ed1fbde0|
|[Fix][Connector-V2] Fix connector support SPI but without no args constructor (#6551)|https://github.com/apache/seatunnel/commit/5f3c9c36a5|
|[Improve] Add SaveMode log of process detail (#6375)|https://github.com/apache/seatunnel/commit/b0d70ce224|
|[Improve][Connector-V2] Support TableSourceFactory on StarRocks (#6498)|https://github.com/apache/seatunnel/commit/aded56299c|
|[Improve] StarRocksSourceReader  use the existing client  (#6480)|https://github.com/apache/seatunnel/commit/1a02c571a9|
|[Improve][API] Unify type system api(data & type) (#5872)|https://github.com/apache/seatunnel/commit/b38c7edcc9|
|[Feature][Connector] add starrocks save_mode (#6029)|https://github.com/apache/seatunnel/commit/66b0f1e1d2|
|[Feature] Add unsupported datatype check for all catalog (#5890)|https://github.com/apache/seatunnel/commit/b9791285a0|
|[Improve] StarRocks support create table template with unique key (#5905)|https://github.com/apache/seatunnel/commit/25b01125e4|
|[Improve][StarRocksSink] add http socket timeout. (#5918)|https://github.com/apache/seatunnel/commit/febdb262b6|
|[Improve] Support create varchar field type in StarRocks (#5911)|https://github.com/apache/seatunnel/commit/6025895167|
|[Improve]Change System.out.println to log output. (#5912)|https://github.com/apache/seatunnel/commit/bbedb07a9c|
|[Improve][Common] Introduce new error define rule (#5793)|https://github.com/apache/seatunnel/commit/9d1b2582b2|
|[Improve] Remove use `SeaTunnelSink::getConsumedType` method and mark it as deprecated (#5755)|https://github.com/apache/seatunnel/commit/8de7408100|
|[Improve][Connector] Add field name to `DataTypeConvertor` to improve error message (#5782)|https://github.com/apache/seatunnel/commit/ab60790f0d|
|[feature][connector-jdbc]Add Save Mode function and Connector-JDBC (MySQL) connector has been realized (#5663)|https://github.com/apache/seatunnel/commit/eff17ccbe5|
|[Improve] Add default implement for `SeaTunnelSink::setTypeInfo` (#5682)|https://github.com/apache/seatunnel/commit/86cba87450|
|Support config column/primaryKey/constraintKey in schema (#5564)|https://github.com/apache/seatunnel/commit/eac76b4e50|
|[Improve] Refactor CatalogTable and add `SeaTunnelSource::getProducedCatalogTables` (#5562)|https://github.com/apache/seatunnel/commit/41173357f8|
|[Hotfix][Connector-V2][StarRocks] fix starrocks template sql parser #5071 (#5332)|https://github.com/apache/seatunnel/commit/23d79b0d17|
|[Improve] [Connector-V2] Remove scheduler in StarRocks sink (#5269)|https://github.com/apache/seatunnel/commit/cb7b794914|
|[Improve][CheckStyle] Remove useless 'SuppressWarnings' annotation of checkstyle. (#5260)|https://github.com/apache/seatunnel/commit/51c0d709ba|
|[Hotfix] Fix com.google.common.base.Preconditions to seatunnel shade one (#5284)|https://github.com/apache/seatunnel/commit/ed5eadcf73|
|Fix StarRocksJsonSerializer will transform array/map/row to string (#5281)|https://github.com/apache/seatunnel/commit/f941953774|
|[Improve] Improve savemode api (#4767)|https://github.com/apache/seatunnel/commit/4acd370d48|
|[Improve] [Connector-V2] Improve StarRocks Auto Create Table To Support Use Primary Key Template In Field (#4487)|https://github.com/apache/seatunnel/commit/e601cd4c37|
|Revert "[Improve][Catalog] refactor catalog (#4540)" (#4628)|https://github.com/apache/seatunnel/commit/2d1933195d|
|[hotfix][starrocks] fix error on get starrocks source typeInfo (#4619)|https://github.com/apache/seatunnel/commit/f7b094f9eb|
|[Improve][Catalog] refactor catalog (#4540)|https://github.com/apache/seatunnel/commit/b0a701cb83|
|[Improve] [Connector-V2] Throw StarRocks Serialize Error To Client (#4484)|https://github.com/apache/seatunnel/commit/e2c107323b|
|[Improve] [Connector-V2] Improve StarRocks Serialize Error Message (#4458)|https://github.com/apache/seatunnel/commit/465e75cbf5|
|[Hotfix][Zeta] Adapt StarRocks With Multi-Table And Single-Table Mode (#4324)|https://github.com/apache/seatunnel/commit/c11c171d36|
|[improve][zeta] fix zeta bugs|https://github.com/apache/seatunnel/commit/3a82e8b39f|
|[Improve] [Zeta] Improve Client Job Info Message|https://github.com/apache/seatunnel/commit/56febf0118|
|[Fix] [Connector-V2] Fix StarRocksSink Without Format Field In Header|https://github.com/apache/seatunnel/commit/463ae6437e|
|[Improve] Support StarRocksCatalog Use JDBC URL With Custom Suffix|https://github.com/apache/seatunnel/commit/d00ced6ecd|
|[Improve] Support MySqlCatalog Use JDBC URL With Custom Suffix|https://github.com/apache/seatunnel/commit/210d0ff1f8|
|[Improve] Change StarRocks Sink Default Format To Json|https://github.com/apache/seatunnel/commit/8703357830|
|[Fix] Fix StarRocks Default Url Can't Use|https://github.com/apache/seatunnel/commit/67c45d353a|
|[hotfix] fixed schema options import error|https://github.com/apache/seatunnel/commit/656805f2df|
|[chore] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/291214ad6f|
|Merge branch 'dev' into merge/cdc|https://github.com/apache/seatunnel/commit/4324ee1912|
|[Improve][Project] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/423b583038|
|[Fix] Fix StarRocks Default Url Can't Use (#4229)|https://github.com/apache/seatunnel/commit/ed74d11090|
|[Bug] Remove StarRocks Auto Creat Table Default Value (#4220)|https://github.com/apache/seatunnel/commit/80b5cd40ae|
|[Feature] Add SaveMode For StarRocks (#4217)|https://github.com/apache/seatunnel/commit/0674f10a53|
|[Improve] Improve StarRocks Catalog Base Url (#4215)|https://github.com/apache/seatunnel/commit/6632a40473|
|[Improve] Improve StarRocks Sink Config (#4212)|https://github.com/apache/seatunnel/commit/8d5712c1db|
|[Hotfix][Zeta] keep deleteCheckpoint method synchronized (#4209)|https://github.com/apache/seatunnel/commit/061f9b5872|
|[Improve] Improve StarRocks Auto Create Table (#4208)|https://github.com/apache/seatunnel/commit/bc9cd6bf69|
|[hotfix][zeta] fix zeta multi-table parser error (#4193)|https://github.com/apache/seatunnel/commit/98f2ad0c19|
|[feature][starrocks] add StarRocks factories (#4191)|https://github.com/apache/seatunnel/commit/c485d887ec|
|[Feature] Change StarRocks CreatTable Template (#4184)|https://github.com/apache/seatunnel/commit/4cf07f3beb|
|[Feature][Connector-V2] StarRocks source connector (#3679)|https://github.com/apache/seatunnel/commit/9681173b10|
|[Improve] [Connector-V2] [StarRocks] Starrocks Support Auto Create Table (#4177)|https://github.com/apache/seatunnel/commit/7e0008e6fb|
|[Improve][build] Give the maven module a human readable name (#4114)|https://github.com/apache/seatunnel/commit/d7cd601051|
|[Improve][Project] Code format with spotless plugin. (#4101)|https://github.com/apache/seatunnel/commit/a2ab166561|
|[Feature][Connector-v2][StarRocks] Support write cdc changelog event(INSERT/UPDATE/DELETE) (#3865)|https://github.com/apache/seatunnel/commit/8e3d158c03|
|[Improve] [Connector-V2] Change Connector Custom Config Prefix To Map (#3719)|https://github.com/apache/seatunnel/commit/ef1b8b1bb5|
|[Improve][Connector-V2][StarRocks] Unified exception for StarRocks source and sink (#3593)|https://github.com/apache/seatunnel/commit/612d0297a0|
|[Improve][Connector-V2][StarRocks] Delete the Mapper may not be used (#3579)|https://github.com/apache/seatunnel/commit/1e868ecf28|
|[Hotfix][OptionRule] Fix option rule about all connectors (#3592)|https://github.com/apache/seatunnel/commit/226dc6a119|
|[Improve][Connector-V2][StarRocks]Add StarRocks connector option rules (#3402)|https://github.com/apache/seatunnel/commit/5d187f69b7|
|[Bugfix][Connector-V2][StarRocks]Fix StarRocks StreamLoad retry bug and fix doc (#3406)|https://github.com/apache/seatunnel/commit/071f9aa055|
|[Feature][Connector-V2] Starrocks sink connector (#3164)|https://github.com/apache/seatunnel/commit/3e6caf7053|
