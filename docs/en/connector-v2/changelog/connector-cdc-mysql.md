| Change | Commit |
| --- | --- |
|[Improve] restruct connector common options (#8634)|https://github.com/apache/seatunnel/commit/f3499a6eeb|
|[Fix][mysql-cdc] Fix GTIDs on startup to correctly recover from checkpoint (#8528)|https://github.com/apache/seatunnel/commit/82e4096c08|
|[Feature][MySQL-CDC] Support database/table wildcards scan read (#8323)|https://github.com/apache/seatunnel/commit/2116843ce8|
|[Feature][Jdbc] Support sink ddl for postgresql (#8276)|https://github.com/apache/seatunnel/commit/353bbd21a1|
|[Feature][CDC] Add 'schema-changes.enabled' options (#8285)|https://github.com/apache/seatunnel/commit/8e29ecf54f|
|Revert "[Feature][Redis] Flush data when the time reaches checkpoint interval" and "[Feature][CDC] Add 'schema-changes.enabled' options" (#8278)|https://github.com/apache/seatunnel/commit/fcb2938286|
|[Feature][CDC] Add 'schema-changes.enabled' options (#8252)|https://github.com/apache/seatunnel/commit/d783f9447c|
|[Improve][dist]add shade check rule (#8136)|https://github.com/apache/seatunnel/commit/51ef800016|
|[Feature][Connector-V2]Jdbc chunk split add  snapshotSplitColumn config #7794 (#7840)|https://github.com/apache/seatunnel/commit/b6c6dc0438|
|[Feature][Core] Support cdc task ddl restore for zeta (#7463)|https://github.com/apache/seatunnel/commit/8e322281ed|
|[Feature][Connector-v2] Support schema evolution for Oracle connector (#7908)|https://github.com/apache/seatunnel/commit/79406bcc2f|
|[Hotfix][CDC] Fix ddl duplicate execution error when config multi_table_sink_replica (#7634)|https://github.com/apache/seatunnel/commit/23ab3edbbb|
|[Hotfix][CDC] Fix package name spelling mistake (#7415)|https://github.com/apache/seatunnel/commit/469112fa64|
|[Hotfix][MySQL-CDC] Fix ArrayIndexOutOfBoundsException in mysql binlog read (#7381)|https://github.com/apache/seatunnel/commit/40c5f313eb|
|[Improve][Connector-V2] Support schema evolution for mysql-cdc and mysql-jdbc (#6929)|https://github.com/apache/seatunnel/commit/cf91e51fc7|
|[Hotfix][MySQL-CDC] Fix read gbk varchar chinese garbled characters (#7046)|https://github.com/apache/seatunnel/commit/4e4d2b8ee5|
|[Improve][CDC] Bump the version of debezium to 1.9.8.Final (#6740)|https://github.com/apache/seatunnel/commit/c3ac953524|
|[Improve][CDC] Close idle subtasks gorup(reader/writer) in increment phase (#6526)|https://github.com/apache/seatunnel/commit/454c339b9c|
|[Improve][JDBC Source] Fix Split can not be cancel (#6825)|https://github.com/apache/seatunnel/commit/ee3b7c3723|
|[Hotfix][Jdbc/CDC] Fix postgresql uuid type in jdbc read (#6684)|https://github.com/apache/seatunnel/commit/868ba4d7c7|
|[Improve][mysql-cdc] Support mysql 5.5 versions (#6710)|https://github.com/apache/seatunnel/commit/058f5594a3|
|[Improve][mysql-cdc] Fallback to desc table when show create table failed (#6701)|https://github.com/apache/seatunnel/commit/6f74663c08|
|[Improve][Jdbc] Add quote identifier for sql (#6669)|https://github.com/apache/seatunnel/commit/849d748d3d|
|[Fix][Connector-V2] Fix connector support SPI but without no args constructor (#6551)|https://github.com/apache/seatunnel/commit/5f3c9c36a5|
|[Improve][CDC-Connector]Fix CDC option rule. (#6454)|https://github.com/apache/seatunnel/commit/1ea27afa87|
|[Improve][CDC] Optimize memory allocation for snapshot split reading (#6281)|https://github.com/apache/seatunnel/commit/4856645837|
|[Improve][API] Unify type system api(data & type) (#5872)|https://github.com/apache/seatunnel/commit/b38c7edcc9|
|[Feature][CDC] Support custom table primary key (#6106)|https://github.com/apache/seatunnel/commit/1312a1dd27|
|[Feature][CDC] Support read no primary key table (#6098)|https://github.com/apache/seatunnel/commit/b42d78de3f|
|[Bug][CDC] Fix state recovery error when switching a single table to multiple tables (#5784)|https://github.com/apache/seatunnel/commit/37fcff347e|
|[Feature][formats][ogg] Support read ogg format message #4201 (#4225)|https://github.com/apache/seatunnel/commit/7728e241e8|
|[Improve][CDC] Clean unused code (#5785)|https://github.com/apache/seatunnel/commit/b5a66d3dbe|
|[Improve][Jdbc] Fix database identifier (#5756)|https://github.com/apache/seatunnel/commit/dbfc8a670a|
|[improve][mysql-cdc] Optimize the default value range of mysql server-id to reduce conflicts. (#5550)|https://github.com/apache/seatunnel/commit/5174639463|
|[Improve] Remove catalog tag for config file (#5645)|https://github.com/apache/seatunnel/commit/dc509aa080|
|[Improve][Pom] Add junit4 to the root pom (#5611)|https://github.com/apache/seatunnel/commit/7b4f7db2a2|
|[Improve] Refactor CatalogTable and add `SeaTunnelSource::getProducedCatalogTables` (#5562)|https://github.com/apache/seatunnel/commit/41173357f8|
|[Improve][connector-cdc-mysql] avoid listing tables under unnecessary databases (#5365)|https://github.com/apache/seatunnel/commit/3e5d018b35|
|[Improve][Docs] Refactor MySQL-CDC docs (#5302)|https://github.com/apache/seatunnel/commit/74530a0461|
|[Improve][CheckStyle] Remove useless 'SuppressWarnings' annotation of checkstyle. (#5260)|https://github.com/apache/seatunnel/commit/51c0d709ba|
|[Hotfix] Fix com.google.common.base.Preconditions to seatunnel shade one (#5284)|https://github.com/apache/seatunnel/commit/ed5eadcf73|
|[Imporve] [CDC Base] Add a fast sampling method that supports character types (#5179)|https://github.com/apache/seatunnel/commit/c0422dbfeb|
|[improve] [CDC Base] Add some split parameters to the optionRule (#5161)|https://github.com/apache/seatunnel/commit/94fd6755e6|
|[Improve][CDC] support exactly-once of cdc and fix the BinlogOffset comparing bug (#5057)|https://github.com/apache/seatunnel/commit/0e4190ab2e|
|[Feature][Connector-V2][CDC] Support string type shard fields. (#5147)|https://github.com/apache/seatunnel/commit/e1be9d7f8a|
|[Feature][CDC] Support tables without primary keys (with unique keys) (#163) (#5150)|https://github.com/apache/seatunnel/commit/32b7f2b690|
|[Feature][Connector-V2][mysql cdc] Conversion of tinyint(1) to bool is supported (#5105)|https://github.com/apache/seatunnel/commit/86b1b7e31a|
|[Feature][connector-v2][mongodbcdc]Support source mongodb cdc (#4923)|https://github.com/apache/seatunnel/commit/d729fcba4c|
|[Bugfix][connector-cdc-mysql] Fix listener not released when BinlogClient reuse (#5011)|https://github.com/apache/seatunnel/commit/3287b1d852|
|[BugFix] [Connector-V2] [MySQL-CDC] serverId from int to long (#5033) (#5035)|https://github.com/apache/seatunnel/commit/4abc80e111|
|[Hotfix][CDC] Fix jdbc connection leak for mysql (#5037)|https://github.com/apache/seatunnel/commit/738925ba10|
|[Feature][CDC] Support disable/enable exactly once for INITIAL (#4921)|https://github.com/apache/seatunnel/commit/6d9a3e5957|
|[Improve][CDC]change driver scope to provider (#5002)|https://github.com/apache/seatunnel/commit/745c0b9e92|
|[Improve][CDC]Remove  driver for cdc connector (#4952)|https://github.com/apache/seatunnel/commit/b65f40c3c9|
|[improve][CDC base] Implement Sample-based Sharding Strategy with Configurable Sampling Rate (#4856)|https://github.com/apache/seatunnel/commit/d827c700f0|
|[Hotfix][CDC] Fix chunk start/end parameter type error (#4777)|https://github.com/apache/seatunnel/commit/c13c031995|
|[feature][catalog] Support for multiplexing connections (#4550)|https://github.com/apache/seatunnel/commit/41277d7f78|
|[BugFix][Mysql-CDC] Fix Time data type is empty when reading from MySQL CDC (#4670)|https://github.com/apache/seatunnel/commit/e4f973daf7|
|[Improve][CDC] Optimize jdbc fetch-size options (#4352)|https://github.com/apache/seatunnel/commit/fbb60ce1be|
|[Improve][CDC] Improve startup.mode/stop.mode options (#4360)|https://github.com/apache/seatunnel/commit/b71d8739d5|
|Update CDC StartupMode and StopMode option to SingleChoiceOption (#4357)|https://github.com/apache/seatunnel/commit/f60ac1a5e9|
|[bugfix][cdc-base] Fix cdc base shutdown thread not cleared (#4327)|https://github.com/apache/seatunnel/commit/ac61409bd8|
|[Feature][CDC] Support export debezium-json format to kafka (#4339)|https://github.com/apache/seatunnel/commit/5817ec07bf|
|[Improve][CDC][MySQL] Ennable binlog watermark compare (#4293)|https://github.com/apache/seatunnel/commit/b22fb259c8|
|[Feature][CDC][Mysql] Support read database list (#4255)|https://github.com/apache/seatunnel/commit/3ca60c6fed|
|Add redshift datatype convertor (#4245)|https://github.com/apache/seatunnel/commit/b19011517f|
|[improve][zeta] fix zeta bugs|https://github.com/apache/seatunnel/commit/3a82e8b39f|
|[Improve] Support MySqlCatalog Use JDBC URL With Custom Suffix|https://github.com/apache/seatunnel/commit/210d0ff1f8|
|[chore] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/291214ad6f|
|Merge branch 'dev' into merge/cdc|https://github.com/apache/seatunnel/commit/4324ee1912|
|[Improve][Project] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/423b583038|
|[improve][jdbc] Reduce jdbc options configuration (#4218)|https://github.com/apache/seatunnel/commit/ddd8f808b5|
|[improve][cdc] support sharding-tables (#4207)|https://github.com/apache/seatunnel/commit/5c3f0c9b00|
|[Hotfix][CDC] Fix multiple-table data read (#4200)|https://github.com/apache/seatunnel/commit/7f5671d2ce|
|[Feature][Zeta] Support shuffle multiple rows by tableId (#4147)|https://github.com/apache/seatunnel/commit/8348f1a108|
|[Improve][build] Give the maven module a human readable name (#4114)|https://github.com/apache/seatunnel/commit/d7cd601051|
|[Feature][CDC] Support batch processing on multiple-table shuffle flow (#4116)|https://github.com/apache/seatunnel/commit/919653d83e|
|[Improve][Project] Code format with spotless plugin. (#4101)|https://github.com/apache/seatunnel/commit/a2ab166561|
|[Feature][CDC] MySQL CDC supports deserialization of multi-tables (#4067)|https://github.com/apache/seatunnel/commit/21ef45fcca|
|fix cdc option rule error (#4018)|https://github.com/apache/seatunnel/commit/ea160429df|
|[Improve][CDC][base] Guaranteed to be exactly-once in the process of switching from SnapshotTask to IncrementalTask (#3837)|https://github.com/apache/seatunnel/commit/8379aaf876|
|[Feature][Connector] add get source method to all source connector (#3846)|https://github.com/apache/seatunnel/commit/417178fb84|
|[Feature][API & Connector & Doc] add parallelism and column projection interface (#3829)|https://github.com/apache/seatunnel/commit/b9164b8ba1|
|[Improve][CDC] Add mysql-cdc source factory (#3791)|https://github.com/apache/seatunnel/commit/356538de8a|
|[feature][connector-v2] add sqlServer CDC (#3686)|https://github.com/apache/seatunnel/commit/0f0afb58af|
|[feature][e2e][cdc] add mysql cdc container (#3667)|https://github.com/apache/seatunnel/commit/7696ba1551|
|[feature][cdc] Fixed error in mysql cdc under real-time job (#3666)|https://github.com/apache/seatunnel/commit/2238fda300|
|[feature][connector][cdc] add SeaTunnelRowDebeziumDeserializeSchema (#3499)|https://github.com/apache/seatunnel/commit/ff44db116e|
|[feature][connector][mysql-cdc] add MySQL CDC enumerator (#3481)|https://github.com/apache/seatunnel/commit/ff4b32dc28|
|[bugfix][connector-v2] fix cdc mysql reader err (#3465)|https://github.com/apache/seatunnel/commit/1b406b5a31|
|[feature][connector] add mysql cdc reader (#3455)|https://github.com/apache/seatunnel/commit/ae981df675|
