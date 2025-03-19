<details><summary> Change Log </summary>

| Change | Commit | Version |
| --- | --- | --- |
|[Improve][CDC] Filter ddl for snapshot phase (#8911)|https://github.com/apache/seatunnel/commit/641cc72f2f| dev |
|[Improve][CDC] Extract duplicate code (#8906)|https://github.com/apache/seatunnel/commit/b922bb90e6| dev |
|[Improve] restruct connector common options (#8634)|https://github.com/apache/seatunnel/commit/f3499a6eeb| dev |
|[Fix][mysql-cdc] Fix GTIDs on startup to correctly recover from checkpoint (#8528)|https://github.com/apache/seatunnel/commit/82e4096c08| dev |
|[Feature][MySQL-CDC] Support database/table wildcards scan read (#8323)|https://github.com/apache/seatunnel/commit/2116843ce8|2.3.9|
|[Feature][Jdbc] Support sink ddl for postgresql (#8276)|https://github.com/apache/seatunnel/commit/353bbd21a1|2.3.9|
|[Feature][CDC] Add &#x27;schema-changes.enabled&#x27; options (#8285)|https://github.com/apache/seatunnel/commit/8e29ecf54f|2.3.9|
|Revert &quot;[Feature][Redis] Flush data when the time reaches checkpoint interval&quot; and &quot;[Feature][CDC] Add &#x27;schema-changes.enabled&#x27; options&quot; (#8278)|https://github.com/apache/seatunnel/commit/fcb2938286|2.3.9|
|[Feature][CDC] Add &#x27;schema-changes.enabled&#x27; options (#8252)|https://github.com/apache/seatunnel/commit/d783f9447c|2.3.9|
|[Improve][dist]add shade check rule (#8136)|https://github.com/apache/seatunnel/commit/51ef800016|2.3.9|
|[Feature][Connector-V2]Jdbc chunk split add  snapshotSplitColumn config #7794 (#7840)|https://github.com/apache/seatunnel/commit/b6c6dc0438|2.3.9|
|[Feature][Core] Support cdc task ddl restore for zeta (#7463)|https://github.com/apache/seatunnel/commit/8e322281ed|2.3.9|
|[Feature][Connector-v2] Support schema evolution for Oracle connector (#7908)|https://github.com/apache/seatunnel/commit/79406bcc2f|2.3.9|
|[Hotfix][CDC] Fix ddl duplicate execution error when config multi_table_sink_replica (#7634)|https://github.com/apache/seatunnel/commit/23ab3edbbb|2.3.8|
|[Hotfix][CDC] Fix package name spelling mistake (#7415)|https://github.com/apache/seatunnel/commit/469112fa64|2.3.8|
|[Hotfix][MySQL-CDC] Fix ArrayIndexOutOfBoundsException in mysql binlog read (#7381)|https://github.com/apache/seatunnel/commit/40c5f313eb|2.3.7|
|[Improve][Connector-V2] Support schema evolution for mysql-cdc and mysql-jdbc (#6929)|https://github.com/apache/seatunnel/commit/cf91e51fc7|2.3.6|
|[Hotfix][MySQL-CDC] Fix read gbk varchar chinese garbled characters (#7046)|https://github.com/apache/seatunnel/commit/4e4d2b8ee5|2.3.6|
|[Improve][CDC] Bump the version of debezium to 1.9.8.Final (#6740)|https://github.com/apache/seatunnel/commit/c3ac953524|2.3.6|
|[Improve][CDC] Close idle subtasks gorup(reader/writer) in increment phase (#6526)|https://github.com/apache/seatunnel/commit/454c339b9c|2.3.6|
|[Improve][JDBC Source] Fix Split can not be cancel (#6825)|https://github.com/apache/seatunnel/commit/ee3b7c3723|2.3.6|
|[Hotfix][Jdbc/CDC] Fix postgresql uuid type in jdbc read (#6684)|https://github.com/apache/seatunnel/commit/868ba4d7c7|2.3.6|
|[Improve][mysql-cdc] Support mysql 5.5 versions (#6710)|https://github.com/apache/seatunnel/commit/058f5594a3|2.3.6|
|[Improve][mysql-cdc] Fallback to desc table when show create table failed (#6701)|https://github.com/apache/seatunnel/commit/6f74663c08|2.3.6|
|[Improve][Jdbc] Add quote identifier for sql (#6669)|https://github.com/apache/seatunnel/commit/849d748d3d|2.3.5|
|[Fix][Connector-V2] Fix connector support SPI but without no args constructor (#6551)|https://github.com/apache/seatunnel/commit/5f3c9c36a5|2.3.5|
|[Improve][CDC-Connector]Fix CDC option rule. (#6454)|https://github.com/apache/seatunnel/commit/1ea27afa87|2.3.5|
|[Improve][CDC] Optimize memory allocation for snapshot split reading (#6281)|https://github.com/apache/seatunnel/commit/4856645837|2.3.5|
|[Improve][API] Unify type system api(data &amp; type) (#5872)|https://github.com/apache/seatunnel/commit/b38c7edcc9|2.3.5|
|[Feature][CDC] Support custom table primary key (#6106)|https://github.com/apache/seatunnel/commit/1312a1dd27|2.3.4|
|[Feature][CDC] Support read no primary key table (#6098)|https://github.com/apache/seatunnel/commit/b42d78de3f|2.3.4|
|[Bug][CDC] Fix state recovery error when switching a single table to multiple tables (#5784)|https://github.com/apache/seatunnel/commit/37fcff347e|2.3.4|
|[Feature][formats][ogg] Support read ogg format message #4201 (#4225)|https://github.com/apache/seatunnel/commit/7728e241e8|2.3.4|
|[Improve][CDC] Clean unused code (#5785)|https://github.com/apache/seatunnel/commit/b5a66d3dbe|2.3.4|
|[Improve][Jdbc] Fix database identifier (#5756)|https://github.com/apache/seatunnel/commit/dbfc8a670a|2.3.4|
|[improve][mysql-cdc] Optimize the default value range of mysql server-id to reduce conflicts. (#5550)|https://github.com/apache/seatunnel/commit/5174639463|2.3.4|
|[Improve] Remove catalog tag for config file (#5645)|https://github.com/apache/seatunnel/commit/dc509aa080|2.3.4|
|[Improve][Pom] Add junit4 to the root pom (#5611)|https://github.com/apache/seatunnel/commit/7b4f7db2a2|2.3.4|
|[Improve] Refactor CatalogTable and add `SeaTunnelSource::getProducedCatalogTables` (#5562)|https://github.com/apache/seatunnel/commit/41173357f8|2.3.4|
|[Improve][connector-cdc-mysql] avoid listing tables under unnecessary databases (#5365)|https://github.com/apache/seatunnel/commit/3e5d018b35|2.3.4|
|[Improve][Docs] Refactor MySQL-CDC docs (#5302)|https://github.com/apache/seatunnel/commit/74530a0461|2.3.4|
|[Improve][CheckStyle] Remove useless &#x27;SuppressWarnings&#x27; annotation of checkstyle. (#5260)|https://github.com/apache/seatunnel/commit/51c0d709ba|2.3.4|
|[Hotfix] Fix com.google.common.base.Preconditions to seatunnel shade one (#5284)|https://github.com/apache/seatunnel/commit/ed5eadcf73|2.3.3|
|[Imporve] [CDC Base] Add a fast sampling method that supports character types (#5179)|https://github.com/apache/seatunnel/commit/c0422dbfeb|2.3.3|
|[improve] [CDC Base] Add some split parameters to the optionRule (#5161)|https://github.com/apache/seatunnel/commit/94fd6755e6|2.3.3|
|[Improve][CDC] support exactly-once of cdc and fix the BinlogOffset comparing bug (#5057)|https://github.com/apache/seatunnel/commit/0e4190ab2e|2.3.3|
|[Feature][Connector-V2][CDC] Support string type shard fields. (#5147)|https://github.com/apache/seatunnel/commit/e1be9d7f8a|2.3.3|
|[Feature][CDC] Support tables without primary keys (with unique keys) (#163) (#5150)|https://github.com/apache/seatunnel/commit/32b7f2b690|2.3.3|
|[Feature][Connector-V2][mysql cdc] Conversion of tinyint(1) to bool is supported (#5105)|https://github.com/apache/seatunnel/commit/86b1b7e31a|2.3.3|
|[Feature][connector-v2][mongodbcdc]Support source mongodb cdc (#4923)|https://github.com/apache/seatunnel/commit/d729fcba4c|2.3.3|
|[Bugfix][connector-cdc-mysql] Fix listener not released when BinlogClient reuse (#5011)|https://github.com/apache/seatunnel/commit/3287b1d852|2.3.3|
|[BugFix] [Connector-V2] [MySQL-CDC] serverId from int to long (#5033) (#5035)|https://github.com/apache/seatunnel/commit/4abc80e111|2.3.3|
|[Hotfix][CDC] Fix jdbc connection leak for mysql (#5037)|https://github.com/apache/seatunnel/commit/738925ba10|2.3.3|
|[Feature][CDC] Support disable/enable exactly once for INITIAL (#4921)|https://github.com/apache/seatunnel/commit/6d9a3e5957|2.3.3|
|[Improve][CDC]change driver scope to provider (#5002)|https://github.com/apache/seatunnel/commit/745c0b9e92|2.3.3|
|[Improve][CDC]Remove  driver for cdc connector (#4952)|https://github.com/apache/seatunnel/commit/b65f40c3c9|2.3.3|
|[improve][CDC base] Implement Sample-based Sharding Strategy with Configurable Sampling Rate (#4856)|https://github.com/apache/seatunnel/commit/d827c700f0|2.3.2|
|[Hotfix][CDC] Fix chunk start/end parameter type error (#4777)|https://github.com/apache/seatunnel/commit/c13c031995|2.3.2|
|[feature][catalog] Support for multiplexing connections (#4550)|https://github.com/apache/seatunnel/commit/41277d7f78|2.3.2|
|[BugFix][Mysql-CDC] Fix Time data type is empty when reading from MySQL CDC (#4670)|https://github.com/apache/seatunnel/commit/e4f973daf7|2.3.2|
|[Improve][CDC] Optimize jdbc fetch-size options (#4352)|https://github.com/apache/seatunnel/commit/fbb60ce1be|2.3.1|
|[Improve][CDC] Improve startup.mode/stop.mode options (#4360)|https://github.com/apache/seatunnel/commit/b71d8739d5|2.3.1|
|Update CDC StartupMode and StopMode option to SingleChoiceOption (#4357)|https://github.com/apache/seatunnel/commit/f60ac1a5e9|2.3.1|
|[bugfix][cdc-base] Fix cdc base shutdown thread not cleared (#4327)|https://github.com/apache/seatunnel/commit/ac61409bd8|2.3.1|
|[Feature][CDC] Support export debezium-json format to kafka (#4339)|https://github.com/apache/seatunnel/commit/5817ec07bf|2.3.1|
|[Improve][CDC][MySQL] Ennable binlog watermark compare (#4293)|https://github.com/apache/seatunnel/commit/b22fb259c8|2.3.1|
|[Feature][CDC][Mysql] Support read database list (#4255)|https://github.com/apache/seatunnel/commit/3ca60c6fed|2.3.1|
|Add redshift datatype convertor (#4245)|https://github.com/apache/seatunnel/commit/b19011517f|2.3.1|
|[improve][zeta] fix zeta bugs|https://github.com/apache/seatunnel/commit/3a82e8b39f|2.3.1|
|[Improve] Support MySqlCatalog Use JDBC URL With Custom Suffix|https://github.com/apache/seatunnel/commit/210d0ff1f8|2.3.1|
|[chore] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/291214ad6f|2.3.1|
|Merge branch &#x27;dev&#x27; into merge/cdc|https://github.com/apache/seatunnel/commit/4324ee1912|2.3.1|
|[Improve][Project] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/423b583038|2.3.1|
|[improve][jdbc] Reduce jdbc options configuration (#4218)|https://github.com/apache/seatunnel/commit/ddd8f808b5|2.3.1|
|[improve][cdc] support sharding-tables (#4207)|https://github.com/apache/seatunnel/commit/5c3f0c9b00|2.3.1|
|[Hotfix][CDC] Fix multiple-table data read (#4200)|https://github.com/apache/seatunnel/commit/7f5671d2ce|2.3.1|
|[Feature][Zeta] Support shuffle multiple rows by tableId (#4147)|https://github.com/apache/seatunnel/commit/8348f1a108|2.3.1|
|[Improve][build] Give the maven module a human readable name (#4114)|https://github.com/apache/seatunnel/commit/d7cd601051|2.3.1|
|[Feature][CDC] Support batch processing on multiple-table shuffle flow (#4116)|https://github.com/apache/seatunnel/commit/919653d83e|2.3.1|
|[Improve][Project] Code format with spotless plugin. (#4101)|https://github.com/apache/seatunnel/commit/a2ab166561|2.3.1|
|[Feature][CDC] MySQL CDC supports deserialization of multi-tables (#4067)|https://github.com/apache/seatunnel/commit/21ef45fcca|2.3.1|
|fix cdc option rule error (#4018)|https://github.com/apache/seatunnel/commit/ea160429df|2.3.1|
|[Improve][CDC][base] Guaranteed to be exactly-once in the process of switching from SnapshotTask to IncrementalTask (#3837)|https://github.com/apache/seatunnel/commit/8379aaf876|2.3.1|
|[Feature][Connector] add get source method to all source connector (#3846)|https://github.com/apache/seatunnel/commit/417178fb84|2.3.1|
|[Feature][API &amp; Connector &amp; Doc] add parallelism and column projection interface (#3829)|https://github.com/apache/seatunnel/commit/b9164b8ba1|2.3.1|
|[Improve][CDC] Add mysql-cdc source factory (#3791)|https://github.com/apache/seatunnel/commit/356538de8a|2.3.1|
|[feature][connector-v2] add sqlServer CDC (#3686)|https://github.com/apache/seatunnel/commit/0f0afb58af|2.3.0|
|[feature][e2e][cdc] add mysql cdc container (#3667)|https://github.com/apache/seatunnel/commit/7696ba1551|2.3.0|
|[feature][cdc] Fixed error in mysql cdc under real-time job (#3666)|https://github.com/apache/seatunnel/commit/2238fda300|2.3.0|
|[feature][connector][cdc] add SeaTunnelRowDebeziumDeserializeSchema (#3499)|https://github.com/apache/seatunnel/commit/ff44db116e|2.3.0|
|[feature][connector][mysql-cdc] add MySQL CDC enumerator (#3481)|https://github.com/apache/seatunnel/commit/ff4b32dc28|2.3.0|
|[bugfix][connector-v2] fix cdc mysql reader err (#3465)|https://github.com/apache/seatunnel/commit/1b406b5a31|2.3.0|
|[feature][connector] add mysql cdc reader (#3455)|https://github.com/apache/seatunnel/commit/ae981df675|2.3.0|

</details>
