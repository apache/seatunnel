<details><summary> Change Log </summary>

| Change | Commit | Version |
| --- | --- | --- |
|[Improve][CDC] Filter ddl for snapshot phase (#8911)|https://github.com/apache/seatunnel/commit/641cc72f2f| dev |
|[Improve][Oracle-CDC] Support ReadOnlyLogWriterFlushStrategy (#8912)|https://github.com/apache/seatunnel/commit/6aebdc0384| dev |
|[Improve][CDC] Extract duplicate code (#8906)|https://github.com/apache/seatunnel/commit/b922bb90e6| dev |
|[Improve][CDC] Filter heartbeat event (#8569)|https://github.com/apache/seatunnel/commit/1870653393| dev |
|[Improve] restruct connector common options (#8634)|https://github.com/apache/seatunnel/commit/f3499a6eeb| dev |
|[Fix][MySQL-CDC]fix recovery task failure caused by binlog deletion (#8587)|https://github.com/apache/seatunnel/commit/087087e592| dev |
|[Fix][mysql-cdc] Fix GTIDs on startup to correctly recover from checkpoint (#8528)|https://github.com/apache/seatunnel/commit/82e4096c08| dev |
|[Feature] [Postgre CDC]support array type (#8560)|https://github.com/apache/seatunnel/commit/021af147cc| dev |
|[Feature][MySQL-CDC] Support database/table wildcards scan read (#8323)|https://github.com/apache/seatunnel/commit/2116843ce8|2.3.9|
|[hotfix] [connector-cdc-oracle ] support read partition table (#8265)|https://github.com/apache/seatunnel/commit/91b86b2faf|2.3.9|
|[Feature][Jdbc] Support sink ddl for postgresql (#8276)|https://github.com/apache/seatunnel/commit/353bbd21a1|2.3.9|
|[Improve][E2E] improve oracle e2e (#8292)|https://github.com/apache/seatunnel/commit/9f761b9d32|2.3.9|
|[Feature][CDC] Add &#x27;schema-changes.enabled&#x27; options (#8285)|https://github.com/apache/seatunnel/commit/8e29ecf54f|2.3.9|
|Revert &quot;[Feature][Redis] Flush data when the time reaches checkpoint interval&quot; and &quot;[Feature][CDC] Add &#x27;schema-changes.enabled&#x27; options&quot; (#8278)|https://github.com/apache/seatunnel/commit/fcb2938286|2.3.9|
|[Feature][CDC] Add &#x27;schema-changes.enabled&#x27; options (#8252)|https://github.com/apache/seatunnel/commit/d783f9447c|2.3.9|
|[Feature][Mongodb-CDC] Support multi-table read (#8029)|https://github.com/apache/seatunnel/commit/49cbaeb9b3|2.3.9|
|[Improve][dist]add shade check rule (#8136)|https://github.com/apache/seatunnel/commit/51ef800016|2.3.9|
|[Improve][Connector-V2] Add pre-check for table enable cdc (#8152)|https://github.com/apache/seatunnel/commit/9a5da78176|2.3.9|
|[Improve][Connector-V2] Fix SqlServer cdc memory leak (#8083)|https://github.com/apache/seatunnel/commit/69cd4ae1a2|2.3.9|
|[Feature][Connector-V2]Jdbc chunk split add  snapshotSplitColumn config #7794 (#7840)|https://github.com/apache/seatunnel/commit/b6c6dc0438|2.3.9|
|[Bug][connectors-v2] fix mongodb bson convert exception (#8044)|https://github.com/apache/seatunnel/commit/b222c13f2f|2.3.9|
|[Feature][Core] Support cdc task ddl restore for zeta (#7463)|https://github.com/apache/seatunnel/commit/8e322281ed|2.3.9|
|[Feature][Transform-v2] Add metadata transform (#7899)|https://github.com/apache/seatunnel/commit/699d16552a|2.3.9|
|[Feature][Connector-v2] Support schema evolution for Oracle connector (#7908)|https://github.com/apache/seatunnel/commit/79406bcc2f|2.3.9|
|[Bug][Connector-v2] MongoDB CDC Set SeatunnelRow&#x27;s tableId (#7935)|https://github.com/apache/seatunnel/commit/f3970d6188|2.3.9|
|[Fix][Connector-V2] Fix cdc use default value when value is null (#7950)|https://github.com/apache/seatunnel/commit/3b432125ae|2.3.9|
|[Hotfix][CDC] Fix occasional database connection leak when read snapshot split (#7918)|https://github.com/apache/seatunnel/commit/a8d0d4ce77|2.3.9|
|[Improve][PostgreSQL CDC]-PostgresSourceOptions description error (#7813)|https://github.com/apache/seatunnel/commit/57f47c2064|2.3.9|
|[Feature][Connector-V2] SqlServer support user-defined type (#7706)|https://github.com/apache/seatunnel/commit/fb89033273|2.3.8|
|[Improve][Connector-V2] Optimize sqlserver package structure (#7715)|https://github.com/apache/seatunnel/commit/9720f118e5|2.3.8|
|[Hotfix][CDC] Fix ddl duplicate execution error when config multi_table_sink_replica (#7634)|https://github.com/apache/seatunnel/commit/23ab3edbbb|2.3.8|
|[Fix][Connector-V2] Fix some throwable error not be caught (#7657)|https://github.com/apache/seatunnel/commit/e19d73282e|2.3.8|
|[Feature] Support tidb cdc connector source #7199 (#7477)|https://github.com/apache/seatunnel/commit/87ec786bd6|2.3.8|
|[Feature][Connector-V2] Support opengauss-cdc (#7433)|https://github.com/apache/seatunnel/commit/81b73515a7|2.3.8|
|[Improve][Connector-V2] Close all ResultSet after used (#7389)|https://github.com/apache/seatunnel/commit/853e973212|2.3.8|
|[Hotfix][CDC] Fix package name spelling mistake (#7415)|https://github.com/apache/seatunnel/commit/469112fa64|2.3.8|
|[Hotfix][MySQL-CDC] Fix ArrayIndexOutOfBoundsException in mysql binlog read (#7381)|https://github.com/apache/seatunnel/commit/40c5f313eb|2.3.7|
|[Improve][Connector-v2] Optimize the count table rows for jdbc-oracle and oracle-cdc (#7248)|https://github.com/apache/seatunnel/commit/0d08b20061|2.3.6|
|[Feature][Connector-V2] Support jdbc hana catalog and type convertor (#6950)|https://github.com/apache/seatunnel/commit/d663398739|2.3.6|
|[Fix][Connector-V2][CDC] SeaTunnelRowDebeziumDeserializationConverters NPE (#7119)|https://github.com/apache/seatunnel/commit/ae81879213|2.3.6|
|[Improve][Connector-V2] Support schema evolution for mysql-cdc and mysql-jdbc (#6929)|https://github.com/apache/seatunnel/commit/cf91e51fc7|2.3.6|
|[Hotfix][MySQL-CDC] Fix read gbk varchar chinese garbled characters (#7046)|https://github.com/apache/seatunnel/commit/4e4d2b8ee5|2.3.6|
|[Hotfix][CDC] Fix split schema change stream (#7003)|https://github.com/apache/seatunnel/commit/0c3044e3f6|2.3.6|
|[Improve][CDC] Bump the version of debezium to 1.9.8.Final (#6740)|https://github.com/apache/seatunnel/commit/c3ac953524|2.3.6|
|[Improve][CDC] Close idle subtasks gorup(reader/writer) in increment phase (#6526)|https://github.com/apache/seatunnel/commit/454c339b9c|2.3.6|
|[Improve][JDBC Source] Fix Split can not be cancel (#6825)|https://github.com/apache/seatunnel/commit/ee3b7c3723|2.3.6|
|[Hotfix][Postgres-CDC/OpenGauss-CDC] Fix read data missing when restore (#6785)|https://github.com/apache/seatunnel/commit/67c32607e7|2.3.6|
|[Improve] Add conditional of start.mode with timestamp in mongo cdc option rule (#6770)|https://github.com/apache/seatunnel/commit/65ae7782c9|2.3.6|
|[Fix] Fix ConnectorSpecificationCheckTest failed (#6828)|https://github.com/apache/seatunnel/commit/52d1020eb7|2.3.6|
|[Hotfix][Jdbc/CDC] Fix postgresql uuid type in jdbc read (#6684)|https://github.com/apache/seatunnel/commit/868ba4d7c7|2.3.6|
|[Chore] remove useless interface (#6746)|https://github.com/apache/seatunnel/commit/3c1aeb3785|2.3.6|
|[Improve][mysql-cdc] Support mysql 5.5 versions (#6710)|https://github.com/apache/seatunnel/commit/058f5594a3|2.3.6|
|[Improve] Improve read table schema in cdc connector (#6702)|https://github.com/apache/seatunnel/commit/a8c6cc6e0c|2.3.6|
|[Improve][mysql-cdc] Fallback to desc table when show create table failed (#6701)|https://github.com/apache/seatunnel/commit/6f74663c08|2.3.6|
|[Improve][Jdbc] Add quote identifier for sql (#6669)|https://github.com/apache/seatunnel/commit/849d748d3d|2.3.5|
|[Feature] Support listening for message delayed events in cdc source (#6634)|https://github.com/apache/seatunnel/commit/01159ec923|2.3.5|
|[Improve][CDC] Optimize split state memory allocation in increment phase (#6554)|https://github.com/apache/seatunnel/commit/fe33422161|2.3.5|
|[Improve][CDC] Improve read performance when record not contains schema field (#6571)|https://github.com/apache/seatunnel/commit/e60beb28ec|2.3.5|
|[Feature][Core] Support event listener for job (#6419)|https://github.com/apache/seatunnel/commit/831d0022eb|2.3.5|
|[Fix][Connector-V2] Fix connector support SPI but without no args constructor (#6551)|https://github.com/apache/seatunnel/commit/5f3c9c36a5|2.3.5|
|[Improve][CDC-Connector]Fix CDC option rule. (#6454)|https://github.com/apache/seatunnel/commit/1ea27afa87|2.3.5|
|[Improve][CDC] Optimize memory allocation for snapshot split reading (#6281)|https://github.com/apache/seatunnel/commit/4856645837|2.3.5|
|[Fix][Connector-V2] Fix mongodb cdc start up mode option values not right (#6338)|https://github.com/apache/seatunnel/commit/c07f56fbc4|2.3.5|
|[Improve][API] Unify type system api(data &amp; type) (#5872)|https://github.com/apache/seatunnel/commit/b38c7edcc9|2.3.5|
|[Feature] Supports iceberg sink #6198 (#6265)|https://github.com/apache/seatunnel/commit/18d3e86194|2.3.5|
|[Feature][Connector]update pgsql-cdc publication for add table (#6309)|https://github.com/apache/seatunnel/commit/2ad7d65236|2.3.5|
|[Fix][Oracle-CDC] Fix invalid split key when no primary key (#6251)|https://github.com/apache/seatunnel/commit/b83c40a6f6|2.3.4|
|[Bugfix][cdc base] Fix negative values in CDCRecordEmitDelay metric (#6259)|https://github.com/apache/seatunnel/commit/68978dbb4e|2.3.4|
|[Improve][Postgres-CDC] Fix name typos (#6248)|https://github.com/apache/seatunnel/commit/2462f1c5f7|2.3.4|
|[BugFix][CDC Base] Fix added columns cannot be parsed after job restore (#6118)|https://github.com/apache/seatunnel/commit/0c593a39e3|2.3.4|
|[Feature][JDBC、CDC] Support Short and Byte Type in spliter (#6027)|https://github.com/apache/seatunnel/commit/6f8d0a5040|2.3.4|
|[Improve][CDC] Disable exactly_once by default to improve stability (#6244)|https://github.com/apache/seatunnel/commit/f47495554b|2.3.4|
|[Improve][Postgres-CDC] Update jdbc fetchsize (#6245)|https://github.com/apache/seatunnel/commit/c25beb9f8a|2.3.4|
|[Improve] Support `int identity` type in sql server (#6186)|https://github.com/apache/seatunnel/commit/1a8da1c843|2.3.4|
|[Bugfix][JDBC、CDC] Fix Spliter Error in Case of Extensive Duplicate Data (#6026)|https://github.com/apache/seatunnel/commit/635c24e8b2|2.3.4|
| [Feature][Connector-V2][Postgres-cdc]Support for Postgres cdc (#5986)|https://github.com/apache/seatunnel/commit/97438b9402|2.3.4|
|[Feature][Oracle-CDC] Support custom table primary key (#6216)|https://github.com/apache/seatunnel/commit/ae4240ca6b|2.3.4|
|[Improve][Oracle-CDC] Clean unused code (#6212)|https://github.com/apache/seatunnel/commit/919a91032a|2.3.4|
|[Hotfix][Oracle-CDC] Fix state recovery error when switching a single table to multiple tables (#6211)|https://github.com/apache/seatunnel/commit/74cfe1995f|2.3.4|
|[Hotfix][Oracle-CDC] Fix jdbc setFetchSize error (#6210)|https://github.com/apache/seatunnel/commit/b7f06ec6d9|2.3.4|
|[Feature][Oracle-CDC] Support read no primary key table (#6209)|https://github.com/apache/seatunnel/commit/3cb34c2b71|2.3.4|
|[Feature][Connector-V2][Oracle-cdc]Support for oracle cdc (#5196)|https://github.com/apache/seatunnel/commit/aaef22b31b|2.3.4|
|[Bugfix][CDC Base] Fix NPE caused by adding a table for restore job (#6145)|https://github.com/apache/seatunnel/commit/8d3f8e4627|2.3.4|
|[Feature][CDC] Support custom table primary key (#6106)|https://github.com/apache/seatunnel/commit/1312a1dd27|2.3.4|
|[Bugfix][CDC base] Fix CDC job cannot consume incremental data After restore run (#625) (#6094)|https://github.com/apache/seatunnel/commit/37567ebb7e|2.3.4|
|[Feature][CDC] Support read no primary key table (#6098)|https://github.com/apache/seatunnel/commit/b42d78de3f|2.3.4|
|[Hotfix][Jdbc] Fix jdbc setFetchSize error (#6005)|https://github.com/apache/seatunnel/commit/d41af8a6ed|2.3.4|
|[Improve][CDC] Disable memory buffering when `exactly_once` is turned off (#6017)|https://github.com/apache/seatunnel/commit/300a624c5b|2.3.4|
|[Improve][Zeta] Remove assert key words (#5947)|https://github.com/apache/seatunnel/commit/dcb4549109|2.3.4|
|[Improve][Common] Introduce new error define rule (#5793)|https://github.com/apache/seatunnel/commit/9d1b2582b2|2.3.4|
|[Bug][CDC] Fix state recovery error when switching a single table to multiple tables (#5784)|https://github.com/apache/seatunnel/commit/37fcff347e|2.3.4|
|[Feature][formats][ogg] Support read ogg format message #4201 (#4225)|https://github.com/apache/seatunnel/commit/7728e241e8|2.3.4|
|[Improve][CDC] Clean unused code (#5785)|https://github.com/apache/seatunnel/commit/b5a66d3dbe|2.3.4|
|[Fix] Fix MultiTableSink restore failed when add new table (#5746)|https://github.com/apache/seatunnel/commit/21503bd771|2.3.4|
|[Improve][Jdbc] Fix database identifier (#5756)|https://github.com/apache/seatunnel/commit/dbfc8a670a|2.3.4|
|[improve][mysql-cdc] Optimize the default value range of mysql server-id to reduce conflicts. (#5550)|https://github.com/apache/seatunnel/commit/5174639463|2.3.4|
|[improve][connector-v2][sqlserver-cdc]Unified sqlserver TypeUtils type conversion mode (#5668)|https://github.com/apache/seatunnel/commit/75b814bc3d|2.3.4|
|[Dependency]Bump org.apache.avro:avro (#5583)|https://github.com/apache/seatunnel/commit/bb791a6d9e|2.3.4|
|[Improve] Add default implement for `SeaTunnelSource::getProducedType` (#5670)|https://github.com/apache/seatunnel/commit/a04add6991|2.3.4|
|[feature][connector-cdc-sqlserver] add dataType datetimeoffset (#5548)|https://github.com/apache/seatunnel/commit/0cf63eed6d|2.3.4|
|[Improve] Remove catalog tag for config file (#5645)|https://github.com/apache/seatunnel/commit/dc509aa080|2.3.4|
|[Improve][Pom] Add junit4 to the root pom (#5611)|https://github.com/apache/seatunnel/commit/7b4f7db2a2|2.3.4|
|[Hotfix][CDC] Fix thread-unsafe collection container in cdc enumerator (#5614)|https://github.com/apache/seatunnel/commit/b2f70fd40b|2.3.4|
|[Feature][CDC] Support MongoDB CDC running on flink (#5644)|https://github.com/apache/seatunnel/commit/8c569b1541|2.3.4|
|[Improve][CDC] Use Source to output the CatalogTable (#5626)|https://github.com/apache/seatunnel/commit/3e6a20acfa|2.3.4|
|Support config column/primaryKey/constraintKey in schema (#5564)|https://github.com/apache/seatunnel/commit/eac76b4e50|2.3.4|
|[Fix]: fix the cdc bug about NPE when the original table deletes a field (#5579)|https://github.com/apache/seatunnel/commit/f5ed47795d|2.3.4|
|[Improve] Refactor CatalogTable and add `SeaTunnelSource::getProducedCatalogTables` (#5562)|https://github.com/apache/seatunnel/commit/41173357f8|2.3.4|
|[Feature][CDC] Support for preferring numeric fields as split keys (#5384)|https://github.com/apache/seatunnel/commit/c687050d88|2.3.4|
|[Feature][Connector-V2][CDC] Support flink running cdc job (#4918)|https://github.com/apache/seatunnel/commit/5e378831ee|2.3.4|
|[Improve][connector-cdc-mysql] avoid listing tables under unnecessary databases (#5365)|https://github.com/apache/seatunnel/commit/3e5d018b35|2.3.4|
|[Improve][Docs] Refactor MySQL-CDC docs (#5302)|https://github.com/apache/seatunnel/commit/74530a0461|2.3.4|
|[Improve][CheckStyle] Remove useless &#x27;SuppressWarnings&#x27; annotation of checkstyle. (#5260)|https://github.com/apache/seatunnel/commit/51c0d709ba|2.3.4|
|[Hotfix] Fix com.google.common.base.Preconditions to seatunnel shade one (#5284)|https://github.com/apache/seatunnel/commit/ed5eadcf73|2.3.3|
|[BUG][Connector-V2][Mongo-cdc] Incremental data kind error in snapshot phase (#5184)|https://github.com/apache/seatunnel/commit/ead1c5fd8c|2.3.3|
|[Imporve] [CDC Base] Add a fast sampling method that supports character types (#5179)|https://github.com/apache/seatunnel/commit/c0422dbfeb|2.3.3|
|[Bugfix][cdc] Fix mysql bit column to java byte (#4817)|https://github.com/apache/seatunnel/commit/aae3e913d0|2.3.3|
|[Hotfix]Fix array index anomalies caused by #5057 (#5195)|https://github.com/apache/seatunnel/commit/1c33429506|2.3.3|
|[Feature][CDC][Zeta] Support schema evolution framework(DDL) (#5125)|https://github.com/apache/seatunnel/commit/4f89c1d272|2.3.3|
|[improve] [CDC Base] Add some split parameters to the optionRule (#5161)|https://github.com/apache/seatunnel/commit/94fd6755e6|2.3.3|
|[Improve][CDC] support exactly-once of cdc and fix the BinlogOffset comparing bug (#5057)|https://github.com/apache/seatunnel/commit/0e4190ab2e|2.3.3|
|[Hotfix][MongodbCDC]Refine data format to adapt to universal logic (#5162)|https://github.com/apache/seatunnel/commit/4b4b5f9640|2.3.3|
|[Feature][Connector-V2][CDC] Support string type shard fields. (#5147)|https://github.com/apache/seatunnel/commit/e1be9d7f8a|2.3.3|
|[Feature][CDC] Support tables without primary keys (with unique keys) (#163) (#5150)|https://github.com/apache/seatunnel/commit/32b7f2b690|2.3.3|
|[Hotfix][Mongodb cdc] Solve startup resume token is negative (#5143)|https://github.com/apache/seatunnel/commit/e964c03dca|2.3.3|
|[Hotfix]Fix mongodb cdc e2e instability (#5128)|https://github.com/apache/seatunnel/commit/6f30b29662|2.3.3|
|[Feature][Connector-V2][mysql cdc] Conversion of tinyint(1) to bool is supported (#5105)|https://github.com/apache/seatunnel/commit/86b1b7e31a|2.3.3|
|[Feature][connector-v2][mongodbcdc]Support source mongodb cdc (#4923)|https://github.com/apache/seatunnel/commit/d729fcba4c|2.3.3|
|[Chore] Modify repeat des (#5088)|https://github.com/apache/seatunnel/commit/936afc2a9e|2.3.3|
|[Bugfix][connector-cdc-mysql] Fix listener not released when BinlogClient reuse (#5011)|https://github.com/apache/seatunnel/commit/3287b1d852|2.3.3|
|[Feature][Connector-V2][cdc] Change the time zone to the default time zone (#5030)|https://github.com/apache/seatunnel/commit/3cff923a79|2.3.3|
|[BugFix] [Connector-V2] [MySQL-CDC] serverId from int to long (#5033) (#5035)|https://github.com/apache/seatunnel/commit/4abc80e111|2.3.3|
|[Bugfix][zeta] Fix cdc connection does not close (#4922)|https://github.com/apache/seatunnel/commit/a2d2f2dda8|2.3.3|
|[Hotfix][CDC] Fix jdbc connection leak for mysql (#5037)|https://github.com/apache/seatunnel/commit/738925ba10|2.3.3|
|[Feature][CDC] Support disable/enable exactly once for INITIAL (#4921)|https://github.com/apache/seatunnel/commit/6d9a3e5957|2.3.3|
|[Improve][CDC]change driver scope to provider (#5002)|https://github.com/apache/seatunnel/commit/745c0b9e92|2.3.3|
|[Improve][CDC]Remove  driver for cdc connector (#4952)|https://github.com/apache/seatunnel/commit/b65f40c3c9|2.3.3|
|[Improve] Documentation and partial word optimization. (#4936)|https://github.com/apache/seatunnel/commit/6e8de0e2a6|2.3.3|
|[Bugfix][zeta] Fix the deadlock issue with JDBC driver loading (#4878)|https://github.com/apache/seatunnel/commit/c30a2a1b1c|2.3.2|
|[improve][CDC base] Implement Sample-based Sharding Strategy with Configurable Sampling Rate (#4856)|https://github.com/apache/seatunnel/commit/d827c700f0|2.3.2|
|[Bugfix][CDC Base] Solving the ConcurrentModificationException caused by snapshotState being modified concurrently. (#4877)|https://github.com/apache/seatunnel/commit/9a2efa51c7|2.3.2|
|[Hotfix][CDC] Fix chunk start/end parameter type error (#4777)|https://github.com/apache/seatunnel/commit/c13c031995|2.3.2|
|[feature][catalog] Support for multiplexing connections (#4550)|https://github.com/apache/seatunnel/commit/41277d7f78|2.3.2|
|[BugFix][Mysql-CDC] Fix Time data type is empty when reading from MySQL CDC (#4670)|https://github.com/apache/seatunnel/commit/e4f973daf7|2.3.2|
|[Bug][CDC] Fix TemporalConversions (#4542)|https://github.com/apache/seatunnel/commit/d2094bf2e1|2.3.2|
|[Feature][CDC][SqlServer] Support multi-table read (#4377)|https://github.com/apache/seatunnel/commit/c4e3f2dc03|2.3.2|
|[Improve][CDC] Optimize jdbc fetch-size options (#4352)|https://github.com/apache/seatunnel/commit/fbb60ce1be|2.3.1|
|[Improve][CDC] Improve startup.mode/stop.mode options (#4360)|https://github.com/apache/seatunnel/commit/b71d8739d5|2.3.1|
|[Improve][CDC] Optimize options &amp; add docs for compatible_debezium_json (#4351)|https://github.com/apache/seatunnel/commit/336f590498|2.3.1|
|Update CDC StartupMode and StopMode option to SingleChoiceOption (#4357)|https://github.com/apache/seatunnel/commit/f60ac1a5e9|2.3.1|
|[bugfix][cdc-base] Fix cdc base shutdown thread not cleared (#4327)|https://github.com/apache/seatunnel/commit/ac61409bd8|2.3.1|
|[Feature][CDC] Support export debezium-json format to kafka (#4339)|https://github.com/apache/seatunnel/commit/5817ec07bf|2.3.1|
|[Feature][CDC] Support add &amp; dorp tables when restore cdc jobs (#4254)|https://github.com/apache/seatunnel/commit/add75d7d5d|2.3.1|
|[Improve][CDC][MySQL] Ennable binlog watermark compare (#4293)|https://github.com/apache/seatunnel/commit/b22fb259c8|2.3.1|
|[Feature][CDC][Mysql] Support read database list (#4255)|https://github.com/apache/seatunnel/commit/3ca60c6fed|2.3.1|
|Add redshift datatype convertor (#4245)|https://github.com/apache/seatunnel/commit/b19011517f|2.3.1|
|[improve][zeta] fix zeta bugs|https://github.com/apache/seatunnel/commit/3a82e8b39f|2.3.1|
|[Improve] Support MySqlCatalog Use JDBC URL With Custom Suffix|https://github.com/apache/seatunnel/commit/210d0ff1f8|2.3.1|
|[chore] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/291214ad6f|2.3.1|
|Merge branch &#x27;dev&#x27; into merge/cdc|https://github.com/apache/seatunnel/commit/4324ee1912|2.3.1|
|[Improve][Project] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/423b583038|2.3.1|
|[Hotfix][Zeta] Fix shuffle checkpoint (#4224)|https://github.com/apache/seatunnel/commit/507ca85611|2.3.1|
|[improve][jdbc] Reduce jdbc options configuration (#4218)|https://github.com/apache/seatunnel/commit/ddd8f808b5|2.3.1|
|[improve][cdc] support sharding-tables (#4207)|https://github.com/apache/seatunnel/commit/5c3f0c9b00|2.3.1|
|[Hotfix][CDC] Fix multiple-table data read (#4200)|https://github.com/apache/seatunnel/commit/7f5671d2ce|2.3.1|
|[hotfix][zeta] fix zeta multi-table parser error (#4193)|https://github.com/apache/seatunnel/commit/98f2ad0c19|2.3.1|
|[Feature][Zeta] Support shuffle multiple rows by tableId (#4147)|https://github.com/apache/seatunnel/commit/8348f1a108|2.3.1|
|[Feature][API] Add Metrics for Connector-V2 (#4017)|https://github.com/apache/seatunnel/commit/32e1f91c7a|2.3.1|
|[Improve][build] Give the maven module a human readable name (#4114)|https://github.com/apache/seatunnel/commit/d7cd601051|2.3.1|
|[Feature][CDC] Support batch processing on multiple-table shuffle flow (#4116)|https://github.com/apache/seatunnel/commit/919653d83e|2.3.1|
|[Improve][Project] Code format with spotless plugin. (#4101)|https://github.com/apache/seatunnel/commit/a2ab166561|2.3.1|
|[Feature][CDC] MySQL CDC supports deserialization of multi-tables (#4067)|https://github.com/apache/seatunnel/commit/21ef45fcca|2.3.1|
|[Improve][Connector-V2][SQLServer-CDC] Add sqlserver cdc optionRule (#4019)|https://github.com/apache/seatunnel/commit/78df503392|2.3.1|
|fix cdc option rule error (#4018)|https://github.com/apache/seatunnel/commit/ea160429df|2.3.1|
|[Bug][CDC] Fix concurrent modify of splits (#3937)|https://github.com/apache/seatunnel/commit/29b04e2405|2.3.1|
|[Improve][CDC][base] Guaranteed to be exactly-once in the process of switching from SnapshotTask to IncrementalTask (#3837)|https://github.com/apache/seatunnel/commit/8379aaf876|2.3.1|
|[Feature][Connector] add get source method to all source connector (#3846)|https://github.com/apache/seatunnel/commit/417178fb84|2.3.1|
|[Feature][API &amp; Connector &amp; Doc] add parallelism and column projection interface (#3829)|https://github.com/apache/seatunnel/commit/b9164b8ba1|2.3.1|
|[Hotfix][SqlServer CDC] fix SqlServerCDC IT failure (#3807)|https://github.com/apache/seatunnel/commit/fd66de5f98|2.3.1|
|[Improve][CDC] Add mysql-cdc source factory (#3791)|https://github.com/apache/seatunnel/commit/356538de8a|2.3.1|
|[feature][connector-v2] add sqlServer CDC (#3686)|https://github.com/apache/seatunnel/commit/0f0afb58af|2.3.0|
|[doc][connector][cdc] add MySQL CDC Source doc (#3707)|https://github.com/apache/seatunnel/commit/555905b0b8|2.3.0|
|[feature][e2e][cdc] add mysql cdc container (#3667)|https://github.com/apache/seatunnel/commit/7696ba1551|2.3.0|
|[feature][cdc] Fixed error in mysql cdc under real-time job (#3666)|https://github.com/apache/seatunnel/commit/2238fda300|2.3.0|
|[feature][connector][cdc] add SeaTunnelRowDebeziumDeserializeSchema (#3499)|https://github.com/apache/seatunnel/commit/ff44db116e|2.3.0|
|[feature][connector][mysql-cdc] add MySQL CDC enumerator (#3481)|https://github.com/apache/seatunnel/commit/ff4b32dc28|2.3.0|
|[bugfix][connector-v2] fix cdc mysql reader err (#3465)|https://github.com/apache/seatunnel/commit/1b406b5a31|2.3.0|
|[feature][connector] add mysql cdc reader (#3455)|https://github.com/apache/seatunnel/commit/ae981df675|2.3.0|
|[feature][connector][cdc] add cdc reader jdbc related (#3433)|https://github.com/apache/seatunnel/commit/7bf00fb19f|2.3.0|
|[feature][connector][cdc] add CDC enumerator base classes (#3419)|https://github.com/apache/seatunnel/commit/9b1821f476|2.3.0|
|[feature][Connector-v2][cdc] Add cdc base reader (#3407)|https://github.com/apache/seatunnel/commit/e454b80dcd|2.3.0|
|[bigfix][Connector-v2][cdc] move version to 1.6.4 (#3389)|https://github.com/apache/seatunnel/commit/b50b543c3e|2.3.0|
|[feature][connector][cdc] CDC base classes (#3363)|https://github.com/apache/seatunnel/commit/2586f305b4|2.3.0|

</details>
