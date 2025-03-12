<details><summary> Change Log </summary>

| Change | Commit | Version |
| --- | --- | --- |
|[Improve][connector-file-base] Improved multiple table file source allocation algorithm for subtasks (#8878)|https://github.com/apache/seatunnel/commit/44a12cc55c| dev |
|[Fix][Connector-V2] Fixed incorrectly setting s3 key in some cases (#8885)|https://github.com/apache/seatunnel/commit/cf4bab5be2| dev |
|[Fix][Connector-File] Fix conflicting `file_format_type` requirement (#8823)|https://github.com/apache/seatunnel/commit/6e0d630f7c| dev |
|[Feature][Connector-V2] Add `filename_extension` parameter for read/write file (#8769)|https://github.com/apache/seatunnel/commit/78b23c0ef5| dev |
|[Improve][Connector-V2] Improve orc read error message (#8751)|https://github.com/apache/seatunnel/commit/d66d9dc9ce| dev |
|[Improve] restruct connector common options (#8634)|https://github.com/apache/seatunnel/commit/f3499a6eeb| dev |
| [improve] update S3File connector config option  (#8615)|https://github.com/apache/seatunnel/commit/80cc9fa6ff| dev |
|[Fix][Connector-V2] User selects csv string pattern (#8572)|https://github.com/apache/seatunnel/commit/227a11f5aa| dev |
|[Fix][Connector-V2] Fix CSV String type write type (#8499)|https://github.com/apache/seatunnel/commit/9268f5a255| dev |
|[Hotfix][Connector-V2][SFTP] Add quote to sftp file names with wildcard characters (#8501)|https://github.com/apache/seatunnel/commit/c5751b001b| dev |
|[Fix][File] Fix Multi-file with binary format synchronization failed (#8546)|https://github.com/apache/seatunnel/commit/6e4ee468a5| dev |
|[Feature][Connector-V2] Support create emtpy file when no data (#8543)|https://github.com/apache/seatunnel/commit/275db78918| dev |
|[Feature][Connector-V2] Support single file mode in file sink (#8518)|https://github.com/apache/seatunnel/commit/e893deed50| dev |
|[Improve][Connector-file-base] Improved file allocation algorithm for subtasks. (#8453)|https://github.com/apache/seatunnel/commit/d61cba233e|2.3.9|
|[Bug] [connector-file] When the data source field is less than the target (Hive) field，it will throw null pointer exception#8150 (#8200)|https://github.com/apache/seatunnel/commit/25b8a02b76|2.3.9|
|[Fix] Set all snappy dependency use one version (#8423)|https://github.com/apache/seatunnel/commit/3ac977c8d3|2.3.9|
|[Improve][Connector][Hive] skip temporary hidden directories (#8402)|https://github.com/apache/seatunnel/commit/9fdedc487e|2.3.9|
|[Feature][Connector-V2] Support use EasyExcel as read excel engine (#8064)|https://github.com/apache/seatunnel/commit/b8e1177fcb|2.3.9|
|[BugFix][Excel] Fix read formulas/number cell value of excel (#8316)|https://github.com/apache/seatunnel/commit/00c5aed1af|2.3.9|
|[Improve][Connector-V2] Add some debug log when create dir in (S)FTP (#8286)|https://github.com/apache/seatunnel/commit/8687bb8e91|2.3.9|
|[Improve][Transform] gz support excel (#8181)|https://github.com/apache/seatunnel/commit/c3ae726ee0|2.3.9|
|[Improve][dist]add shade check rule (#8136)|https://github.com/apache/seatunnel/commit/51ef800016|2.3.9|
|[Feature][File] Support config null format for text file read (#8109)|https://github.com/apache/seatunnel/commit/2dbf02df47|2.3.9|
|[Improve][Excel] Support read blank string &amp; auto type-cast (#8111)|https://github.com/apache/seatunnel/commit/3a54f1253f|2.3.9|
|[Improve][API] Unified tables_configs and table_list (#8100)|https://github.com/apache/seatunnel/commit/84c0b8d660|2.3.9|
|[Feature][Connectors] LocalFile Support reading gz (#8025)|https://github.com/apache/seatunnel/commit/337aa50f08|2.3.9|
|[Hotfix][Zeta] Fix the dependency conflict between the guava in hadoop-aws and hive-exec (#7986)|https://github.com/apache/seatunnel/commit/a7837f1f19|2.3.9|
|[Fix][Connector-V2] Fix file binary format sync convert directory to file (#7942)|https://github.com/apache/seatunnel/commit/86ae9272c4|2.3.9|
|[Fix][Connector-V2][FTP] Fix FTP connector connection_mode is not effective (#7865)|https://github.com/apache/seatunnel/commit/26c528a5ed|2.3.9|
|[Fix][Connector-V2][connector-file-base-hadoop] Fixed HdfsFile source load the krb5_path configuration (#7870)|https://github.com/apache/seatunnel/commit/cd9836bced|2.3.9|
|[Improve][Connector-V2] Change File Read/WriteStrategy `setSeaTunnelRowTypeInfo` to `setCatalogTable` (#7829)|https://github.com/apache/seatunnel/commit/6b5f74e524|2.3.9|
|[Feature][Connector-V2]Sftp file source support multiple table (#7824)|https://github.com/apache/seatunnel/commit/cfb8760f58|2.3.9|
|[Feature][Restapi] Allow metrics information to be associated to logical plan nodes (#7786)|https://github.com/apache/seatunnel/commit/6b7c53d03c|2.3.9|
|[Bug] [connectors-v2] The Hadoop Source/Sink fails with Unable to find valid Kerberos Ticket. (#7809)|https://github.com/apache/seatunnel/commit/a8bdea24cc|2.3.9|
|[Fix][Connector-V2] Fix When reading Excel data, string and date type conversion errors (#7796)|https://github.com/apache/seatunnel/commit/749b2fe364|2.3.9|
|[Feature][Connector-V2]Ftp file source support multiple table (#7795)|https://github.com/apache/seatunnel/commit/22fe27a3d6|2.3.9|
|[Feature][Connector-V2] sftp file sink suport multiple table and save mode (#7668)|https://github.com/apache/seatunnel/commit/dc4b9898f7|2.3.8|
|[Improve][Connector-V2] Support read archive compress file (#7633)|https://github.com/apache/seatunnel/commit/3f98cd8a16|2.3.8|
|[Feature][Connector-V2] Ftp file sink suport multiple table and save mode (#7665)|https://github.com/apache/seatunnel/commit/4f812e12ae|2.3.8|
|[Improve] Refactor S3FileCatalog and it&#x27;s factory (#7457)|https://github.com/apache/seatunnel/commit/d928e8b113|2.3.8|
|[Improve] Added OSSFileCatalog and it&#x27;s factory (#7458)|https://github.com/apache/seatunnel/commit/9006a205db|2.3.8|
|[Feature][Connector-V2][Iceberg] Support Iceberg Kerberos (#7246)|https://github.com/apache/seatunnel/commit/e3001207c8|2.3.8|
|[Improve][Connector] Add multi-table sink option check (#7360)|https://github.com/apache/seatunnel/commit/2489f6446b|2.3.7|
|[Feature][Core] Support using upstream table placeholders in sink options and auto replacement (#7131)|https://github.com/apache/seatunnel/commit/c4ca74122c|2.3.6|
|[feature][connector-file-local] add save mode function for localfile (#7080)|https://github.com/apache/seatunnel/commit/7b2f538310|2.3.6|
|[Hotfix][Hive Connector] Fix Hive hdfs-site.xml and hive-site.xml not be load error (#7069)|https://github.com/apache/seatunnel/commit/c23a577f34|2.3.6|
|[Feature][Connector-V2] Add Huawei Cloud OBS connector (#4578)|https://github.com/apache/seatunnel/commit/d266f4db64|2.3.6|
|[Improve][File Connector]Improve xml read code &amp; fix can not use true for a boolean option (#6930)|https://github.com/apache/seatunnel/commit/c13a563994|2.3.6|
|[Improve][Files] Support write fixed/timestamp as int96 of parquet (#6971)|https://github.com/apache/seatunnel/commit/1a48a9c493|2.3.6|
|[Feature][Connector-V2] Supports the transfer of any file (#6826)|https://github.com/apache/seatunnel/commit/c1401787b3|2.3.6|
|[Feature][S3 File] Make S3 File Connector support multiple table write (#6698)|https://github.com/apache/seatunnel/commit/8f2049b2f1|2.3.6|
|[Feature][Doris] Add Doris type converter (#6354)|https://github.com/apache/seatunnel/commit/5189991843|2.3.6|
|[Improve][Connector-v2] The hive connector support multiple filesystem (#6648)|https://github.com/apache/seatunnel/commit/8a4c01fe35|2.3.6|
|[bigfix][S3 File]:Change the [SCHEMA] attribute of the [S3CONF class] to be non-static to avoid being reassigned after deserialization (#6717)|https://github.com/apache/seatunnel/commit/79bb70101a|2.3.6|
|[Improve] Improve read with parquet type convert error (#6683)|https://github.com/apache/seatunnel/commit/6c65805699|2.3.5|
|[Hotfix] fix http source can not read yyyy-MM-dd HH:mm:ss format bug &amp; Improve DateTime Utils (#6601)|https://github.com/apache/seatunnel/commit/19888e7969|2.3.5|
|[Feature][Tool] Add connector check script for issue 6199 (#6635)|https://github.com/apache/seatunnel/commit/65aedf6a79|2.3.5|
|[Bug] Fix OrcWriteStrategy/ParquetWriteStrategy doesn&#x27;t login with kerberos (#6472)|https://github.com/apache/seatunnel/commit/24441c876d|2.3.5|
|[Bug] [formats] Fix fail to parse line when content contains the file delimiter (#6589)|https://github.com/apache/seatunnel/commit/17e29185fa|2.3.5|
|[Improve][Connector-V2] Support read orc with schema config to cast type (#6531)|https://github.com/apache/seatunnel/commit/d1599f8ad9|2.3.5|
|[Chore] Fix `file` spell errors (#6606)|https://github.com/apache/seatunnel/commit/2599d3b736|2.3.5|
|[Fix][Connector-V2] Fix connector support SPI but without no args constructor (#6551)|https://github.com/apache/seatunnel/commit/5f3c9c36a5|2.3.5|
|[Feature][Connectors-V2][File]support assign encoding for file source/sink (#6489)|https://github.com/apache/seatunnel/commit/d159fbe086|2.3.5|
|Add support for XML file type to various file connectors such as SFTP, FTP, LocalFile, HdfsFile, and more. (#6327)|https://github.com/apache/seatunnel/commit/ec533ecd9a|2.3.5|
|[BugFix][Connector-file-sftp] Fix SFTPInputStream.close does not correctly trigger the closing of the file stream (#6323) (#6329)|https://github.com/apache/seatunnel/commit/eee881af91|2.3.5|
|[Test][E2E] Add thread leak check for connector (#5773)|https://github.com/apache/seatunnel/commit/1f2f3fc5f0|2.3.4|
|Fix HiveMetaStoreProxy#enableKerberos will return true if doesn&#x27;t enable kerberos (#6307)|https://github.com/apache/seatunnel/commit/1dad6f7061|2.3.4|
|[Feature][Connector]add s3file save mode function (#6131)|https://github.com/apache/seatunnel/commit/81c51073bf|2.3.4|
|[bugfix][file-execl] Fix the Issue of Abnormal Data Reading from Excel Files (#5932)|https://github.com/apache/seatunnel/commit/6a2b05a845|2.3.4|
|[Feature][Connectors-v2-file-ftp] FTP source/sink add ftp connection mode (#6077)  (#6099)|https://github.com/apache/seatunnel/commit/f6bcc4d59d|2.3.4|
|Disable HDFSFileSystem cache (#6039)|https://github.com/apache/seatunnel/commit/135c91818e|2.3.4|
|[Feature][OssFile Connector] Make Oss implement source factory and sink factory (#6062)|https://github.com/apache/seatunnel/commit/1a8e9b4554|2.3.4|
|[Improve][Common] Adapt `FILE_OPERATION_FAILED` to `CommonError` (#5928)|https://github.com/apache/seatunnel/commit/b3dc0bbc21|2.3.4|
|[Feature][Connector-V2] Support read .xls excel file (#6066)|https://github.com/apache/seatunnel/commit/43787a3dde|2.3.4|
|Add multiple table file sink to base (#6049)|https://github.com/apache/seatunnel/commit/085e0e5fc3|2.3.4|
|[Refactor][File Connector] Put Multiple Table File API to File Base Module (#6033)|https://github.com/apache/seatunnel/commit/c324d663b4|2.3.4|
|[Hotfix][Oss File Connector] fix oss connector can not run bug (#6010)|https://github.com/apache/seatunnel/commit/755bc2a730|2.3.4|
|Support using multiple hadoop account (#5903)|https://github.com/apache/seatunnel/commit/d69d88d1aa|2.3.4|
|[Feature][Core] Upgrade flink source translation (#5100)|https://github.com/apache/seatunnel/commit/5aabb14a94|2.3.4|
|[Feature] LocalFile sink support multiple table (#5931)|https://github.com/apache/seatunnel/commit/0fdf45f94d|2.3.4|
|[Improve][File] Clean memory buffer of `JsonWriteStrategy` &amp; `ExcelWriteStrategy` (#5925)|https://github.com/apache/seatunnel/commit/7297a4c95c|2.3.4|
|[Bug][Connector][FileBase]Parquet reader parsing array type exception. (#4457)|https://github.com/apache/seatunnel/commit/5c6b11329c|2.3.4|
|[Improve]Change System.out.println to log output. (#5912)|https://github.com/apache/seatunnel/commit/bbedb07a9c|2.3.4|
|[Feature] LocalFileSource support multiple table|https://github.com/apache/seatunnel/commit/72be6663ad|2.3.4|
|[Improve][Common] Introduce new error define rule (#5793)|https://github.com/apache/seatunnel/commit/9d1b2582b2|2.3.4|
|[Improve] Remove use `SeaTunnelSink::getConsumedType` method and mark it as deprecated (#5755)|https://github.com/apache/seatunnel/commit/8de7408100|2.3.4|
|[Improve][connector-file] unifiy option between file source/sink and update document (#5680)|https://github.com/apache/seatunnel/commit/8d87cf8fc4|2.3.4|
|[Improve][LocalFile] parquet use system timezone (#5605)|https://github.com/apache/seatunnel/commit/b3e13513ac|2.3.4|
|[Bugfix][Connector-v2] fix file sink `isPartitionFieldWriteInFile` occurred exception when no columns are given (#5508)|https://github.com/apache/seatunnel/commit/9fb5499295|2.3.4|
|[Feature] Support `LZO` compress on File Read (#5083)|https://github.com/apache/seatunnel/commit/a4a1901096|2.3.4|
|[Feature][Connector-V2][File] Support read empty directory (#5591)|https://github.com/apache/seatunnel/commit/1f58f224a0|2.3.4|
|Support config column/primaryKey/constraintKey in schema (#5564)|https://github.com/apache/seatunnel/commit/eac76b4e50|2.3.4|
|[Feature] [File Connector]optionrule FILE_FORMAT_TYPE is text/csv ,add parameter BaseSinkConfig.ENABLE_HEADER_WRITE: #5566 (#5567)|https://github.com/apache/seatunnel/commit/0e02db768d|2.3.4|
|[Hotfix][File-Connector] Fix WriteStrategy parallel writing thread unsafe issue (#5546)|https://github.com/apache/seatunnel/commit/1177d02d55|2.3.4|
|[Bugfix][jindo] Remove useless code (#5540)|https://github.com/apache/seatunnel/commit/b889618379|2.3.4|
|[Feature] [File Connector] Supports writing column names when the output type is file (CSV) (#5459)|https://github.com/apache/seatunnel/commit/f73b37291e|2.3.4|
|[bugfix][CI]remove jindo dependencies|https://github.com/apache/seatunnel/commit/38e1e30e20|2.3.4|
|[Feature][Connector-V2][Oss jindo] Fix the problem of jindo driver download failure. (#5511)|https://github.com/apache/seatunnel/commit/a14d9c0d08|2.3.4|
|Revert &quot;[fix][hive-source][bug] fix An error occurred reading an empty directory (#5427)&quot; (#5487)|https://github.com/apache/seatunnel/commit/093901068e|2.3.4|
|[fix][hive-source][bug] fix An error occurred reading an empty directory (#5427)|https://github.com/apache/seatunnel/commit/de7b86a5dd|2.3.4|
|[Improve][CheckStyle] Remove useless &#x27;SuppressWarnings&#x27; annotation of checkstyle. (#5260)|https://github.com/apache/seatunnel/commit/51c0d709ba|2.3.4|
|[Hotfix] Fix com.google.common.base.Preconditions to seatunnel shade one (#5284)|https://github.com/apache/seatunnel/commit/ed5eadcf73|2.3.3|
|[Feature][Connector V2][File] Add config of &#x27;file_filter_pattern&#x27;, which used for filtering files. (#5153)|https://github.com/apache/seatunnel/commit/a3c13e59eb|2.3.3|
|[bugfix] [File Base] Fix Hadoop Kerberos authentication related issues. (#5171)|https://github.com/apache/seatunnel/commit/2a85525f4c|2.3.3|
|[Feature][Connector-V2][File] Add cos source&amp;sink (#4979)|https://github.com/apache/seatunnel/commit/1f94676436|2.3.3|
|[Improve][Connector[File] Optimize files commit order (#5045)|https://github.com/apache/seatunnel/commit/1e18a8c530|2.3.3|
|[Improve][Connector-V2][OSS-Jindo] Optimize jindo oss connector (#4964)|https://github.com/apache/seatunnel/commit/5fbfd05061|2.3.3|
|[Feature][E2E][FtpFile] add ftp file e2e test case (#4647)|https://github.com/apache/seatunnel/commit/b1b1f5e7e0|2.3.3|
|[Bugfix] [Connector-V2] [File] Fix read temp file (#4876)|https://github.com/apache/seatunnel/commit/5e03d22d6c|2.3.2|
|[Bug Fix] [seatunnel-connectors-v2][SFTP] Fix incorrect exception handling logic (#4720)|https://github.com/apache/seatunnel/commit/dc350e67c3|2.3.2|
|[Fix][Connector-V2] Fix file-oss config check bug and amend file-oss-jindo factoryIdentifier (#4581)|https://github.com/apache/seatunnel/commit/5c4f17df20|2.3.2|
|[chore] delete unavailable S3 &amp; Kafka Catalogs (#4477)|https://github.com/apache/seatunnel/commit/e0aec5ecec|2.3.2|
| [Feature][ConnectorV2]add file excel sink and source (#4164)|https://github.com/apache/seatunnel/commit/e3b97ae5d2|2.3.2|
|Change file type to file_format_type in file source/sink (#4249)|https://github.com/apache/seatunnel/commit/973a2fae3c|2.3.1|
|[Chore] Upgrade guava to 27.0-jre (#4238)|https://github.com/apache/seatunnel/commit/4851bee575|2.3.1|
|Add redshift datatype convertor (#4245)|https://github.com/apache/seatunnel/commit/b19011517f|2.3.1|
|[improve][zeta] fix zeta bugs|https://github.com/apache/seatunnel/commit/3a82e8b39f|2.3.1|
|[chore] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/291214ad6f|2.3.1|
|Merge branch &#x27;dev&#x27; into merge/cdc|https://github.com/apache/seatunnel/commit/4324ee1912|2.3.1|
|[Improve][Project] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/423b583038|2.3.1|
|[improve][api] Refactoring schema parse (#4157)|https://github.com/apache/seatunnel/commit/b2f573a13e|2.3.1|
|[Imprve][Connector-V2][Hive] Support read text table &amp; Column projection (#4105)|https://github.com/apache/seatunnel/commit/717620f542|2.3.1|
|[Improve][build] Give the maven module a human readable name (#4114)|https://github.com/apache/seatunnel/commit/d7cd601051|2.3.1|
|Add S3Catalog (#4121)|https://github.com/apache/seatunnel/commit/7d7f506547|2.3.1|
|[Improve][Project] Code format with spotless plugin. (#4101)|https://github.com/apache/seatunnel/commit/a2ab166561|2.3.1|
|[Improve][Connector-V2][Hive] Support assign partitions (#3842)|https://github.com/apache/seatunnel/commit/6a4a850b4c|2.3.1|
|[Bug][Connectors] Text And Json WriteStrategy lost the sinkColumnsIndexInRow (#3863)|https://github.com/apache/seatunnel/commit/7b5f6f1bc2|2.3.1|
|[Feature][Connector-V2][File] Support compress (#3899)|https://github.com/apache/seatunnel/commit/55602f6b1c|2.3.1|
|[Feature][Connector-V2][File] Allow the user to set the row delimiter as an empty string (#3854)|https://github.com/apache/seatunnel/commit/84508fcb65|2.3.1|
|[Feature][Connector-V2] Support kerberos in hive and hdfs file connector (#3840)|https://github.com/apache/seatunnel/commit/055ad9d836|2.3.1|
|[Feature][Connector-V2][File] Support skip number when reading text csv files (#3900)|https://github.com/apache/seatunnel/commit/243b6a6b23|2.3.1|
|[Feature][Connector] add get source method to all source connector (#3846)|https://github.com/apache/seatunnel/commit/417178fb84|2.3.1|
|[Feature][API &amp; Connector &amp; Doc] add parallelism and column projection interface (#3829)|https://github.com/apache/seatunnel/commit/b9164b8ba1|2.3.1|
|[Improve][Connector-V2][File] Improve file connector option rule and document (#3812)|https://github.com/apache/seatunnel/commit/bd76077669|2.3.1|
|[Improve][Connector-V2][File] File Connector add lzo compression way. (#3782)|https://github.com/apache/seatunnel/commit/8875d02589|2.3.1|
|[Improve][Connector-V2] The log outputs detailed exception stack information (#3805)|https://github.com/apache/seatunnel/commit/d0c6217f27|2.3.1|
|fix file source connector option rule bug (#3804)|https://github.com/apache/seatunnel/commit/cab42f6eb1|2.3.1|
|[Feature][Shade] Add seatunnel hadoop3 uber (#3755)|https://github.com/apache/seatunnel/commit/5a024bdf8f|2.3.0|
|[Improve][Connector-V2][HDFS] Support setting hdfs-site.xml (#3778)|https://github.com/apache/seatunnel/commit/c8d59ecac1|2.3.0|
|[Feature][Connector-V2][File] Optimize filesystem utils (#3749)|https://github.com/apache/seatunnel/commit/ac4e880fb5|2.3.0|
|[Improve] [Connector-V2] Fix Kafka sink can&#x27;t run EXACTLY_ONCE semantics (#3724)|https://github.com/apache/seatunnel/commit/5e3f196e29|2.3.0|
|[Connector-V2] [File] Fix bug data file name will duplicate when use SeaTunnel Engine (#3717)|https://github.com/apache/seatunnel/commit/c96c53004f|2.3.0|
|[Engine][Checkpoint]Unified naming style (#3714)|https://github.com/apache/seatunnel/commit/bc0bd3bec3|2.3.0|
|[Connector][File-S3]Set AK is not required (#3713)|https://github.com/apache/seatunnel/commit/da3c526172|2.3.0|
|[Hotfix][Connector-V2][File] Fix file sink connector npe (#3706)|https://github.com/apache/seatunnel/commit/a662a88fdc|2.3.0|
|[Connector&amp;Engine]Set S3 AK to optional (#3688)|https://github.com/apache/seatunnel/commit/4710918b02|2.3.0|
|[Hotfix][OssFile Connector]fix ossfile bug (#3684)|https://github.com/apache/seatunnel/commit/ba6259274d|2.3.0|
|[Feature][Connector-V2][Oss jindo] Add oss jindo source &amp; sink connector (#3456)|https://github.com/apache/seatunnel/commit/2507372311|2.3.0|
|[Improve][Connector-V2][File] Support split file based on batch size (#3625)|https://github.com/apache/seatunnel/commit/f39e3a531d|2.3.0|
|[Connector][S3]Support s3a protocol (#3632)|https://github.com/apache/seatunnel/commit/ae4cc9c1ec|2.3.0|
|[Hotfix][OptionRule] Fix option rule about all connectors (#3592)|https://github.com/apache/seatunnel/commit/226dc6a119|2.3.0|
|[Improve][Connector-V2][File] Unified excetion for file source &amp; sink connectors (#3525)|https://github.com/apache/seatunnel/commit/031e8e263c|2.3.0|
|[Hotfix][Connector-V2][Hive] Fix npe of getting file system (#3506)|https://github.com/apache/seatunnel/commit/e1fc3d1b01|2.3.0|
|[Improve][core-v1][seatunnel-core-base] remove seatunnel-core-base (#3480)|https://github.com/apache/seatunnel/commit/d6e6a02a36|2.3.0|
|[Feature][Connector-V2][File] Add option and factory for file connectors (#3375)|https://github.com/apache/seatunnel/commit/db286e8631|2.3.0|
|[Improve][Connector-V2][File] Improve code structure (#3238)|https://github.com/apache/seatunnel/commit/dd5c353881|2.3.0|
|[Connector-V2] [ElasticSearch] Add ElasticSearch Source/Sink Factory (#3325)|https://github.com/apache/seatunnel/commit/38254e3f26|2.3.0|
|[Hotfix][Connector-V2][Hive] Fix the bug that when write data to hive throws NullPointerException (#3258)|https://github.com/apache/seatunnel/commit/777bf6b42e|2.3.0|
|[Core] [Improve] Fix some sonar check error (#3240)|https://github.com/apache/seatunnel/commit/8664bb53a5|2.3.0|
|[Bug]add 3node worker done test and fix some bug (#3115)|https://github.com/apache/seatunnel/commit/bc852a4dff|2.3.0|
|[Feature][Connector-V2][SFTP] Add SFTP file source &amp; sink connector (#3006)|https://github.com/apache/seatunnel/commit/9e496383b8|2.3.0|
|[Feature][Connector-V2][S3] Add S3 file source &amp; sink connector (#3119)|https://github.com/apache/seatunnel/commit/f27d68ca9c|2.3.0-beta|
|[Feature][Connector-V2][File] Fix filesystem get error (#3117)|https://github.com/apache/seatunnel/commit/7404c180de|2.3.0-beta|
|[Improve][Connector-v2][file] Reuse array type container when read row data (#3123)|https://github.com/apache/seatunnel/commit/da0646ac6d|2.3.0-beta|
|[Hotfix][Connector-V2][File] Fix ParquetReadStrategy get NPE (#3122)|https://github.com/apache/seatunnel/commit/ba99de08c8|2.3.0-beta|
|[hotfix][engine] Add master node switch test and fix bug (#3082)|https://github.com/apache/seatunnel/commit/608be51bc4|2.3.0-beta|
|[Improve][Connector-V2][File] Support parse field from file path (#2985)|https://github.com/apache/seatunnel/commit/0bc12085c2|2.3.0-beta|
|[hotfix][connector][file] Solved the bug of can not parse &#x27;\t&#x27; as delimiter from config file (#3083)|https://github.com/apache/seatunnel/commit/bfde596754|2.3.0-beta|
|unify `flatten-maven-plugin` version (#3078)|https://github.com/apache/seatunnel/commit/ed743fddcc|2.3.0-beta|
|[Improve][Connector-V2] Improve text write (#2971)|https://github.com/apache/seatunnel/commit/0ecd7906c2|2.3.0-beta|
|[Improve][connector][file] Support user-defined schema for reading text file (#2976)|https://github.com/apache/seatunnel/commit/1c05ee0d7e|2.3.0-beta|
|[Bug][Connector-V2][File] Fix the bug of incorrect path in windows environment (#2980)|https://github.com/apache/seatunnel/commit/2e16161865|2.3.0-beta|
|[Improve][Connector] Improve write parquet (#2943)|https://github.com/apache/seatunnel/commit/8fd966394b|2.3.0-beta|
|[Improve][all] change Log to @Slf4j (#3001)|https://github.com/apache/seatunnel/commit/6016100f12|2.3.0-beta|
|[Bug][connector-file-base] Fix source split assigning reader to negative number (#2921)|https://github.com/apache/seatunnel/commit/0b5a2852fb|2.3.0-beta|
|[Improve][Connector-V2] Improve orc write strategy to support all data types (#2860)|https://github.com/apache/seatunnel/commit/4d048cc23e|2.3.0-beta|
|[Fix] [Connector-V2-File] Fix file connector bug (#2858)|https://github.com/apache/seatunnel/commit/e0459bbab6|2.2.0-beta|
|[Fix][Connector-V2] Fix HiveSource Connector read orc table error (#2845)|https://github.com/apache/seatunnel/commit/61720306e7|2.2.0-beta|
|[Improve][Connector-V2] Improve read parquet (#2841)|https://github.com/apache/seatunnel/commit/e19bc82f9b|2.2.0-beta|
|[Imporve][Connector-V2] Refactor ftp sink &amp; Add ftp file source (#2774)|https://github.com/apache/seatunnel/commit/4aacbcdd1f|2.2.0-beta|
|[Bug] [Connector-V2] Fix hive source connector parallelism not work (#2823)|https://github.com/apache/seatunnel/commit/9f21d4c769|2.2.0-beta|
|[Improve][Connector-V2] Imporve orc read strategy (#2747)|https://github.com/apache/seatunnel/commit/af34beda37|2.2.0-beta|
|[Bug][Connector-V2] Fix error option (#2775)|https://github.com/apache/seatunnel/commit/488e561eef|2.2.0-beta|
|[Improve][Connector-V2] Refactor hdfs file sink connector code structure (#2701)|https://github.com/apache/seatunnel/commit/6129c02567|2.2.0-beta|
|[DEV][Api] Replace SeaTunnelContext with JobContext and remove singleton pattern (#2706)|https://github.com/apache/seatunnel/commit/cbf82f755c|2.2.0-beta|
|[Improve][build] Improved scope of maven-shade-plugin (#2665)|https://github.com/apache/seatunnel/commit/93bc8bd116|2.2.0-beta|
|[Improve][Connector-V2] Refactor local file sink connector code structure (#2655)|https://github.com/apache/seatunnel/commit/6befd599a1|2.2.0-beta|
|[Feature][Connector-V2] Add oss sink (#2629)|https://github.com/apache/seatunnel/commit/bb2ad40487|2.2.0-beta|
|[#2606]Dependency management split (#2630)|https://github.com/apache/seatunnel/commit/fc047be69b|2.2.0-beta|
|[Improve][Connector-V2] Refactor the structure of file sink to reduce redundant codes (#2555)|https://github.com/apache/seatunnel/commit/6315092930|2.2.0-beta|
|[chore][connector-common] Rename SeatunnelSchema to SeaTunnelSchema (#2538)|https://github.com/apache/seatunnel/commit/7dc2a27388|2.2.0-beta|
|[Feature][Connector-V2] Add oss source connector (#2467)|https://github.com/apache/seatunnel/commit/712b77744e|2.2.0-beta|
|[Feature][File connector] Support ftp file sink (#2483)|https://github.com/apache/seatunnel/commit/a87e5de80a|2.2.0-beta|
|[Feature][Connector-V2] Local file json support (#2465)|https://github.com/apache/seatunnel/commit/65a92f2496|2.2.0-beta|
|[Feature][Connector-V2] Add hdfs file json support (#2451)|https://github.com/apache/seatunnel/commit/84f6b17c15|2.2.0-beta|
|[Improve][Connector-V2] Refactor the package of hdfs file connector (#2402)|https://github.com/apache/seatunnel/commit/87d0624c5b|2.2.0-beta|
|[Feature][Connector-V2] Add hdfs file source connector (#2420)|https://github.com/apache/seatunnel/commit/4fb6f2a216|2.2.0-beta|
|[Feature][Connector-V2] Add local file connector source (#2419)|https://github.com/apache/seatunnel/commit/eff595c452|2.2.0-beta|
|[Feature][Connector-V2] Add base source connector code for connector-file-base (#2399)|https://github.com/apache/seatunnel/commit/1829ddc662|2.2.0-beta|
|[Improve][Connector-V2] Refactor the package of local file connector (#2403)|https://github.com/apache/seatunnel/commit/a538daed5c|2.2.0-beta|
|[Feature][Connector-V2] Add json file sink &amp; json format (#2385)|https://github.com/apache/seatunnel/commit/dd68c06b0a|2.2.0-beta|
|[Bug][Connector-V2] Fix the bug that file connector release resources multi times (#2379)|https://github.com/apache/seatunnel/commit/58c64aab2a|2.2.0-beta|
|[Improve][Connector-V2] Optimize the code structure (#2380)|https://github.com/apache/seatunnel/commit/7376ec7ab1|2.2.0-beta|
|[Imporve][Connector-V2] Remove redundant type judge logic because of pr #2315 (#2370)|https://github.com/apache/seatunnel/commit/42e8c25e50|2.2.0-beta|
|[Feature][Connector-V2] Support orc file format in file connector (#2369)|https://github.com/apache/seatunnel/commit/f44fe1e033|2.2.0-beta|
|[improve][UT] Upgrade junit to 5.+ (#2305)|https://github.com/apache/seatunnel/commit/362319ff3e|2.2.0-beta|
|Replace plain string with constants (#2308)|https://github.com/apache/seatunnel/commit/3c0415e56e|2.2.0-beta|
|[Connector-V2] Add parquet writer in file connector (#2273)|https://github.com/apache/seatunnel/commit/c95cc72cfa|2.2.0-beta|
|[checkstyle] Improved validation scope of MagicNumber (#2194)|https://github.com/apache/seatunnel/commit/6d08b5f369|2.2.0-beta|
|[Connector-V2] Add Hive sink connector v2 (#2158)|https://github.com/apache/seatunnel/commit/23ad4ee735|2.2.0-beta|
|[Connector-V2] Add File Sink Connector (#2117)|https://github.com/apache/seatunnel/commit/e2283da64f|2.2.0-beta|

</details>
