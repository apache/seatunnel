<details><summary> Change Log </summary>

| Change | Commit | Version |
| --- | --- | --- |
|[Improve][connector-hive] Improved hive file allocation algorithm for subtasks (#8876)|https://github.com/apache/seatunnel/commit/89d1878ade| dev |
|[Improve] restruct connector common options (#8634)|https://github.com/apache/seatunnel/commit/f3499a6eeb| dev |
|[Fix][Hive] Writing parquet files supports the optional timestamp int96 (#8509)|https://github.com/apache/seatunnel/commit/856aea1952| dev |
|[Fix] Set all snappy dependency use one version (#8423)|https://github.com/apache/seatunnel/commit/3ac977c8d3|2.3.9|
|[Fix][Connector-V2] Fix hive krb5 path not work (#8228)|https://github.com/apache/seatunnel/commit/e18a4d07b4|2.3.9|
|[Improve][dist]add shade check rule (#8136)|https://github.com/apache/seatunnel/commit/51ef800016|2.3.9|
|[Feature][File] Support config null format for text file read (#8109)|https://github.com/apache/seatunnel/commit/2dbf02df47|2.3.9|
|[Improve][API] Unified tables_configs and table_list (#8100)|https://github.com/apache/seatunnel/commit/84c0b8d660|2.3.9|
|[Feature][Core] Rename `result_table_name`/`source_table_name` to `plugin_input/plugin_output` (#8072)|https://github.com/apache/seatunnel/commit/c7bbd322db|2.3.9|
|[Feature][E2E] Add hive3 e2e test case (#8003)|https://github.com/apache/seatunnel/commit/9a24fac2c4|2.3.9|
|[Improve][Connector-V2] Change File Read/WriteStrategy `setSeaTunnelRowTypeInfo` to `setCatalogTable` (#7829)|https://github.com/apache/seatunnel/commit/6b5f74e524|2.3.9|
|[Feature][Restapi] Allow metrics information to be associated to logical plan nodes (#7786)|https://github.com/apache/seatunnel/commit/6b7c53d03c|2.3.9|
|[Improve][Zeta] Split the classloader of task group (#7580)|https://github.com/apache/seatunnel/commit/3be0d1cc61|2.3.8|
|[Feature][Core] Support using upstream table placeholders in sink options and auto replacement (#7131)|https://github.com/apache/seatunnel/commit/c4ca74122c|2.3.6|
|[Improve][Hive] Close resources when exception occurs (#7205)|https://github.com/apache/seatunnel/commit/561171528b|2.3.6|
|[Hotfix][Hive Connector] Fix Hive hdfs-site.xml and hive-site.xml not be load error (#7069)|https://github.com/apache/seatunnel/commit/c23a577f34|2.3.6|
|Fix hive load hive_site_path and hdfs_site_path too late (#7017)|https://github.com/apache/seatunnel/commit/e2578a5b4d|2.3.6|
|[Bug] [connector-hive] Eanble login with kerberos for hive (#6893)|https://github.com/apache/seatunnel/commit/26e433e472|2.3.6|
|[Feature][S3 File] Make S3 File Connector support multiple table write (#6698)|https://github.com/apache/seatunnel/commit/8f2049b2f1|2.3.6|
|[Feature] Hive Source/Sink support multiple table (#5929)|https://github.com/apache/seatunnel/commit/4d9287fce4|2.3.6|
|[Improve][Hive] udpate hive3 version (#6699)|https://github.com/apache/seatunnel/commit/1184c05c29|2.3.6|
|[HiveSink]Fix the risk of resource leakage. (#6721)|https://github.com/apache/seatunnel/commit/c23804f13b|2.3.6|
|[Improve][Connector-v2] The hive connector support multiple filesystem (#6648)|https://github.com/apache/seatunnel/commit/8a4c01fe35|2.3.6|
|[Fix][Connector-V2] Fix add hive partition error when partition already existed (#6577)|https://github.com/apache/seatunnel/commit/2a0a0b9d19|2.3.5|
|Fix HiveMetaStoreProxy#enableKerberos will return true if doesn&#x27;t enable kerberos (#6307)|https://github.com/apache/seatunnel/commit/1dad6f7061|2.3.4|
|[Feature][Engine] Unify job env parameters (#6003)|https://github.com/apache/seatunnel/commit/2410ab38f0|2.3.4|
|[Refactor][File Connector] Put Multiple Table File API to File Base Module (#6033)|https://github.com/apache/seatunnel/commit/c324d663b4|2.3.4|
|Support using multiple hadoop account (#5903)|https://github.com/apache/seatunnel/commit/d69d88d1aa|2.3.4|
|[Improve][Common] Introduce new error define rule (#5793)|https://github.com/apache/seatunnel/commit/9d1b2582b2|2.3.4|
|Support config column/primaryKey/constraintKey in schema (#5564)|https://github.com/apache/seatunnel/commit/eac76b4e50|2.3.4|
|[Hotfix][Connector-V2][Hive] fix the bug that hive-site.xml can not be injected in HiveConf (#5261)|https://github.com/apache/seatunnel/commit/04ce22ac1e|2.3.4|
|[Improve][Connector-v2][HiveSink]remove drop partition when abort. (#4940)|https://github.com/apache/seatunnel/commit/edef87b523|2.3.3|
|[feature][web] hive add option because web need (#5154)|https://github.com/apache/seatunnel/commit/5e1511ff0d|2.3.3|
|[Hotfix][Connector-V2][Hive] Support user-defined hive-site.xml (#4965)|https://github.com/apache/seatunnel/commit/2a064bcdb0|2.3.3|
|Change file type to file_format_type in file source/sink (#4249)|https://github.com/apache/seatunnel/commit/973a2fae3c|2.3.1|
|[hotfix] fixed schema options import error|https://github.com/apache/seatunnel/commit/656805f2df|2.3.1|
|[chore] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/291214ad6f|2.3.1|
|Merge branch &#x27;dev&#x27; into merge/cdc|https://github.com/apache/seatunnel/commit/4324ee1912|2.3.1|
|[Improve][Project] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/423b583038|2.3.1|
|[Imprve][Connector-V2][Hive] Support read text table &amp; Column projection (#4105)|https://github.com/apache/seatunnel/commit/717620f542|2.3.1|
|[Hotfix][Connector-V2][Hive] Fix hive unknownhost (#4141)|https://github.com/apache/seatunnel/commit/f1a1dfe4af|2.3.1|
|[Improve][build] Give the maven module a human readable name (#4114)|https://github.com/apache/seatunnel/commit/d7cd601051|2.3.1|
|[Improve][Project] Code format with spotless plugin. (#4101)|https://github.com/apache/seatunnel/commit/a2ab166561|2.3.1|
|[Improve][Connector-V2][Hive] Support assign partitions (#3842)|https://github.com/apache/seatunnel/commit/6a4a850b4c|2.3.1|
|[Improve][Connector-V2][Hive] Improve config check logic (#3886)|https://github.com/apache/seatunnel/commit/b4348f6f44|2.3.1|
|[Feature][Connector-V2] Support kerberos in hive and hdfs file connector (#3840)|https://github.com/apache/seatunnel/commit/055ad9d836|2.3.1|
|[Feature][Connector] add get source method to all source connector (#3846)|https://github.com/apache/seatunnel/commit/417178fb84|2.3.1|
|[Improve][Connector-V2] The log outputs detailed exception stack information (#3805)|https://github.com/apache/seatunnel/commit/d0c6217f27|2.3.1|
|[Feature][Shade] Add seatunnel hadoop3 uber (#3755)|https://github.com/apache/seatunnel/commit/5a024bdf8f|2.3.0|
|[Feature][Connector-V2][File] Optimize filesystem utils (#3749)|https://github.com/apache/seatunnel/commit/ac4e880fb5|2.3.0|
|[Hotfix][OptionRule] Fix option rule about all connectors (#3592)|https://github.com/apache/seatunnel/commit/226dc6a119|2.3.0|
|[Hotfix][Connector-V2][Hive] Fix npe of getting file system (#3506)|https://github.com/apache/seatunnel/commit/e1fc3d1b01|2.3.0|
|[Improve][Connector-V2][Hive] Unified exceptions for hive source &amp; sink connector (#3541)|https://github.com/apache/seatunnel/commit/12c0fb91d2|2.3.0|
|[Feature][Connector-V2][File] Add option and factory for file connectors (#3375)|https://github.com/apache/seatunnel/commit/db286e8631|2.3.0|
|[Hotfix][Connector-V2][Hive] Fix the bug that when write data to hive throws NullPointerException (#3258)|https://github.com/apache/seatunnel/commit/777bf6b42e|2.3.0|
|[Improve][Connector-V2][Hive] Hive Sink Support msck partitions (#3133)|https://github.com/apache/seatunnel/commit/a8738ef3c4|2.3.0-beta|
|unify `flatten-maven-plugin` version (#3078)|https://github.com/apache/seatunnel/commit/ed743fddcc|2.3.0-beta|
|[Engine][Merge] fix merge problem|https://github.com/apache/seatunnel/commit/0e9ceeefc9|2.3.0-beta|
|Merge remote-tracking branch &#x27;upstream/dev&#x27; into st-engine|https://github.com/apache/seatunnel/commit/ca80df779a|2.3.0-beta|
|update hive.metastore.version to hive.exec.version (#2879)|https://github.com/apache/seatunnel/commit/018ee0a3db|2.2.0-beta|
|[Bug][Connector-V2] Fix hive sink bug (#2870)|https://github.com/apache/seatunnel/commit/d661fa011e|2.2.0-beta|
|[Fix][Connector-V2] Fix HiveSource Connector read orc table error (#2845)|https://github.com/apache/seatunnel/commit/61720306e7|2.2.0-beta|
|[Bug][Connector-V2] Fix hive source text table name (#2797)|https://github.com/apache/seatunnel/commit/563637ebd1|2.2.0-beta|
|[Improve][Connector-V2] Refactor hive source &amp; sink connector (#2708)|https://github.com/apache/seatunnel/commit/a357dca365|2.2.0-beta|
|[DEV][Api] Replace SeaTunnelContext with JobContext and remove singleton pattern (#2706) (#2731)|https://github.com/apache/seatunnel/commit/e8929ab605|2.3.0-beta|
|[DEV][Api] Replace SeaTunnelContext with JobContext and remove singleton pattern (#2706)|https://github.com/apache/seatunnel/commit/cbf82f755c|2.2.0-beta|
|[#2606]Dependency management split (#2630)|https://github.com/apache/seatunnel/commit/fc047be69b|2.2.0-beta|
|[Improve][Connector-V2] Refactor the package of hdfs file connector (#2402)|https://github.com/apache/seatunnel/commit/87d0624c5b|2.2.0-beta|
|[Feature][Connector-V2] Add orc file support in connector hive sink (#2311) (#2374)|https://github.com/apache/seatunnel/commit/81cb80c050|2.2.0-beta|
|[improve][UT] Upgrade junit to 5.+ (#2305)|https://github.com/apache/seatunnel/commit/362319ff3e|2.2.0-beta|
|Decide table format using outputFormat in HiveSinkConfig #2303|https://github.com/apache/seatunnel/commit/3a2586f6dc|2.2.0-beta|
|[Feature][Connector-V2-Hive] Add parquet file format support to Hive Sink (#2310)|https://github.com/apache/seatunnel/commit/4ab3c21b8d|2.2.0-beta|
|Add BaseHiveCommitInfo for common hive commit info (#2306)|https://github.com/apache/seatunnel/commit/0d2f6f4d7c|2.2.0-beta|
|Remove same code to independent method in HiveSinkWriter (#2307)|https://github.com/apache/seatunnel/commit/e99e6ee726|2.2.0-beta|
|Avoid potential null pointer risk in HiveSinkWriter#snapshotState (#2302)|https://github.com/apache/seatunnel/commit/e7d817f7d2|2.2.0-beta|
|[Connector-V2] Add file type check logic in hive connector (#2275)|https://github.com/apache/seatunnel/commit/5488337c67|2.2.0-beta|
|[Connector-V2] Add parquet file reader for Hive Source Connector (#2199) (#2237)|https://github.com/apache/seatunnel/commit/59db97ed34|2.2.0-beta|
|Merge from dev to st-engine (#2243)|https://github.com/apache/seatunnel/commit/41e530afd5|2.3.0-beta|
|StateT of SeaTunnelSource should extend `Serializable` (#2214)|https://github.com/apache/seatunnel/commit/8c426ef850|2.2.0-beta|
|[Bug][connector-hive] filter &#x27;_SUCCESS&#x27; file in file list (#2235) (#2236)|https://github.com/apache/seatunnel/commit/db04651523|2.2.0-beta|
|[Bug][hive-connector-v2] Resolve the schema inconsistency bug (#2229) (#2230)|https://github.com/apache/seatunnel/commit/62ca075915|2.2.0-beta|
|[Bug][spark-connector-v2-example] fix the bug of no class found. (#2191) (#2192)|https://github.com/apache/seatunnel/commit/5dbc2df17e|2.2.0-beta|
|[Connector-V2] Add Hive sink connector v2 (#2158)|https://github.com/apache/seatunnel/commit/23ad4ee735|2.2.0-beta|
|[Connector-V2] Add File Sink Connector (#2117)|https://github.com/apache/seatunnel/commit/e2283da64f|2.2.0-beta|
|[Connector-V2]Hive Source (#2123)|https://github.com/apache/seatunnel/commit/ffcf3f59e2|2.2.0-beta|
|[api-draft][Optimize] Optimize module name (#2062)|https://github.com/apache/seatunnel/commit/f79e3112b1|2.2.0-beta|

</details>
