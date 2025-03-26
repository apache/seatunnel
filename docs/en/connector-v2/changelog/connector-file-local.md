<details><summary> Change Log </summary>

| Change | Commit | Version |
| --- | --- | --- |
|[Improve] Refactor file enumerator to prevent duplicate put split (#8989)|https://github.com/apache/seatunnel/commit/fdf1beae9| dev |
|[Improve][File] Add row_delimiter options into text file sink (#9017)|https://github.com/apache/seatunnel/commit/92aa855a3| dev |
|Revert &quot; [improve] update localfile connector config&quot; (#9018)|https://github.com/apache/seatunnel/commit/cdc79e13a|2.3.10|
| [improve] update localfile connector config (#8765)|https://github.com/apache/seatunnel/commit/def369a85|2.3.10|
|[Feature][Connector-V2] Add `filename_extension` parameter for read/write file (#8769)|https://github.com/apache/seatunnel/commit/78b23c0ef|2.3.10|
|[Improve] restruct connector common options (#8634)|https://github.com/apache/seatunnel/commit/f3499a6ee|2.3.10|
|[Feature][Connector-V2] Support create emtpy file when no data (#8543)|https://github.com/apache/seatunnel/commit/275db7891|2.3.10|
|[Feature][Connector-V2] Support single file mode in file sink (#8518)|https://github.com/apache/seatunnel/commit/e893deed5|2.3.10|
|[Feature][File] Support config null format for text file read (#8109)|https://github.com/apache/seatunnel/commit/2dbf02df4|2.3.9|
|[Improve][API] Unified tables_configs and table_list (#8100)|https://github.com/apache/seatunnel/commit/84c0b8d66|2.3.9|
|[Feature][Restapi] Allow metrics information to be associated to logical plan nodes (#7786)|https://github.com/apache/seatunnel/commit/6b7c53d03|2.3.9|
|[Improve][Connector-V2] Support read archive compress file (#7633)|https://github.com/apache/seatunnel/commit/3f98cd8a1|2.3.8|
|[Improve][Connector] Add multi-table sink option check (#7360)|https://github.com/apache/seatunnel/commit/2489f6446|2.3.7|
|[Feature][Core] Support using upstream table placeholders in sink options and auto replacement (#7131)|https://github.com/apache/seatunnel/commit/c4ca74122|2.3.6|
|[feature][connector-file-local] add save mode function for localfile (#7080)|https://github.com/apache/seatunnel/commit/7b2f53831|2.3.6|
|[Improve][Files] Support write fixed/timestamp as int96 of parquet (#6971)|https://github.com/apache/seatunnel/commit/1a48a9c49|2.3.6|
|[Chore] Fix `file` spell errors (#6606)|https://github.com/apache/seatunnel/commit/2599d3b73|2.3.5|
|[Feature][Connectors-V2][File]support assign encoding for file source/sink (#6489)|https://github.com/apache/seatunnel/commit/d159fbe08|2.3.5|
|Add support for XML file type to various file connectors such as SFTP, FTP, LocalFile, HdfsFile, and more. (#6327)|https://github.com/apache/seatunnel/commit/ec533ecd9|2.3.5|
|[Feature][OssFile Connector] Make Oss implement source factory and sink factory (#6062)|https://github.com/apache/seatunnel/commit/1a8e9b455|2.3.4|
|Add multiple table file sink to base (#6049)|https://github.com/apache/seatunnel/commit/085e0e5fc|2.3.4|
|[Refactor][File Connector] Put Multiple Table File API to File Base Module (#6033)|https://github.com/apache/seatunnel/commit/c324d663b|2.3.4|
|Support using multiple hadoop account (#5903)|https://github.com/apache/seatunnel/commit/d69d88d1a|2.3.4|
|[Feature] LocalFile sink support multiple table (#5931)|https://github.com/apache/seatunnel/commit/0fdf45f94|2.3.4|
|[Feature] LocalFileSource support multiple table|https://github.com/apache/seatunnel/commit/72be6663a|2.3.4|
|[Improve][Common] Introduce new error define rule (#5793)|https://github.com/apache/seatunnel/commit/9d1b2582b|2.3.4|
|[Improve][connector-file] unifiy option between file source/sink and update document (#5680)|https://github.com/apache/seatunnel/commit/8d87cf8fc|2.3.4|
|[Feature][Connector-V2][File] Support read empty directory (#5591)|https://github.com/apache/seatunnel/commit/1f58f224a|2.3.4|
|Support config column/primaryKey/constraintKey in schema (#5564)|https://github.com/apache/seatunnel/commit/eac76b4e5|2.3.4|
|[Feature] [File Connector]optionrule FILE_FORMAT_TYPE is text/csv ,add parameter BaseSinkConfig.ENABLE_HEADER_WRITE: #5566 (#5567)|https://github.com/apache/seatunnel/commit/0e02db768|2.3.4|
|[Feature][Connector V2][File] Add config of &#x27;file_filter_pattern&#x27;, which used for filtering files. (#5153)|https://github.com/apache/seatunnel/commit/a3c13e59e|2.3.3|
| [Feature][ConnectorV2]add file excel sink and source (#4164)|https://github.com/apache/seatunnel/commit/e3b97ae5d|2.3.2|
|Change file type to file_format_type in file source/sink (#4249)|https://github.com/apache/seatunnel/commit/973a2fae3|2.3.1|
|Merge branch &#x27;dev&#x27; into merge/cdc|https://github.com/apache/seatunnel/commit/4324ee191|2.3.1|
|[Improve][Project] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/423b58303|2.3.1|
|[improve][api] Refactoring schema parse (#4157)|https://github.com/apache/seatunnel/commit/b2f573a13|2.3.1|
|[Improve][build] Give the maven module a human readable name (#4114)|https://github.com/apache/seatunnel/commit/d7cd60105|2.3.1|
|[Improve][Project] Code format with spotless plugin. (#4101)|https://github.com/apache/seatunnel/commit/a2ab16656|2.3.1|
|[Feature][Connector-V2][File] Support compress (#3899)|https://github.com/apache/seatunnel/commit/55602f6b1|2.3.1|
|[Feature][Connector] add get source method to all source connector (#3846)|https://github.com/apache/seatunnel/commit/417178fb8|2.3.1|
|[Improve][Connector-V2][File] Improve file connector option rule and document (#3812)|https://github.com/apache/seatunnel/commit/bd7607766|2.3.1|
|[Feature][Shade] Add seatunnel hadoop3 uber (#3755)|https://github.com/apache/seatunnel/commit/5a024bdf8|2.3.0|
|[Hotfix][OptionRule] Fix option rule about all connectors (#3592)|https://github.com/apache/seatunnel/commit/226dc6a11|2.3.0|
|[Improve][Connector-V2][File] Unified excetion for file source &amp; sink connectors (#3525)|https://github.com/apache/seatunnel/commit/031e8e263|2.3.0|
|[Feature][Connector-V2][File] Add option and factory for file connectors (#3375)|https://github.com/apache/seatunnel/commit/db286e863|2.3.0|
|[Improve][Connector-V2][File] Improve code structure (#3238)|https://github.com/apache/seatunnel/commit/dd5c35388|2.3.0|
|[Connector-V2] [ElasticSearch] Add ElasticSearch Source/Sink Factory (#3325)|https://github.com/apache/seatunnel/commit/38254e3f2|2.3.0|
|[Improve][Connector-V2][File] Support parse field from file path (#2985)|https://github.com/apache/seatunnel/commit/0bc12085c|2.3.0-beta|
|[Improve][connector][file] Support user-defined schema for reading text file (#2976)|https://github.com/apache/seatunnel/commit/1c05ee0d7|2.3.0-beta|
|[Improve][Connector] Improve write parquet (#2943)|https://github.com/apache/seatunnel/commit/8fd966394|2.3.0-beta|
|[Fix][Connector-V2] Fix HiveSource Connector read orc table error (#2845)|https://github.com/apache/seatunnel/commit/61720306e|2.2.0-beta|
|[Improve][Connector-V2] Improve read parquet (#2841)|https://github.com/apache/seatunnel/commit/e19bc82f9|2.2.0-beta|
|[Bug][Connector-V2] Fix error option (#2775)|https://github.com/apache/seatunnel/commit/488e561ee|2.2.0-beta|
|[Improve][Connector-V2] Refactor local file sink connector code structure (#2655)|https://github.com/apache/seatunnel/commit/6befd599a|2.2.0-beta|
|[#2606]Dependency management split (#2630)|https://github.com/apache/seatunnel/commit/fc047be69|2.2.0-beta|
|[chore][connector-common] Rename SeatunnelSchema to SeaTunnelSchema (#2538)|https://github.com/apache/seatunnel/commit/7dc2a2738|2.2.0-beta|
|[Feature][Connector-V2] Local file json support (#2465)|https://github.com/apache/seatunnel/commit/65a92f249|2.2.0-beta|
|[Feature][Connector-V2] Add local file connector source (#2419)|https://github.com/apache/seatunnel/commit/eff595c45|2.2.0-beta|
|[Improve][Connector-V2] Refactor the package of local file connector (#2403)|https://github.com/apache/seatunnel/commit/a538daed5|2.2.0-beta|
|[Feature][Connector-V2] Add json file sink &amp; json format (#2385)|https://github.com/apache/seatunnel/commit/dd68c06b0|2.2.0-beta|
|[Imporve][Connector-V2] Remove redundant type judge logic because of pr #2315 (#2370)|https://github.com/apache/seatunnel/commit/42e8c25e5|2.2.0-beta|
|[Feature][Connector-V2] Support orc file format in file connector (#2369)|https://github.com/apache/seatunnel/commit/f44fe1e03|2.2.0-beta|
|[improve][UT] Upgrade junit to 5.+ (#2305)|https://github.com/apache/seatunnel/commit/362319ff3|2.2.0-beta|
|[Connector-V2] Add parquet writer in file connector (#2273)|https://github.com/apache/seatunnel/commit/c95cc72cf|2.2.0-beta|
|[checkstyle] Improved validation scope of MagicNumber (#2194)|https://github.com/apache/seatunnel/commit/6d08b5f36|2.2.0-beta|
|[Connector-V2] Add Hive sink connector v2 (#2158)|https://github.com/apache/seatunnel/commit/23ad4ee73|2.2.0-beta|
|[Connector-V2] Add File Sink Connector (#2117)|https://github.com/apache/seatunnel/commit/e2283da64|2.2.0-beta|

</details>
