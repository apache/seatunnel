<details><summary> Change Log </summary>

| Change | Commit | Version |
| --- | --- | --- |
|[Feature][Connector-V2] Add `filename_extension` parameter for read/write file (#8769)|https://github.com/apache/seatunnel/commit/78b23c0ef5| dev |
|[Improve] restruct connector common options (#8634)|https://github.com/apache/seatunnel/commit/f3499a6eeb| dev |
|[Feature][Connector-V2] Support create emtpy file when no data (#8543)|https://github.com/apache/seatunnel/commit/275db78918| dev |
|[Feature][Connector-V2] Support single file mode in file sink (#8518)|https://github.com/apache/seatunnel/commit/e893deed50| dev |
|[Feature][File] Support config null format for text file read (#8109)|https://github.com/apache/seatunnel/commit/2dbf02df47|2.3.9|
|[Feature][Restapi] Allow metrics information to be associated to logical plan nodes (#7786)|https://github.com/apache/seatunnel/commit/6b7c53d03c|2.3.9|
|[Improve][Connector-V2] Support read archive compress file (#7633)|https://github.com/apache/seatunnel/commit/3f98cd8a16|2.3.8|
|[Improve][Files] Support write fixed/timestamp as int96 of parquet (#6971)|https://github.com/apache/seatunnel/commit/1a48a9c493|2.3.6|
|Add support for XML file type to various file connectors such as SFTP, FTP, LocalFile, HdfsFile, and more. (#6327)|https://github.com/apache/seatunnel/commit/ec533ecd9a|2.3.5|
|[Refactor][File Connector] Put Multiple Table File API to File Base Module (#6033)|https://github.com/apache/seatunnel/commit/c324d663b4|2.3.4|
|[Improve][connector-file] unifiy option between file source/sink and update document (#5680)|https://github.com/apache/seatunnel/commit/8d87cf8fc4|2.3.4|
|[Feature] Support `LZO` compress on File Read (#5083)|https://github.com/apache/seatunnel/commit/a4a1901096|2.3.4|
|Support config column/primaryKey/constraintKey in schema (#5564)|https://github.com/apache/seatunnel/commit/eac76b4e50|2.3.4|
|[Feature] [File Connector]optionrule FILE_FORMAT_TYPE is text/csv ,add parameter BaseSinkConfig.ENABLE_HEADER_WRITE: #5566 (#5567)|https://github.com/apache/seatunnel/commit/0e02db768d|2.3.4|
|[Feature][Connector V2][File] Add config of &#x27;file_filter_pattern&#x27;, which used for filtering files. (#5153)|https://github.com/apache/seatunnel/commit/a3c13e59eb|2.3.3|
| [Feature][ConnectorV2]add file excel sink and source (#4164)|https://github.com/apache/seatunnel/commit/e3b97ae5d2|2.3.2|
|Change file type to file_format_type in file source/sink (#4249)|https://github.com/apache/seatunnel/commit/973a2fae3c|2.3.1|
|Merge branch &#x27;dev&#x27; into merge/cdc|https://github.com/apache/seatunnel/commit/4324ee1912|2.3.1|
|[Improve][Project] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/423b583038|2.3.1|
|[improve][api] Refactoring schema parse (#4157)|https://github.com/apache/seatunnel/commit/b2f573a13e|2.3.1|
|[Improve][build] Give the maven module a human readable name (#4114)|https://github.com/apache/seatunnel/commit/d7cd601051|2.3.1|
|[Improve][Project] Code format with spotless plugin. (#4101)|https://github.com/apache/seatunnel/commit/a2ab166561|2.3.1|
|[Feature][Connector-V2][File] Support compress (#3899)|https://github.com/apache/seatunnel/commit/55602f6b1c|2.3.1|
|[Feature][Connector] add get source method to all source connector (#3846)|https://github.com/apache/seatunnel/commit/417178fb84|2.3.1|
|[Improve][Connector-V2][File] Improve file connector option rule and document (#3812)|https://github.com/apache/seatunnel/commit/bd76077669|2.3.1|
|[Feature][Shade] Add seatunnel hadoop3 uber (#3755)|https://github.com/apache/seatunnel/commit/5a024bdf8f|2.3.0|
|[Hotfix][OptionRule] Fix option rule about all connectors (#3592)|https://github.com/apache/seatunnel/commit/226dc6a119|2.3.0|
|[Feature][Connector-V2][File] Add option and factory for file connectors (#3375)|https://github.com/apache/seatunnel/commit/db286e8631|2.3.0|
|[Improve][Connector] Improve write parquet (#2943)|https://github.com/apache/seatunnel/commit/8fd966394b|2.3.0-beta|
|[Fix][Connector-V2] Fix HiveSource Connector read orc table error (#2845)|https://github.com/apache/seatunnel/commit/61720306e7|2.2.0-beta|
|[Improve][Connector-V2] Improve read parquet (#2841)|https://github.com/apache/seatunnel/commit/e19bc82f9b|2.2.0-beta|
|[Improve][Connector-V2] Refactor hdfs file sink connector code structure (#2701)|https://github.com/apache/seatunnel/commit/6129c02567|2.2.0-beta|
|[#2606]Dependency management split (#2630)|https://github.com/apache/seatunnel/commit/fc047be69b|2.2.0-beta|
|[chore][connector-common] Rename SeatunnelSchema to SeaTunnelSchema (#2538)|https://github.com/apache/seatunnel/commit/7dc2a27388|2.2.0-beta|
|[Feature][Connector-V2] Add hdfs file json support (#2451)|https://github.com/apache/seatunnel/commit/84f6b17c15|2.2.0-beta|
|[Improve][Connector-V2] Refactor the package of hdfs file connector (#2402)|https://github.com/apache/seatunnel/commit/87d0624c5b|2.2.0-beta|
|[Feature][Connector-V2] Add hdfs file source connector (#2420)|https://github.com/apache/seatunnel/commit/4fb6f2a216|2.2.0-beta|
|[Feature][Connector-V2] Add json file sink &amp; json format (#2385)|https://github.com/apache/seatunnel/commit/dd68c06b0a|2.2.0-beta|
|[Imporve][Connector-V2] Remove redundant type judge logic because of pr #2315 (#2370)|https://github.com/apache/seatunnel/commit/42e8c25e50|2.2.0-beta|
|[Feature][Connector-V2] Support orc file format in file connector (#2369)|https://github.com/apache/seatunnel/commit/f44fe1e033|2.2.0-beta|
|[improve][UT] Upgrade junit to 5.+ (#2305)|https://github.com/apache/seatunnel/commit/362319ff3e|2.2.0-beta|
|[Connector-V2] Add parquet writer in file connector (#2273)|https://github.com/apache/seatunnel/commit/c95cc72cfa|2.2.0-beta|
|[checkstyle] Improved validation scope of MagicNumber (#2194)|https://github.com/apache/seatunnel/commit/6d08b5f369|2.2.0-beta|
|[Connector-V2] Add Hive sink connector v2 (#2158)|https://github.com/apache/seatunnel/commit/23ad4ee735|2.2.0-beta|
|[Connector-V2] Add File Sink Connector (#2117)|https://github.com/apache/seatunnel/commit/e2283da64f|2.2.0-beta|

</details>
