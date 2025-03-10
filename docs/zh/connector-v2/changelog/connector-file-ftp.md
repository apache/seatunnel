<details><summary>dev</summary>

| Change | Commit |
| --- | --- |
|[Feature][Connector-V2] Add `filename_extension` parameter for read/write file (#8769)|https://github.com/apache/seatunnel/commit/78b23c0ef5|
|[Improve] restruct connector common options (#8634)|https://github.com/apache/seatunnel/commit/f3499a6eeb|
|[Feature][Connector-V2] Support create emtpy file when no data (#8543)|https://github.com/apache/seatunnel/commit/275db78918|
|[Feature][Connector-V2] Support single file mode in file sink (#8518)|https://github.com/apache/seatunnel/commit/e893deed50|

</details>
<details><summary>2.3.9</summary>

| Change | Commit |
| --- | --- |
|[Improve][Connector-V2] Add some debug log when create dir in (S)FTP (#8286)|https://github.com/apache/seatunnel/commit/8687bb8e91|
|[Feature][File] Support config null format for text file read (#8109)|https://github.com/apache/seatunnel/commit/2dbf02df47|
|[Fix][Connector-V2][FTP] Fix FTP connector connection_mode is not effective (#7865)|https://github.com/apache/seatunnel/commit/26c528a5ed|
|[Feature][Restapi] Allow metrics information to be associated to logical plan nodes (#7786)|https://github.com/apache/seatunnel/commit/6b7c53d03c|
|[Feature][Connector-V2]Ftp file source support multiple table (#7795)|https://github.com/apache/seatunnel/commit/22fe27a3d6|

</details>
<details><summary>2.3.8</summary>

| Change | Commit |
| --- | --- |
|[Improve][Connector-V2] Support read archive compress file (#7633)|https://github.com/apache/seatunnel/commit/3f98cd8a16|
|[Feature][Connector-V2] Ftp file sink suport multiple table and save mode (#7665)|https://github.com/apache/seatunnel/commit/4f812e12ae|

</details>
<details><summary>2.3.6</summary>

| Change | Commit |
| --- | --- |
|[Improve][Files] Support write fixed/timestamp as int96 of parquet (#6971)|https://github.com/apache/seatunnel/commit/1a48a9c493|
|[Feature][Connector-V2] Supports the transfer of any file (#6826)|https://github.com/apache/seatunnel/commit/c1401787b3|

</details>
<details><summary>2.3.5</summary>

| Change | Commit |
| --- | --- |
|Add support for XML file type to various file connectors such as SFTP, FTP, LocalFile, HdfsFile, and more. (#6327)|https://github.com/apache/seatunnel/commit/ec533ecd9a|

</details>
<details><summary>2.3.4</summary>

| Change | Commit |
| --- | --- |
|[Feature][Connectors-v2-file-ftp] FTP source/sink add ftp connection mode (#6077)  (#6099)|https://github.com/apache/seatunnel/commit/f6bcc4d59d|
|[Refactor][File Connector] Put Multiple Table File API to File Base Module (#6033)|https://github.com/apache/seatunnel/commit/c324d663b4|
|Support using multiple hadoop account (#5903)|https://github.com/apache/seatunnel/commit/d69d88d1aa|
|[Improve][Common] Introduce new error define rule (#5793)|https://github.com/apache/seatunnel/commit/9d1b2582b2|
|[Improve][connector-file] unifiy option between file source/sink and update document (#5680)|https://github.com/apache/seatunnel/commit/8d87cf8fc4|
|[Feature] Support `LZO` compress on File Read (#5083)|https://github.com/apache/seatunnel/commit/a4a1901096|
|[Feature][Connector-V2][File] Support read empty directory (#5591)|https://github.com/apache/seatunnel/commit/1f58f224a0|
|Support config column/primaryKey/constraintKey in schema (#5564)|https://github.com/apache/seatunnel/commit/eac76b4e50|
|[Feature] [File Connector]optionrule FILE_FORMAT_TYPE is text/csv ,add parameter BaseSinkConfig.ENABLE_HEADER_WRITE: #5566 (#5567)|https://github.com/apache/seatunnel/commit/0e02db768d|

</details>
<details><summary>2.3.3</summary>

| Change | Commit |
| --- | --- |
|[Feature][Connector V2][File] Add config of 'file_filter_pattern', which used for filtering files. (#5153)|https://github.com/apache/seatunnel/commit/a3c13e59eb|

</details>
<details><summary>2.3.2</summary>

| Change | Commit |
| --- | --- |
| [Feature][ConnectorV2]add file excel sink and source (#4164)|https://github.com/apache/seatunnel/commit/e3b97ae5d2|

</details>
<details><summary>2.3.1</summary>

| Change | Commit |
| --- | --- |
|Change file type to file_format_type in file source/sink (#4249)|https://github.com/apache/seatunnel/commit/973a2fae3c|
|Merge branch 'dev' into merge/cdc|https://github.com/apache/seatunnel/commit/4324ee1912|
|[Improve][Project] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/423b583038|
|[improve][api] Refactoring schema parse (#4157)|https://github.com/apache/seatunnel/commit/b2f573a13e|
|[Improve][build] Give the maven module a human readable name (#4114)|https://github.com/apache/seatunnel/commit/d7cd601051|
|[Improve][Project] Code format with spotless plugin. (#4101)|https://github.com/apache/seatunnel/commit/a2ab166561|
|[Feature][Connector-V2][File] Support compress (#3899)|https://github.com/apache/seatunnel/commit/55602f6b1c|
|[Feature][Connector] add get source method to all source connector (#3846)|https://github.com/apache/seatunnel/commit/417178fb84|
|[Improve][Connector-V2][File] Improve file connector option rule and document (#3812)|https://github.com/apache/seatunnel/commit/bd76077669|

</details>
<details><summary>2.3.0</summary>

| Change | Commit |
| --- | --- |
|[Feature][Shade] Add seatunnel hadoop3 uber (#3755)|https://github.com/apache/seatunnel/commit/5a024bdf8f|
|[Hotfix][OptionRule] Fix option rule about all connectors (#3592)|https://github.com/apache/seatunnel/commit/226dc6a119|
|[Improve][Connector-V2][File] Unified excetion for file source & sink connectors (#3525)|https://github.com/apache/seatunnel/commit/031e8e263c|
|[Feature][Connector-V2][File] Add option and factory for file connectors (#3375)|https://github.com/apache/seatunnel/commit/db286e8631|
|[Improve][Connector-V2][File] Improve code structure (#3238)|https://github.com/apache/seatunnel/commit/dd5c353881|
|[Connector-V2] [ElasticSearch] Add ElasticSearch Source/Sink Factory (#3325)|https://github.com/apache/seatunnel/commit/38254e3f26|
|[Core] [Improve] Fix some sonar check error (#3240)|https://github.com/apache/seatunnel/commit/8664bb53a5|

</details>
<details><summary>2.3.0-beta</summary>

| Change | Commit |
| --- | --- |
|[Improve][Connector-V2][File] Support parse field from file path (#2985)|https://github.com/apache/seatunnel/commit/0bc12085c2|
|[Improve][connector][file] Support user-defined schema for reading text file (#2976)|https://github.com/apache/seatunnel/commit/1c05ee0d7e|
|[Improve][Connector] Improve write parquet (#2943)|https://github.com/apache/seatunnel/commit/8fd966394b|

</details>
<details><summary>2.2.0-beta</summary>

| Change | Commit |
| --- | --- |
|[Fix][Connector-V2] Fix HiveSource Connector read orc table error (#2845)|https://github.com/apache/seatunnel/commit/61720306e7|
|[Improve][Connector-V2] Improve read parquet (#2841)|https://github.com/apache/seatunnel/commit/e19bc82f9b|
|[Imporve][Connector-V2] Refactor ftp sink & Add ftp file source (#2774)|https://github.com/apache/seatunnel/commit/4aacbcdd1f|
|[Feature][File connector] Support ftp file sink (#2483)|https://github.com/apache/seatunnel/commit/a87e5de80a|

</details>
