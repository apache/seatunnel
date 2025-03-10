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
|[Feature][File] Support config null format for text file read (#8109)|https://github.com/apache/seatunnel/commit/2dbf02df47|
|[Improve][API] Unified tables_configs and table_list (#8100)|https://github.com/apache/seatunnel/commit/84c0b8d660|
|[Feature][Restapi] Allow metrics information to be associated to logical plan nodes (#7786)|https://github.com/apache/seatunnel/commit/6b7c53d03c|

</details>
<details><summary>2.3.8</summary>

| Change | Commit |
| --- | --- |
|[Improve][Connector-V2] Support read archive compress file (#7633)|https://github.com/apache/seatunnel/commit/3f98cd8a16|
|[Improve] Added OSSFileCatalog and it&#x27;s factory (#7458)|https://github.com/apache/seatunnel/commit/9006a205db|

</details>
<details><summary>2.3.7</summary>

| Change | Commit |
| --- | --- |
|[Improve][Connector] Add multi-table sink option check (#7360)|https://github.com/apache/seatunnel/commit/2489f6446b|

</details>
<details><summary>2.3.6</summary>

| Change | Commit |
| --- | --- |
|[Feature][Core] Support using upstream table placeholders in sink options and auto replacement (#7131)|https://github.com/apache/seatunnel/commit/c4ca74122c|
|[Improve][Files] Support write fixed/timestamp as int96 of parquet (#6971)|https://github.com/apache/seatunnel/commit/1a48a9c493|

</details>
<details><summary>2.3.5</summary>

| Change | Commit |
| --- | --- |
|[Chore] Fix `file` spell errors (#6606)|https://github.com/apache/seatunnel/commit/2599d3b736|
|[Fix][Connector-V2] Fix connector support SPI but without no args constructor (#6551)|https://github.com/apache/seatunnel/commit/5f3c9c36a5|
|Add support for XML file type to various file connectors such as SFTP, FTP, LocalFile, HdfsFile, and more. (#6327)|https://github.com/apache/seatunnel/commit/ec533ecd9a|

</details>
<details><summary>2.3.4</summary>

| Change | Commit |
| --- | --- |
|[Feature][OssFile Connector] Make Oss implement source factory and sink factory (#6062)|https://github.com/apache/seatunnel/commit/1a8e9b4554|
|[Refactor][File Connector] Put Multiple Table File API to File Base Module (#6033)|https://github.com/apache/seatunnel/commit/c324d663b4|
|[Hotfix][Oss File Connector] fix oss connector can not run bug (#6010)|https://github.com/apache/seatunnel/commit/755bc2a730|
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
|[Feature][Connector V2][File] Add config of &#x27;file_filter_pattern&#x27;, which used for filtering files. (#5153)|https://github.com/apache/seatunnel/commit/a3c13e59eb|

</details>
<details><summary>2.3.2</summary>

| Change | Commit |
| --- | --- |
|[Fix][Connector-V2] Fix file-oss config check bug and amend file-oss-jindo factoryIdentifier (#4581)|https://github.com/apache/seatunnel/commit/5c4f17df20|
| [Feature][ConnectorV2]add file excel sink and source (#4164)|https://github.com/apache/seatunnel/commit/e3b97ae5d2|

</details>
<details><summary>2.3.1</summary>

| Change | Commit |
| --- | --- |
|Change file type to file_format_type in file source/sink (#4249)|https://github.com/apache/seatunnel/commit/973a2fae3c|
|Merge branch &#x27;dev&#x27; into merge/cdc|https://github.com/apache/seatunnel/commit/4324ee1912|
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
|[Hotfix][OptionRule] Fix option rule about all connectors (#3592)|https://github.com/apache/seatunnel/commit/226dc6a119|
|[Improve][Connector-V2][File] Unified excetion for file source &amp; sink connectors (#3525)|https://github.com/apache/seatunnel/commit/031e8e263c|
|[Feature][Connector-V2][File] Add option and factory for file connectors (#3375)|https://github.com/apache/seatunnel/commit/db286e8631|
|[Improve][Connector-V2][File] Improve code structure (#3238)|https://github.com/apache/seatunnel/commit/dd5c353881|
|[Connector-V2] [ElasticSearch] Add ElasticSearch Source/Sink Factory (#3325)|https://github.com/apache/seatunnel/commit/38254e3f26|

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
|[Feature][Connector-V2] Add oss sink (#2629)|https://github.com/apache/seatunnel/commit/bb2ad40487|
|[#2606]Dependency management split (#2630)|https://github.com/apache/seatunnel/commit/fc047be69b|
|[chore][connector-common] Rename SeatunnelSchema to SeaTunnelSchema (#2538)|https://github.com/apache/seatunnel/commit/7dc2a27388|
|[Feature][Connector-V2] Add oss source connector (#2467)|https://github.com/apache/seatunnel/commit/712b77744e|

</details>
