<details><summary>dev</summary>

| Change | Commit |
| --- | --- |
|[Feature][Connector-V2] Add `filename_extension` parameter for read/write file (#8769)|https://github.com/apache/seatunnel/commit/78b23c0ef5|
|[Improve] restruct connector common options (#8634)|https://github.com/apache/seatunnel/commit/f3499a6eeb|
|[Hotfix][Connector-V2][SFTP] Add quote to sftp file names with wildcard characters (#8501)|https://github.com/apache/seatunnel/commit/c5751b001b|
|[Feature][Connector-V2] Support create emtpy file when no data (#8543)|https://github.com/apache/seatunnel/commit/275db78918|
|[Feature][Connector-V2] Support single file mode in file sink (#8518)|https://github.com/apache/seatunnel/commit/e893deed50|

</details>
<details><summary>2.3.9</summary>

| Change | Commit |
| --- | --- |
|[Improve][Connector-V2] Add some debug log when create dir in (S)FTP (#8286)|https://github.com/apache/seatunnel/commit/8687bb8e91|
|[Feature][File] Support config null format for text file read (#8109)|https://github.com/apache/seatunnel/commit/2dbf02df47|
|[Feature][Connector-V2]Sftp file source support multiple table (#7824)|https://github.com/apache/seatunnel/commit/cfb8760f58|
|[Feature][Restapi] Allow metrics information to be associated to logical plan nodes (#7786)|https://github.com/apache/seatunnel/commit/6b7c53d03c|

</details>
<details><summary>2.3.8</summary>

| Change | Commit |
| --- | --- |
|[Feature][Connector-V2] sftp file sink suport multiple table and save mode (#7668)|https://github.com/apache/seatunnel/commit/dc4b9898f7|
|[Improve][Connector-V2] Support read archive compress file (#7633)|https://github.com/apache/seatunnel/commit/3f98cd8a16|

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
|[BugFix][Connector-file-sftp] Fix SFTPInputStream.close does not correctly trigger the closing of the file stream (#6323) (#6329)|https://github.com/apache/seatunnel/commit/eee881af91|

</details>
<details><summary>2.3.4</summary>

| Change | Commit |
| --- | --- |
|[Test][E2E] Add thread leak check for connector (#5773)|https://github.com/apache/seatunnel/commit/1f2f3fc5f0|
|[Refactor][File Connector] Put Multiple Table File API to File Base Module (#6033)|https://github.com/apache/seatunnel/commit/c324d663b4|
|Support using multiple hadoop account (#5903)|https://github.com/apache/seatunnel/commit/d69d88d1aa|
|[Improve][Common] Introduce new error define rule (#5793)|https://github.com/apache/seatunnel/commit/9d1b2582b2|
|[Improve][connector-file] unifiy option between file source/sink and update document (#5680)|https://github.com/apache/seatunnel/commit/8d87cf8fc4|
|[Feature] Support `LZO` compress on File Read (#5083)|https://github.com/apache/seatunnel/commit/a4a1901096|
|[Feature][Connector-V2][File] Support read empty directory (#5591)|https://github.com/apache/seatunnel/commit/1f58f224a0|
|Support config column/primaryKey/constraintKey in schema (#5564)|https://github.com/apache/seatunnel/commit/eac76b4e50|
|[Feature] [File Connector]optionrule FILE_FORMAT_TYPE is text/csv ,add parameter BaseSinkConfig.ENABLE_HEADER_WRITE: #5566 (#5567)|https://github.com/apache/seatunnel/commit/0e02db768d|
|[Improve][CheckStyle] Remove useless &#x27;SuppressWarnings&#x27; annotation of checkstyle. (#5260)|https://github.com/apache/seatunnel/commit/51c0d709ba|

</details>
<details><summary>2.3.3</summary>

| Change | Commit |
| --- | --- |
|[Feature][Connector V2][File] Add config of &#x27;file_filter_pattern&#x27;, which used for filtering files. (#5153)|https://github.com/apache/seatunnel/commit/a3c13e59eb|

</details>
<details><summary>2.3.2</summary>

| Change | Commit |
| --- | --- |
|[Bug Fix] [seatunnel-connectors-v2][SFTP] Fix incorrect exception handling logic (#4720)|https://github.com/apache/seatunnel/commit/dc350e67c3|
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
|[Feature][Shade] Add seatunnel hadoop3 uber (#3755)|https://github.com/apache/seatunnel/commit/5a024bdf8f|
|[Hotfix][OptionRule] Fix option rule about all connectors (#3592)|https://github.com/apache/seatunnel/commit/226dc6a119|
|[Improve][Connector-V2][File] Unified excetion for file source &amp; sink connectors (#3525)|https://github.com/apache/seatunnel/commit/031e8e263c|
|[Feature][Connector-V2][File] Add option and factory for file connectors (#3375)|https://github.com/apache/seatunnel/commit/db286e8631|
|[Improve][Connector-V2][File] Improve code structure (#3238)|https://github.com/apache/seatunnel/commit/dd5c353881|
|[Connector-V2] [ElasticSearch] Add ElasticSearch Source/Sink Factory (#3325)|https://github.com/apache/seatunnel/commit/38254e3f26|
|[Feature][Connector-V2][SFTP] Add SFTP file source &amp; sink connector (#3006)|https://github.com/apache/seatunnel/commit/9e496383b8|

</details>
