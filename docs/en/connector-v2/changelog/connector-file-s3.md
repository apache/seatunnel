<details><summary> Change Log </summary>

| Change | Commit | Version |
| --- | --- | --- |
|[Fix][Connector-V2] Fixed incorrectly setting s3 key in some cases (#8885)|https://github.com/apache/seatunnel/commit/cf4bab5be2| dev |
|[Feature][Connector-V2] Add `filename_extension` parameter for read/write file (#8769)|https://github.com/apache/seatunnel/commit/78b23c0ef5| dev |
|[Improve] restruct connector common options (#8634)|https://github.com/apache/seatunnel/commit/f3499a6eeb| dev |
| [improve] update S3File connector config option  (#8615)|https://github.com/apache/seatunnel/commit/80cc9fa6ff| dev |
|[Feature][Connector-V2] Support create emtpy file when no data (#8543)|https://github.com/apache/seatunnel/commit/275db78918| dev |
|[Feature][Connector-V2] Support single file mode in file sink (#8518)|https://github.com/apache/seatunnel/commit/e893deed50| dev |
|[Feature][File] Support config null format for text file read (#8109)|https://github.com/apache/seatunnel/commit/2dbf02df47|2.3.9|
|[Hotfix][Zeta] Fix the dependency conflict between the guava in hadoop-aws and hive-exec (#7986)|https://github.com/apache/seatunnel/commit/a7837f1f19|2.3.9|
|[Feature][Restapi] Allow metrics information to be associated to logical plan nodes (#7786)|https://github.com/apache/seatunnel/commit/6b7c53d03c|2.3.9|
|[Improve][Connector-V2] Support read archive compress file (#7633)|https://github.com/apache/seatunnel/commit/3f98cd8a16|2.3.8|
|[Improve] Refactor S3FileCatalog and it&#x27;s factory (#7457)|https://github.com/apache/seatunnel/commit/d928e8b113|2.3.8|
|[Improve][Connector] Add multi-table sink option check (#7360)|https://github.com/apache/seatunnel/commit/2489f6446b|2.3.7|
|[Feature][Core] Support using upstream table placeholders in sink options and auto replacement (#7131)|https://github.com/apache/seatunnel/commit/c4ca74122c|2.3.6|
|[Improve][Files] Support write fixed/timestamp as int96 of parquet (#6971)|https://github.com/apache/seatunnel/commit/1a48a9c493|2.3.6|
|[Feature][S3 File] Make S3 File Connector support multiple table write (#6698)|https://github.com/apache/seatunnel/commit/8f2049b2f1|2.3.6|
|[Improve][Connector-v2] The hive connector support multiple filesystem (#6648)|https://github.com/apache/seatunnel/commit/8a4c01fe35|2.3.6|
|[bigfix][S3 File]:Change the [SCHEMA] attribute of the [S3CONF class] to be non-static to avoid being reassigned after deserialization (#6717)|https://github.com/apache/seatunnel/commit/79bb70101a|2.3.6|
|[Fix][Connector-V2] Fix connector support SPI but without no args constructor (#6551)|https://github.com/apache/seatunnel/commit/5f3c9c36a5|2.3.5|
|Add support for XML file type to various file connectors such as SFTP, FTP, LocalFile, HdfsFile, and more. (#6327)|https://github.com/apache/seatunnel/commit/ec533ecd9a|2.3.5|
|[Test][E2E] Add thread leak check for connector (#5773)|https://github.com/apache/seatunnel/commit/1f2f3fc5f0|2.3.4|
|[Feature][Connector]add s3file save mode function (#6131)|https://github.com/apache/seatunnel/commit/81c51073bf|2.3.4|
|[Refactor][File Connector] Put Multiple Table File API to File Base Module (#6033)|https://github.com/apache/seatunnel/commit/c324d663b4|2.3.4|
|Support using multiple hadoop account (#5903)|https://github.com/apache/seatunnel/commit/d69d88d1aa|2.3.4|
|[Improve][Common] Introduce new error define rule (#5793)|https://github.com/apache/seatunnel/commit/9d1b2582b2|2.3.4|
|[Improve][connector-file] unifiy option between file source/sink and update document (#5680)|https://github.com/apache/seatunnel/commit/8d87cf8fc4|2.3.4|
|[Feature] Support `LZO` compress on File Read (#5083)|https://github.com/apache/seatunnel/commit/a4a1901096|2.3.4|
|[Feature][Connector-V2][File] Support read empty directory (#5591)|https://github.com/apache/seatunnel/commit/1f58f224a0|2.3.4|
|Support config column/primaryKey/constraintKey in schema (#5564)|https://github.com/apache/seatunnel/commit/eac76b4e50|2.3.4|
|[Feature] [File Connector]optionrule FILE_FORMAT_TYPE is text/csv ,add parameter BaseSinkConfig.ENABLE_HEADER_WRITE: #5566 (#5567)|https://github.com/apache/seatunnel/commit/0e02db768d|2.3.4|
|[Feature][Connector V2][File] Add config of &#x27;file_filter_pattern&#x27;, which used for filtering files. (#5153)|https://github.com/apache/seatunnel/commit/a3c13e59eb|2.3.3|
|[chore] delete unavailable S3 &amp; Kafka Catalogs (#4477)|https://github.com/apache/seatunnel/commit/e0aec5ecec|2.3.2|
| [Feature][ConnectorV2]add file excel sink and source (#4164)|https://github.com/apache/seatunnel/commit/e3b97ae5d2|2.3.2|
|Change file type to file_format_type in file source/sink (#4249)|https://github.com/apache/seatunnel/commit/973a2fae3c|2.3.1|
|[Chore] Upgrade guava to 27.0-jre (#4238)|https://github.com/apache/seatunnel/commit/4851bee575|2.3.1|
|Add redshift datatype convertor (#4245)|https://github.com/apache/seatunnel/commit/b19011517f|2.3.1|
|Merge branch &#x27;dev&#x27; into merge/cdc|https://github.com/apache/seatunnel/commit/4324ee1912|2.3.1|
|[Improve][Project] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/423b583038|2.3.1|
|[improve][api] Refactoring schema parse (#4157)|https://github.com/apache/seatunnel/commit/b2f573a13e|2.3.1|
|[Improve][build] Give the maven module a human readable name (#4114)|https://github.com/apache/seatunnel/commit/d7cd601051|2.3.1|
|Add S3Catalog (#4121)|https://github.com/apache/seatunnel/commit/7d7f506547|2.3.1|
|[Improve][Project] Code format with spotless plugin. (#4101)|https://github.com/apache/seatunnel/commit/a2ab166561|2.3.1|
|[Feature][Connector-V2][File] Support compress (#3899)|https://github.com/apache/seatunnel/commit/55602f6b1c|2.3.1|
|[Feature][Connector] add get source method to all source connector (#3846)|https://github.com/apache/seatunnel/commit/417178fb84|2.3.1|
|[Improve][Connector-V2][File] Improve file connector option rule and document (#3812)|https://github.com/apache/seatunnel/commit/bd76077669|2.3.1|
|[Feature][Shade] Add seatunnel hadoop3 uber (#3755)|https://github.com/apache/seatunnel/commit/5a024bdf8f|2.3.0|
|[Engine][Checkpoint]Unified naming style (#3714)|https://github.com/apache/seatunnel/commit/bc0bd3bec3|2.3.0|
|[Connector][File-S3]Set AK is not required (#3713)|https://github.com/apache/seatunnel/commit/da3c526172|2.3.0|
|[Connector&amp;Engine]Set S3 AK to optional (#3688)|https://github.com/apache/seatunnel/commit/4710918b02|2.3.0|
|[Connector][S3]Support s3a protocol (#3632)|https://github.com/apache/seatunnel/commit/ae4cc9c1ec|2.3.0|
|[Hotfix][OptionRule] Fix option rule about all connectors (#3592)|https://github.com/apache/seatunnel/commit/226dc6a119|2.3.0|
|[Improve][Connector-V2][File] Unified excetion for file source &amp; sink connectors (#3525)|https://github.com/apache/seatunnel/commit/031e8e263c|2.3.0|
|[Feature][Connector-V2][File] Add option and factory for file connectors (#3375)|https://github.com/apache/seatunnel/commit/db286e8631|2.3.0|
|[Improve][Connector-V2][File] Improve code structure (#3238)|https://github.com/apache/seatunnel/commit/dd5c353881|2.3.0|
|[Connector-V2] [ElasticSearch] Add ElasticSearch Source/Sink Factory (#3325)|https://github.com/apache/seatunnel/commit/38254e3f26|2.3.0|
|[Feature][Connector-V2][S3] Add S3 file source &amp; sink connector (#3119)|https://github.com/apache/seatunnel/commit/f27d68ca9c|2.3.0-beta|

</details>
