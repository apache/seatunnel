<details><summary> Change Log </summary>

| Change | Commit | Version |
| --- | --- | --- |
|[Improve] iceberg options (#8967)|https://github.com/apache/seatunnel/commit/82a374ec8|2.3.10|
|[Improve] restruct connector common options (#8634)|https://github.com/apache/seatunnel/commit/f3499a6ee|2.3.10|
|[Feature][Iceberg] Support read multi-table (#8524)|https://github.com/apache/seatunnel/commit/2bfb97e50|2.3.10|
|[Improve][Iceberg] Filter catalog table primaryKey is empty (#8413)|https://github.com/apache/seatunnel/commit/857aab5e8|2.3.9|
|[Improve][Connector-V2] Reduce the create times of iceberg sink writer (#8155)|https://github.com/apache/seatunnel/commit/45a7a715a|2.3.9|
|[Improve][dist]add shade check rule (#8136)|https://github.com/apache/seatunnel/commit/51ef80001|2.3.9|
|[Feature][Iceberg] Support custom delete sql for sink savemode (#8094)|https://github.com/apache/seatunnel/commit/29ca928c3|2.3.9|
|[Improve][Connector-V2] Reduce the request times of iceberg load table (#8149)|https://github.com/apache/seatunnel/commit/555f5eb40|2.3.9|
|[Feature][Core] Support cdc task ddl restore for zeta (#7463)|https://github.com/apache/seatunnel/commit/8e322281e|2.3.9|
|[Improve][Iceberg] Support table comment for catalog (#7936)|https://github.com/apache/seatunnel/commit/72ab38f31|2.3.9|
|[Feature][Restapi] Allow metrics information to be associated to logical plan nodes (#7786)|https://github.com/apache/seatunnel/commit/6b7c53d03|2.3.9|
|[Fix][Connector-V2] Fix iceberg throw java: package sun.security.krb5 does not exist when use jdk 11 (#7734)|https://github.com/apache/seatunnel/commit/116af4feb|2.3.8|
|[Hotfix][Connector-V2] Release resources when task is closed for iceberg sinkwriter (#7729)|https://github.com/apache/seatunnel/commit/ff281183b|2.3.8|
|[Fix][Connector-V2] Fixed iceberg sink can not handle uppercase fields (#7660)|https://github.com/apache/seatunnel/commit/b7be0cb4a|2.3.8|
|[Hotfix][CDC] Fix ddl duplicate execution error when config multi_table_sink_replica (#7634)|https://github.com/apache/seatunnel/commit/23ab3edbb|2.3.8|
|[Improve][Iceberg] Add savemode create table primaryKey testcase (#7641)|https://github.com/apache/seatunnel/commit/6b36f90f4|2.3.8|
|[Hotfix] Fix iceberg missing column comment when savemode create table (#7608)|https://github.com/apache/seatunnel/commit/b35bd94bf|2.3.8|
|[Improve][Connector-V2] Remove hard code iceberg table format version (#7500)|https://github.com/apache/seatunnel/commit/f49b263e6|2.3.8|
|[Improve][API] Move catalog open to SaveModeHandler (#7439)|https://github.com/apache/seatunnel/commit/8c2c5c79a|2.3.8|
|[Feature][Connector-V2][Iceberg] Support Iceberg Kerberos (#7246)|https://github.com/apache/seatunnel/commit/e3001207c|2.3.8|
|[Improve][Connector] Add multi-table sink option check (#7360)|https://github.com/apache/seatunnel/commit/2489f6446|2.3.7|
|[Feature][Core] Support using upstream table placeholders in sink options and auto replacement (#7131)|https://github.com/apache/seatunnel/commit/c4ca74122|2.3.6|
|[Bug][Connector-Iceberg]fix create iceberg v2 table with pks (#6895)|https://github.com/apache/seatunnel/commit/40d2c1b21|2.3.6|
|[Feature][Connector-V2] Iceberg-sink supports writing data to branches (#6697)|https://github.com/apache/seatunnel/commit/e3103535c|2.3.6|
|[Fix][Connector-V2] Fix connector support SPI but without no args constructor (#6551)|https://github.com/apache/seatunnel/commit/5f3c9c36a|2.3.5|
|[Improve] Add SaveMode log of process detail (#6375)|https://github.com/apache/seatunnel/commit/b0d70ce22|2.3.5|
|[Improve][Zeta] Add classloader cache mode to fix metaspace leak (#6355)|https://github.com/apache/seatunnel/commit/9c3c2f183|2.3.5|
|[Improve][API] Unify type system api(data &amp; type) (#5872)|https://github.com/apache/seatunnel/commit/b38c7edcc|2.3.5|
|[Feature] Supports iceberg sink #6198 (#6265)|https://github.com/apache/seatunnel/commit/18d3e8619|2.3.5|
|[Test][E2E] Add thread leak check for connector (#5773)|https://github.com/apache/seatunnel/commit/1f2f3fc5f|2.3.4|
|[Improve][Common] Introduce new error define rule (#5793)|https://github.com/apache/seatunnel/commit/9d1b2582b|2.3.4|
|[BUG][Connector-V2] Iceberg source lost data with parallelism option (#5732)|https://github.com/apache/seatunnel/commit/7f3b4be07|2.3.4|
|[Dependency]Bump org.apache.avro:avro in /seatunnel-connectors-v2/connector-iceberg (#5582)|https://github.com/apache/seatunnel/commit/13753a927|2.3.4|
|[Improve][Pom] Add junit4 to the root pom (#5611)|https://github.com/apache/seatunnel/commit/7b4f7db2a|2.3.4|
|Support config column/primaryKey/constraintKey in schema (#5564)|https://github.com/apache/seatunnel/commit/eac76b4e5|2.3.4|
|[Doc][Iceberg] Improved iceberg documentation (#5335)|https://github.com/apache/seatunnel/commit/659a68a0b|2.3.4|
|[Hotfix] Fix com.google.common.base.Preconditions to seatunnel shade one (#5284)|https://github.com/apache/seatunnel/commit/ed5eadcf7|2.3.3|
|[Hotfix][Connector][Iceberg] Fix iceberg source stream mode init error (#4638)|https://github.com/apache/seatunnel/commit/64760eed4|2.3.2|
|Merge branch &#x27;dev&#x27; into merge/cdc|https://github.com/apache/seatunnel/commit/4324ee191|2.3.1|
|[Improve][Project] Code format with spotless plugin.|https://github.com/apache/seatunnel/commit/423b58303|2.3.1|
|[Improve][SourceConnector] Unifie Iceberg source fields to schema (#3959)|https://github.com/apache/seatunnel/commit/20e1255fa|2.3.1|
|[improve][api] Refactoring schema parse (#4157)|https://github.com/apache/seatunnel/commit/b2f573a13|2.3.1|
|[Improve][build] Give the maven module a human readable name (#4114)|https://github.com/apache/seatunnel/commit/d7cd60105|2.3.1|
|[Improve][Project] Code format with spotless plugin. (#4101)|https://github.com/apache/seatunnel/commit/a2ab16656|2.3.1|
|[Improve][Connector-V2][Iceberg] Unified exception for iceberg source connector (#3677)|https://github.com/apache/seatunnel/commit/e24843515|2.3.1|
|[Feature][Connector] add get source method to all source connector (#3846)|https://github.com/apache/seatunnel/commit/417178fb8|2.3.1|
|[Feature][API &amp; Connector &amp; Doc] add parallelism and column projection interface (#3829)|https://github.com/apache/seatunnel/commit/b9164b8ba|2.3.1|
|[Hotfix][OptionRule] Fix option rule about all connectors (#3592)|https://github.com/apache/seatunnel/commit/226dc6a11|2.3.0|
|[Feature][Connector-V2][Iceberg] Modify the scope of flink-shaded-hadoop-2 to provided to be compatible with hadoop3.x (#3046)|https://github.com/apache/seatunnel/commit/b38c50789|2.3.0|
|[Feature][Connector V2] expose configurable options in Iceberg (#3394)|https://github.com/apache/seatunnel/commit/bd9a313de|2.3.0|
|[Improve][Connector][Iceberg] Improve code. (#3065)|https://github.com/apache/seatunnel/commit/9f38e3da7|2.3.0-beta|
|[Code-Improve][Iceberg] Use automatic resource management to replace &#x27;try - finally&#x27; code block. (#2909)|https://github.com/apache/seatunnel/commit/b7f640724|2.3.0-beta|
|[Feature][Connector-V2] Add iceberg source connector (#2615)|https://github.com/apache/seatunnel/commit/ffc6088a7|2.2.0-beta|

</details>
