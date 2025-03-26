<details><summary> Change Log </summary>

| Change | Commit | Version |
| --- | --- | --- |
|[Fix][Paimon] nullable and comment attribute was lost during automatic table creation (#9020)|https://github.com/apache/seatunnel/commit/eb54fdd52| dev |
|[Feature][Connector-V2] Support between predicate pushdown in paimon (#8962)|https://github.com/apache/seatunnel/commit/3b141cf62|2.3.10|
|[Feature][Connector-V2] Suppor Time type in paimon connector (#8880)|https://github.com/apache/seatunnel/commit/9f1e59009|2.3.10|
|[Feature][Paimon] Customize the hadoop user  (#8888)|https://github.com/apache/seatunnel/commit/2657626f9|2.3.10|
|[Improve][Connector-v2][Paimon]PaimonCatalog close error message update (#8640)|https://github.com/apache/seatunnel/commit/48253da8d|2.3.10|
|[Improve] restruct connector common options (#8634)|https://github.com/apache/seatunnel/commit/f3499a6ee|2.3.10|
|[Improve][Connector-v2] Support checkpoint in batch mode for paimon sink (#8333)|https://github.com/apache/seatunnel/commit/f22d4ebd4|2.3.9|
|[Feature][Connector-v2] Support schema evolution for paimon sink (#8211)|https://github.com/apache/seatunnel/commit/57190e2a3|2.3.9|
|[Improve][dist]add shade check rule (#8136)|https://github.com/apache/seatunnel/commit/51ef80001|2.3.9|
|[Feature][Connector-v2] Support S3 filesystem of paimon connector (#8036)|https://github.com/apache/seatunnel/commit/e2a477293|2.3.9|
|[Feature][transform] transform support explode (#7928)|https://github.com/apache/seatunnel/commit/132278c06|2.3.9|
|[Feature][Connector-V2] Piamon Sink supports changelog-procuder is lookup and full-compaction mode (#7834)|https://github.com/apache/seatunnel/commit/c0f27c2f7|2.3.9|
|[Fix][connector-v2]Fix Paimon table connector  Error log information. (#7873)|https://github.com/apache/seatunnel/commit/a3b49e635|2.3.9|
|[Improve][Connector-v2] Use checkpointId as the commit&#x27;s identifier instead of the hash for streaming write of paimon sink (#7835)|https://github.com/apache/seatunnel/commit/c7a384af2|2.3.9|
|[Feature][Restapi] Allow metrics information to be associated to logical plan nodes (#7786)|https://github.com/apache/seatunnel/commit/6b7c53d03|2.3.9|
|[Fix][Connecotr-V2] Fix paimon dynamic bucket tale in primary key is not first (#7728)|https://github.com/apache/seatunnel/commit/dc7f69553|2.3.8|
|[Improve][Connector-v2] Remove useless code and add changelog doc for paimon sink (#7748)|https://github.com/apache/seatunnel/commit/846d876dc|2.3.8|
|[Hotfix][Connector-V2] Release resources even the task is crashed for paimon sink (#7726)|https://github.com/apache/seatunnel/commit/5ddf8d461|2.3.8|
|[Fix][Connector-V2] Fix paimon e2e error (#7721)|https://github.com/apache/seatunnel/commit/61d196436|2.3.8|
|[Feature][Connector-Paimon] Support dynamic bucket splitting improves Paimon writing efficiency (#7335)|https://github.com/apache/seatunnel/commit/bc0326cba|2.3.8|
|[Feature][Connector-v2] Support streaming read for paimon (#7681)|https://github.com/apache/seatunnel/commit/4a2e27291|2.3.8|
|[Hotfix][Seatunnel-common] Fix the CommonError msg for paimon sink (#7591)|https://github.com/apache/seatunnel/commit/d1f5db925|2.3.8|
|[Feature][CONNECTORS-V2-Paimon] Paimon Sink supported truncate table (#7560)|https://github.com/apache/seatunnel/commit/4f3df2212|2.3.8|
|[Improve][Connector-v2] Improve the exception msg in case-sensitive case for paimon sink (#7549)|https://github.com/apache/seatunnel/commit/7d31e5668|2.3.8|
|[Hotfix][Connector-V2] Fixed lost data precision for decimal data types (#7527)|https://github.com/apache/seatunnel/commit/df210ea73|2.3.8|
|[Improve][API] Move catalog open to SaveModeHandler (#7439)|https://github.com/apache/seatunnel/commit/8c2c5c79a|2.3.8|
|[Improve][Connector] Add multi-table sink option check (#7360)|https://github.com/apache/seatunnel/commit/2489f6446|2.3.7|
|The isNullable attribute is true when the primary key field in the Paimon table converts the Column object. #7231 (#7242)|https://github.com/apache/seatunnel/commit/b0fe432e9|2.3.6|
|[Feature][Core] Support using upstream table placeholders in sink options and auto replacement (#7131)|https://github.com/apache/seatunnel/commit/c4ca74122|2.3.6|
|[Paimon]support projection for paimon source (#6343)|https://github.com/apache/seatunnel/commit/6c1577267|2.3.6|
|[Improve][Paimon] Add check for the base type between source and sink before write. (#6953)|https://github.com/apache/seatunnel/commit/d56d64fc0|2.3.6|
|[Improve][Connector-V2] Improve the paimon source (#6887)|https://github.com/apache/seatunnel/commit/658643ae5|2.3.6|
|[Hotfix][Connector-V2] Close the tableWrite when task is close (#6897)|https://github.com/apache/seatunnel/commit/23a744b9b|2.3.6|
|[Fix][Connector-V2] Field information lost during Paimon DataType and SeaTunnel Column conversion (#6767)|https://github.com/apache/seatunnel/commit/6cf6e41da|2.3.6|
|[Improve][Connector-V2] Support hive catalog for paimon sink (#6833)|https://github.com/apache/seatunnel/commit/4969c91dc|2.3.6|
|[Hotfix][Connector-V2] Fix the batch write with paimon (#6865)|https://github.com/apache/seatunnel/commit/9ec971d94|2.3.6|
|[Feature][Doris] Add Doris type converter (#6354)|https://github.com/apache/seatunnel/commit/518999184|2.3.6|
|[Improve][Connector-V2] Support hadoop ha and kerberos for paimon sink (#6585)|https://github.com/apache/seatunnel/commit/20b62f3bf|2.3.5|
|[Feature][Paimon] Support specify paimon table write properties, partition keys and primary keys (#6535)|https://github.com/apache/seatunnel/commit/2b1234c7a|2.3.5|
|[Feature][Connector-V2] Support multi-table sink feature for paimon #5652 (#6449)|https://github.com/apache/seatunnel/commit/b0abbd2d8|2.3.5|
|[Feature][Connectors-v2-Paimon] Adaptation Paimon 0.6 Version (#6061)|https://github.com/apache/seatunnel/commit/b32df930e|2.3.4|
|[Fix] [Connectors-v2-Paimon] Flink table store failed to prepare commit (#6057)|https://github.com/apache/seatunnel/commit/c8dcefc3b|2.3.4|
|[Improve][Common] Introduce new error define rule (#5793)|https://github.com/apache/seatunnel/commit/9d1b2582b|2.3.4|
|[Improve] Remove use `SeaTunnelSink::getConsumedType` method and mark it as deprecated (#5755)|https://github.com/apache/seatunnel/commit/8de740810|2.3.4|
|[Hotfix][Connector-V2][Paimon] Bump paimon-bundle version to 0.4.0-incubating (#5219)|https://github.com/apache/seatunnel/commit/2917542bf|2.3.3|
|[Improve] Documentation and partial word optimization. (#4936)|https://github.com/apache/seatunnel/commit/6e8de0e2a|2.3.3|
|[Connector-V2][Paimon] Introduce paimon connector (#4178)|https://github.com/apache/seatunnel/commit/da507bbe0|2.3.2|

</details>
