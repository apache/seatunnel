<details><summary> Change Log </summary>

| Change | Commit | Version |
| --- | --- | --- |
|[Improve] doris options (#8745)|https://github.com/apache/seatunnel/commit/268d76cbf3| dev |
|[Improve] restruct connector common options (#8634)|https://github.com/apache/seatunnel/commit/f3499a6eeb| dev |
|[Fix][Connector-V2] fix starRocks automatically creates tables with comment (#8568)|https://github.com/apache/seatunnel/commit/c4cb1fc4a3| dev |
|[Fix][Connector-V2] Fixed adding table comments (#8514)|https://github.com/apache/seatunnel/commit/edca75b0d6| dev |
|[Fix][Doris] Fix catalog not closed (#8415)|https://github.com/apache/seatunnel/commit/2d1db66b9f|2.3.9|
|[Feature][Connector-V2[Doris]Support sink ddl (#8250)|https://github.com/apache/seatunnel/commit/ecd8269f2e|2.3.9|
|[Feature][Connector-V2]Support Doris Fe Node HA (#8311)|https://github.com/apache/seatunnel/commit/3e86102f47|2.3.9|
|[Feature][Core] Support read arrow data (#8137)|https://github.com/apache/seatunnel/commit/4710ea0f8d|2.3.9|
|[Feature][Clickhouse] Support sink savemode  (#8086)|https://github.com/apache/seatunnel/commit/e6f92fd79b|2.3.9|
|[Improve][dist]add shade check rule (#8136)|https://github.com/apache/seatunnel/commit/51ef800016|2.3.9|
|[Feature][Doris] Support multi-table source read (#7895)|https://github.com/apache/seatunnel/commit/10c37acb34|2.3.9|
|[Improve][Connector-V2] Add doris/starrocks create table with comment (#7847)|https://github.com/apache/seatunnel/commit/207b8c16fd|2.3.9|
|[Feature][Restapi] Allow metrics information to be associated to logical plan nodes (#7786)|https://github.com/apache/seatunnel/commit/6b7c53d03c|2.3.9|
|[Fixbug] doris custom sql work (#7464)|https://github.com/apache/seatunnel/commit/5c6a7c6984|2.3.8|
|[Improve][API] Move catalog open to SaveModeHandler (#7439)|https://github.com/apache/seatunnel/commit/8c2c5c79a1|2.3.8|
|[Improve][Connector-V2] Close all ResultSet after used (#7389)|https://github.com/apache/seatunnel/commit/853e973212|2.3.8|
|Revert &quot;[Fix][Connector-V2] Fix doris primary key order and fields order are inconsistent (#7377)&quot; (#7402)|https://github.com/apache/seatunnel/commit/bb72d91770|2.3.8|
|[Fix][Connector-V2] Fix doris primary key order and fields order are inconsistent (#7377)|https://github.com/apache/seatunnel/commit/464da8fb9b|2.3.7|
|[Bugfix][Doris-connector] Fix Json serialization, null value causes data error problem|https://github.com/apache/seatunnel/commit/7b19df585f|2.3.7|
|[Improve][Connector-V2] Improve doris error msg (#7343)|https://github.com/apache/seatunnel/commit/16950a67cd|2.3.7|
|[Fix][Doris] Fix the abnormality of deleting data in CDC scenario. (#7315)|https://github.com/apache/seatunnel/commit/bb2c912404|2.3.7|
|fix [Bug] Unable to create a source for identifier &#x27;Iceberg&#x27;. #7182 (#7279)|https://github.com/apache/seatunnel/commit/4897491708|2.3.7|
|[Fix][Connector-V2] Fix doris TRANSFER_ENCODING header error (#7267)|https://github.com/apache/seatunnel/commit/d886495584|2.3.6|
|[Improve][Doris Connector] Unified serialization method,Use RowToJsonConverter and TextSerializationSchema (#7229)|https://github.com/apache/seatunnel/commit/4b3af9bef4|2.3.6|
|[Feature][Core] Support using upstream table placeholders in sink options and auto replacement (#7131)|https://github.com/apache/seatunnel/commit/c4ca74122c|2.3.6|
|[Improve][Zeta] Move SaveMode behavior to master (#6843)|https://github.com/apache/seatunnel/commit/80cf91318d|2.3.6|
|[bugFix][Connector-V2][Doris] The multi-FE configuration is supported (#6341)|https://github.com/apache/seatunnel/commit/b6d075194b|2.3.6|
|[Feature][Doris] Add Doris type converter (#6354)|https://github.com/apache/seatunnel/commit/5189991843|2.3.6|
|[Improve] Improve doris create table template default value (#6720)|https://github.com/apache/seatunnel/commit/bd64740314|2.3.6|
|[Bug Fix] Sink Doris error status(#6753) (#6755)|https://github.com/apache/seatunnel/commit/0ce2c0f220|2.3.6|
|[Improve] Improve doris stream load client side error message (#6688)|https://github.com/apache/seatunnel/commit/007a9940e3|2.3.6|
|[Fix][Connector-v2] Fix the sql statement error of create table for doris and starrocks (#6679)|https://github.com/apache/seatunnel/commit/88263cd69f|2.3.6|
|[Fix][Connector-V2] Fixed doris/starrocks create table sql parse error (#6580)|https://github.com/apache/seatunnel/commit/f2ed1fbde0|2.3.5|
|[Fix][Connector-V2] Fix doris sink can not be closed when stream load not read any data (#6570)|https://github.com/apache/seatunnel/commit/341615f488|2.3.5|
|[Fix][Connector-V2] Fix connector support SPI but without no args constructor (#6551)|https://github.com/apache/seatunnel/commit/5f3c9c36a5|2.3.5|
|[Improve] Add SaveMode log of process detail (#6375)|https://github.com/apache/seatunnel/commit/b0d70ce224|2.3.5|
|[Feature] Support nanosecond in Doris DateTimeV2 type (#6358)|https://github.com/apache/seatunnel/commit/76967066bf|2.3.5|
|[Fix][Connector-V2] Fix doris source select fields loss primary key information (#6339)|https://github.com/apache/seatunnel/commit/78abe2f202|2.3.5|
|[Improve][API] Unify type system api(data &amp; type) (#5872)|https://github.com/apache/seatunnel/commit/b38c7edcc9|2.3.5|
|[Fix] Fix doris stream load failed not reported error (#6315)|https://github.com/apache/seatunnel/commit/a09a5a2bb8|2.3.5|
|[Improve][Connector-V2] Doris stream load use FE instead of BE (#6235)|https://github.com/apache/seatunnel/commit/0a7acdce95|2.3.4|
|[Feature][Connector-V2][Doris] Add Doris ConnectorV2 Source (#6161)|https://github.com/apache/seatunnel/commit/fc2d80382a|2.3.4|
|[Improve] Improve doris sink to random use be (#6132)|https://github.com/apache/seatunnel/commit/869417660e|2.3.4|
|[Feature] Support SaveMode on Doris (#6085)|https://github.com/apache/seatunnel/commit/b2375fffe8|2.3.4|
|[Improve] Add batch flush in doris sink (#6024)|https://github.com/apache/seatunnel/commit/2c5b48e907|2.3.4|
|[Fix] Fix DorisCatalog not implement `name` method (#5988)|https://github.com/apache/seatunnel/commit/d4a323efef|2.3.4|
|[Feature][Catalog] Doris Catalog (#5175)|https://github.com/apache/seatunnel/commit/1d3e335d8e|2.3.4|
|[Improve][Common] Introduce new error define rule (#5793)|https://github.com/apache/seatunnel/commit/9d1b2582b2|2.3.4|
|[Improve] Remove use `SeaTunnelSink::getConsumedType` method and mark it as deprecated (#5755)|https://github.com/apache/seatunnel/commit/8de7408100|2.3.4|
|[Improve][Connector] Add field name to `DataTypeConvertor` to improve error message (#5782)|https://github.com/apache/seatunnel/commit/ab60790f0d|2.3.4|
|[Chore] Using try-with-resources to simplify the code. (#4995)|https://github.com/apache/seatunnel/commit/d0aff52425|2.3.4|
|[Fix] Fix RestService report NullPointerException (#5319)|https://github.com/apache/seatunnel/commit/5d4b319477|2.3.4|
|[feature][doris] Doris factory type (#5061)|https://github.com/apache/seatunnel/commit/d952cea43c|2.3.3|
|[Bug][connector-v2][doris] add streamload Content-type for doris URLdecode error (#4880)|https://github.com/apache/seatunnel/commit/1b91816021|2.3.3|
|[Bug][Connector-V2][Doris] update last checkpoint id when doing snapshot (#4881)|https://github.com/apache/seatunnel/commit/0360e7e518|2.3.2|
|[Improve] Add a jobId to the doris label to distinguish between tasks (#4839)|https://github.com/apache/seatunnel/commit/6672e94077|2.3.2|
|[BUG][Doris] Add a jobId to the doris label to distinguish between tasks (#4853)|https://github.com/apache/seatunnel/commit/20ee2faecf|2.3.2|
|[Improve][Connector-V2][Doris]Remove serialization code that is no longer used (#4313)|https://github.com/apache/seatunnel/commit/0c0e5f978e|2.3.1|
|[Improve][Connector-V2][Doris] Refactor some Doris Sink code as well as support 2pc and cdc (#4235)|https://github.com/apache/seatunnel/commit/7c4005af85|2.3.1|
|[Hotfix][Connector][Doris] Fix Content Length header already present (#4277)|https://github.com/apache/seatunnel/commit/df82b77153|2.3.1|
|[Improve][build] Give the maven module a human readable name (#4114)|https://github.com/apache/seatunnel/commit/d7cd601051|2.3.1|
|[Improve][Project] Code format with spotless plugin. (#4101)|https://github.com/apache/seatunnel/commit/a2ab166561|2.3.1|
|[Improve][Connector-V2][Doris] Change Doris Config Prefix (#3856)|https://github.com/apache/seatunnel/commit/16e39a506b|2.3.1|
|[Feature][Connector-V2][Doris] Add Doris StreamLoad sink connector (#3631)|https://github.com/apache/seatunnel/commit/72158be395|2.3.0|

</details>
