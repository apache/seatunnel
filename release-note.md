# 2.3.8 Release Note


## Bug fix

### Core

- [Hotfix][Core] Fix concurrency exceptions when MultiTableSink#PrepareCommit (#7686)
- [hotfix][zeta] conf property is null, print log bug fix (#7487)
- [Fix][Seatunnel-core]Fix syntax error in the execution script on Windows (#7423)

### Connectors
- [Fix][Connecotr-V2] Fix paimon dynamic bucket tale in primary key is not first (#7728)
- [Fix][Connector-V2] Fix iceberg throw java: package sun.security.krb5 does not exist when use jdk 11 (#7734)
- [Fix][Connector-V2] Release resources when task is closed for iceberg sinkwriter (#7729)
- [Fix][Connector-V2] Release resources even the task is crashed for paimon sink (#7726)
- [Fix][Connector-V2] Fix paimon e2e error (#7721)
- [Fix][Connector-V2] Fix known directory create and delete ignore issues (#7700)
- [Fix][Connector-V2] Fixed iceberg sink can not handle uppercase fields (#7660)
- [Hotfix][CDC] Fix ddl duplicate execution error when config multi_table_sink_replica (#7634)
- [Fix][Connector-V2] Fix some throwable error not be caught (#7657)
- [Fix][Connector-V2] Fix jdbc test case failed (#7690)
- [Fix][Connector-v2] Throw Exception in sql query for JdbcCatalog in table or db exists query (#7651)
- [Fix][JDBC] Fix starrocks jdbc dialect catalog conflict with starrocks connector (#7578)
- [bugfix] fix oracle query table length (#7627)
- [Hotfix] Fix iceberg missing column comment when savemode create table (#7608)
- [Hotfix][Seatunnel-common] Fix the CommonError msg for paimon sink (#7591)
- [Hotfix][Connector-v2] Fix the ClassCastException for connector-mongodb (#7586)
- [Hotfix][Connector-v2] Fix the NullPointerException for jdbc oracle which used the table_list (#7544)
- [Bug][e2e][jdbc-gbase] The gbase driver address certificate expired (#7531)
- [Hotfix][Connector-V2] Fixed lost data precision for decimal data types (#7527)
- [Fix][Connector-V2] Fix jdbc query sql can not get table path (#7484)
- [Hotfix][Connector-V2] Fix null not inserted in es (#7493)
- [Fixbug] doris custom sql work (#7464)
- [Fix] Fix oracle sample data from column error (#7340)
- [Hotifx][Jdbc] Fix MySQL unsupport 'ZEROFILL' column type (#7407)
- [Hotfix][CDC] Fix package name spelling mistake (#7415)
- [Hotfix][Zeta] Fix hazelcast client should overwrite cluster address by system env (#7790)

### Zeta(ST-Engine)
- [Core][Zeta] If Zeta not a TCP discovery, it cannot find other members (#7757)
- [Fix][Zeta] Fix resource isolation not working on multi node (#7471)

### Transformer
- [Bug][Transforms-V2] Fix LLM transform can not parse boolean value type (#7620)

### CI
- [Fix][CI] Fix CI loss document check when raise PR (#7749)
- [Hotfix][ci] Fix github ci License header error (#7738)
- [Fix][CI] Add doc sync to fix ci error (#7720)
- [hotfix] fix FixSlotResourceTest unstable issue (#7577)
- [Fix][e2e] remote loading driver ignores the certificate to avoid certificate address expiration (#7547)
- [Fix] Fix document build error (#7546)
- [Fix][doc] fix dead link (#7508)
- [Fix] update paimon.apache.org deadlink (#7504)
- [Hotfix][Metrics] fix sporadic multi-metrics ci (#7468)
- [Fix] Fix dead link on seatunnel connectors list url (#7453)
- [FIX][E2E]Modify the OceanBase test case to the latest imageChange image (#7452)


## Improve

### Core
- [Improve][Spark] Convert array type to exact type (#7758)
- [Improve][Zeta] Split the classloader of task group (#7580)
- [Improve][Core] Config variables update doc and add test case (#7709)
- [Improve][Zeta] Add log for tryTriggerPendingCheckpoint because the wrong time of server (#7717)
- [Improve][EventService] improve event code and extract event code to EventService (#7153)
- [Chore] Code specification adjustments (#7572)
- [Chore] Update zeta example log4j2.properties (#7563)
- [Improve] Update docker doc and build tag (#7486)
- [Improve][Zeta] Handle user privacy when submitting a task print config logs (#7247)
- [Improve][API] Add IGNORE savemode type (#7443)
- [Improve][API] Move catalog open to SaveModeHandler (#7439)
- [Improve] Skip downloading transitive dependency when install plugins (#7374)
- [Improve] Flink support embedding transform (#7592)

### Connector-V2
- [Improve][Connector-v2] Remove useless code and add changelog doc for paimon sink (#7748)
- [Improve][Connector-V2] Optimize sqlserver package structure (#7715)
- [Improve][Connector-V2] Optimize milvus code (#7691)
- [Improve][Redis]Redis scan command supports versions 5, 6, 7 (#7666)
- [Improve][Connector-V2] Support read archive compress file (#7633)
- [Improve][Jdbc] Jdbc truncate table should check table not database (#7654)
- [Improve] [Connector-V2] Optimize milvus-connector config code (#7658)
- [Improve][Connector-V2] Time supports default value (#7639)
- [Improve][Iceberg] Add savemode create table primaryKey testcase (#7641)
- [Improve][Kafka] kafka source refactored some reader read logic (#6408)
- [Improve][Connector-V2][MongoDB] A BsonInt32 will be convert to a long type (#7567)
- [Improve][Connector-v2] Improve the exception msg in case-sensitive case for paimon sink (#7549)
- [Improve][Connector-v2] Support mysql 8.1/8.2/8.3 for jdbc (#7530)
- [Improve][Connector-v2] Release resource in closeStatements even exception occurred in executeBatch (#7533)
- [Improve][Connector-V2] Remove hard code iceberg table format version (#7500)
- [Improve][Connector-V2] Fake supports column configuration (#7503)
- [Improve][Connector-V2] update vectorType (#7446)
- [Improve] Improve some connectors prepare check error message (#7465)
- [Improve] Refactor S3FileCatalog and it's factory (#7457)
- [Improve] Added OSSFileCatalog and it's factory (#7458)
- [Improve][Connector-V2] Reuse connection in StarRocksCatalog (#7342)
- [Improve][Connector-V2] Remove system table limit (#7391)
- [Improve][Connector-V2]Support multi-table sink feature for email (#7368)
- [Improve] Update pull request template and github action guide (#7376)
- [Improve][Connector-V2] Close all ResultSet after used (#7389)
- [Improvement] add starrocks jdbc dialect (#7294)

### Transform
- [Improve][Transform] Improve inner sql query for map field type (#7718)
- [Improve][Transform] Support errorHandleWay on jsonpath transform (#7524)
- [Improve][Transform-v2] Refactor a transformRow from FilterFieldTransform (#7598)
- [Improve][Transform] Add LLM model provider microsoft #7778

### CI
- [Improve][CI] Move paimon into single task (#7719)
- [Improve][Test][Connector-V2][MongoDB] Add few test cases for BsonToRowDataConverters (#7579)
- [Improve][Test] Remove useless code of S3Utils. (#7515)
- [Improve][E2E] update doris image to official version #7773


## Feature

### Core
- [Feature][Flink] Support multiple tables read and write (#7713)
- [Feature][Zeta][Core] Support output log file of job (#7712)
- [Feature][REST-API] Add threaddump rest api (#7615)
- [Feature][rest-api] Add whether master node identifier (#7603)
- [Feature][Core] shell batch cancel task (#7612)
- [Feature] Support config variable substitution with default value (#7562)
- [Feature][zeta]Support exposing monitoring metrics by prometheus exporter protocol (#7564)
- [Feature][Zeta] add rest api to update node  tags (#7542)
- [Feature][Zeta] Optimized llm doc && add DOUBAO LLM (#7584)
- [Feature][Zeta] Support slf4j mdc tracing job id output into logs (#7491)
- [Feature][Core] Added rest-api for batch start and stop (#7522)
- [Feature][Core] Add event notify for all connector (#7501)
- [Feature] add dockerfile (#7346)
- [Feature][Zeta] Added other metrics info of multi-table (#7338)
- [Feature][Spark] Support multiple tables read and write (#7283)

### Connector-V2
- [Feature][Connector-V2] Assert support multi-table check (#7687)
- [Feature][Connector-Paimon] Support dynamic bucket splitting improves Paimon writing efficiency (#7335)
- [Feature][Connector-v2] Support streaming read for paimon (#7681)
- [Feature][Connector-V2] Optimize hudi sink (#7662)
- [Feature][Connector-V2] jdbc saphana source tablepath support view and  synonym (#7670)
- [Feature][Connector-V2] Ftp file sink suport multiple table and save mode (#7665)
- [Feature] Support tidb cdc connector source #7199 (#7477)
- [Feature][kafka] Add arg  poll.timeout  for interval poll messages (#7606)
- [Feature][Connector-V2][Hbase] implement hbase catalog (#7516)
- [Feature][Elastic search] Support multi-table source feature (#7502)
- [Feature][CONNECTORS-V2-Paimon] Paimon Sink supported truncate table (#7560)
- [Feature][Connector-V2] Support Qdrant sink and source connector (#7299)
- [Feature][Connector-V2] Support multi-table sink feature for HBase (#7169)
- [Feature][Connector-V2] Support typesense connector (#7450)
- [Feature][Rabbitmq] Allow configuration of queue durability and deletion policy (#7365)
- [Feature][Connector-V2] Add `decimal_type_narrowing` option in jdbc (#7461)
- [Feature][connector-v2]Add Kafka Protobuf Data Parsing Support (#7361)
- [Feature][Connector-V2][Tablestore] Support Source connector for Tablestore #7448  (#7467)
- [Feature][Connector-V2] Support opengauss-cdc (#7433)
- [Feature][Connector-V2] Suport choose the start page in http paging (#7180)
- [Feature][Connector-V2][OceanBase] Support vector types on OceanBase (#7375)
- [Feature][Connector-V2] Fake Source support produce vector data (#7401)
- [Feature][Connector-V2][Iceberg] Support Iceberg Kerberos (#7246)
- [Feature][Connector-V2] SqlServer support user-defined type (#7706)
- [Feature][Connector-V2] sftp file sink suport multiple table and save mode (#7668)

### Transform
- [Feature][Transforms-V2] LLM transforms Support custom field name (#7640)
- [Feature][Transforms-V2] LLM transforms Support KimiAI (#7630)
- [Future][Transforms-V2] llm trans support field projection (#7621)
- [Feature][Transform] Add embedding transform (#7534)

## Docs 

- [Docs] Spark use doc update (#7722)
- [Docs] Improve startrocks sink doc (#7744)
- [Docs] Add logging into sidebars.js (#7742)
- [Docs] Add tuning guide for doris sink in streaming mode (#7732)
- [Docs] update Dynamic-compile document (#7730)
- [Docs] Improve engine deployment doc for skip deploy client (#7723)
- [Docs] Add some connector icons (#7710)
- [Docs] Fixed hive sink doc (#7696)
- [Docs] Refactor job env config document (#7631)
- [Docs] Add doc for mysql-cdc schema evolution (#7626)
- [Docs] Additional rest api documentation. (#7645)
- [Docs] Add a usage method for a keyword in sql-config (#7594)
- [Docs] add docker cluster guide (#7532)
- [Docs] Add remind for job config file suffix (#7625)
- [Docs] Fixed telemetry doc (#7623)
- [Docs] Fix document symbol escape error (#7611)
- [Docs] resource isolation add update node tags link (#7607)
- [Docs] Update PostgreSQL-CDC.md (#7601)
- [Docs] Improve quick start and build from source code (#7548)
- [Docs] Update jar download link and formats sidebar (#7569)
- [Docs]add kafka doc (#7553)
- [Docs] Remove a `fs.oss.credentials.provider` option (#7507)
- [Docs] Update the doc structure (#7425)
- [Docs] Update the guide and add example (#7388)
- [ASF] Add new collaborator (#7399)
- [Docs] Improve startrocks source doc #7777
