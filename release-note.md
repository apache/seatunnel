# 2.3.7 Release Note

## Bug fix

### Connector-V2

- [Hotfix][MySQL-CDC] Fix ArrayIndexOutOfBoundsException in mysql binlog read (#7381) (#40c5f313e)
- [Fix][Doris] Fix doris primary key order and fields order are inconsistent (#7377) (#464da8fb9)
- [Bugfix][Doris] Fix Json serialization, null value causes data error problem (#7b19df585)
- [Hotfix][Jdbc] Fix jdbc compile error (#7359) (#2769ed502)
- [Fix][OceanBase] Remove OceanBase catalog's dependency on mysql driver (#7311) (#3130ae089)
- [Fix][Tdengine] Fix sql exception and concurrentmodifyexception when connect to taos and read data (#a18fca800)
- [Hotfix][Kafka] Fix kafka consumer log next startup offset (#7312) (#891652399)
- [Fix][Doris] Fix the abnormality of deleting data in CDC scenario. (#7315) (#bb2c91240)
- [hotfix][Hbase]fix and optimize hbase source problem (#7148) (#34a6b8e9f)
- [Fix][Iceberg] Unable to create a source for identifier 'Iceberg'. #7182 (#7279) (#489749170)

### Zeta(ST-Engine)

- [Fix][Zeta] Fix task can not end cause by lock metrics failed (#7357) (#6a7df83b3)
- [Hotfix][Zeta] Fix task cannot be stopped when system is busy (#7292) (#73632bad2)
- [Hotfix][Zeta] Fix task cannot be stopped when system is busy (#7280) (#3ccc6a8bd)

### E2E

- [Fix][Http] Fix http e2e case (#7356) (#9d161d24e)

## Improve

### Core

- [Improve][Flink] optimize method name (#7372) (#7e7738430)
- [Improve][API] Check catalog table fields name legal before send to downstream (#7358) (#fa34ac98b)
- [Improve][Flink] refactor flink proxy source/sink (#7355) (#068c5e3e3)
- [Improve][API] Make sure the table name in TablePath not be null (#7252) (#764d8b0bc)
- [Improve][Core] Improve base on plugin name of lookup strategy (#7278) (#21c4f5245)

### Connector-V2

- [Improve][multi-table] Add multi-table sink option check (#7360) (#2489f6446)
- [Improve][Console] Update ConsoleSinkFactory.java (#7350) (#921662722)
- [Improve][Jdbc] Skip all index when auto create table to improve performance of write (#7288) (#dc3c23981)
- [Improve][Doris] Improve doris error msg (#7343) (#16950a67c)
- [Improve][Jdbc] Remove MysqlType references in JdbcDialect (#7333) (#16eeb1c12)
- [Improve][Jdbc] Merge user config primary key when create table (#7313) (#819c68565)
- [Improve][Jdbc] Optimize the way of databases and tables are checked for existence (#7261) (#f012b2a6f)

### Transforms-V2

- [Improve][DynamicCompile] Improve DynamicCompile transform (#7319) (#064fcad36)
- [Improve][SQL] Remove escape identifier from output fields (#7297) (#82f5d8c71)
- [Improve][DynamicCompile] Improve DynamicCompile transform  (#7264) (#9df557cb1)

### E2E

- [Improve][Improve] Enable fakesource e2e of spark/flink (#7325) (#460e73ec3)
- [Improve][Improve] Enable JdbcPostgresIdentifierIT (#7326) (#f6a1e51b8)
- [Improve][Improve] Support windows for the e2e of paimon (#7329) (#a4db64d7c)

## Feature

### Connector-V2

- [Feature][Aliyun SLS] add Aliyun SLS connector #3733 (#7348) (#527c7c7b5)
- [Feature][Activemq] Added activemq sink  (#7251) (#f0cefbeb4)

### Transforms-V2

- [Feature] Split transform and move jar into connectors directory (#7218) (#d46cf16e5)
- [Feature][LLM] Add LLM transform (#7303) (#855254e73)
- [Feature][SQL] Support cast to bytes function of sql (#7284) (#b9acb573b)

## Docs

- [Docs] Change deprecated connector name in setup.md (#7366) (#862e2055c)
- [Docs] Fix username parameter error in sftp sink document (#7334) (#191d9e18b)
- [Docs] fix document configuration is rectified when the oss is selected as the checkpoint base (#7332) (#a12786b82)
- [Docs] Fix miss sink-options-placeholders.md in sidebars (#7310) (#c94ea325b)
- [Docs] Update Oracle-CDC.md (#7285) (#9d56cc33b)
- [Docs] Fix hybrid cluster deployment document display error (#7306) (#2fd4eec22)
- [Docs] translate event-listener doc into chinese (#7274) (#ec1c3198b)

## Others

- Bump org.apache.activemq:activemq-client (#7323) (#e23e3ac4e)
- [Improve] Remove unused code (#7324) (#7c3cd99e0)
- [Improve] Update snapshot version to 2.3.7 (#7305) (#4f120ff34)

