# 2.3.5 Release Note

## Bug fix

### Core

- [fix] fix get seatunnel row size npe issue (#6681)
- [Hotfix] Fix DEFAULT TABLE problem (#6352)
- [Chore] Fix `file` spell errors (#6606)
- [BugFix][Spark-translation] map type cast error (#6552)
- [Hotfix] Fix spark example (#6486)
- [Hotfix] Fix compile error (#6463) 

### Transformer

- [Fix][SQLTransform] fix the scale loss for the sql transform (#6553)
- [Bug] Fix minus constant error in SQLTransform (#6533)

### Connectors

- [Fix][Kafka-Sink] fix kafka sink factory option rule (#6657)
- [Hotfix] fix http source can not read yyyy-MM-dd HH:mm:ss format bug & Improve DateTime Utils (#6601) 
- [Bug] Fix OrcWriteStrategy/ParquetWriteStrategy doesn't login with kerberos (#6472)
- [Fix][Doc] Fix FTP sink config key `username` to `user` (#6627)
- [E2E] Fix AmazondynamodbIT unstable (#6640)
- [Fix][Connector-V2] Fix add hive partition error when partition already existed (#6577)
- [Fix][Connector-V2] Fixed doris/starrocks create table sql parse error (#6580)
- [Fix][Connector-V2] Fix doris sink can not be closed when stream load not read any data (#6570)
- [Fix][Connector-V2] Fix connector support SPI but without no args constructor (#6551)
- [Fix][Connector-V2] Fix doris source select fields loss primary key information (#6339)
- [Fix][FakeSource] fix random from template not include the latest value issue (#6438)
- [Fix][Connector-V2] Fix mongodb cdc start up mode option values not right (#6338)
- [BugFix][Connector-file-sftp] Fix SFTPInputStream.close does not correctly trigger the closing of the file stream (#6323) (#6329)
- [Fix] Fix doris stream load failed not reported error (#6315)
- [fix][connector-rocketmq]Fix a NPE problem when checkpoint.interval is set too small(#6624)
- [Bugfix][TDengine] Fix the issue of losing the driver due to multiple calls to the submit job REST API #6581 (#6596)
- [Fix][StarRocks] Fix NPE when upstream catalogtable table path only have table name part (#6540)

### Formats

- [Bug] [formats] Fix fail to parse line when content contains the file delimiter (#6589)


### Zeta(ST-Engine)

- [Hotfix] fix http source can not read yyyy-MM-dd HH:mm:ss format bug & Improve DateTime Utils (#6601) 
- [Fix][Zeta] Fix the thread stuck problem caused by savepoint checking mechanism (#6568) 
- [Fix][Zeta] improve the local mode hazelcast connection (#6521)
- [Fix][Zeta] Fix thread classloader be set to null when use cache mode (#6509)
- [Bug] [zeta] Fix null pointer exception when submitting jobs (#6492)
- [bugfix] [Zeta] Fix the problem of class loader not releasing when using REST API to submit jobs 
- [BUG][Zeta]job name Display error #6470
- [Hotfix][Zeta] Fix job deadlock when schema change (#6389)

### E2E

- [E2E] Enable StarRocksCDCSinkIT (#6626)


## Improve

- [Doc][Improve] Add Support Chinese for seatunnel-engine (#6656)
- [Doc][Improve]Add Support Chinese for start-v2/locally/quick-start-flink.md and start-v2/locally/quick-start-spark.md (#6412)
- [Improve] add icon for idea (#6394)
- [Improve] Add deprecated annotation for `ReadonlyConfig::toConfig` (#6353)


### Core

- [Improve][RestAPI] always return jobId when call getJobInfoById API (#6422)
- [Improve][RestAPI] return finished job info when job is finished (#6576)
- [Improve] Improve MultiTableSinkWriter prepare commit performance (#6495)
- [Improve] Add SaveMode log of process detail (#6375)
- [Improve][API] Unify type system api(data & type) (#5872)

### Formats

- [Improve] Improve read with parquet type convert error (#6683)

### Connector-V2

- [Improve][Connector-V2]Support multi-table sink feature for redis (#6314)
- [Improve][Connector-V2] oracle cdc e2e optimization (#6232)
- [Improve][Connector-V2]Support multi-table sink feature for httpsink (#6316)
- [Improve][Connector-V2] Support INFINI Easysearch (#5933)
- [Improve][Connector-V2] Support hadoop ha and kerberos for paimon sink (#6585)
- [Improve][CDC-Connector]Fix CDC option rule. (#6454)
- [Improve][CDC] Optimize memory allocation for snapshot split reading (#6281)
- [Improve][Connector-V2] Support TableSourceFactory on StarRocks (#6498)
- [Improve][Jdbc] Using varchar2 datatype store string in oracle (#6392)
- [Improve] StarRocksSourceReader  use the existing client  (#6480)
- [Improve][JDBC] Optimized code style for getting jdbc field types (#6583)
- [Improve][Connector-V2] Add ElasticSearch type converter (#6546)
- [Improve][Connector-V2] Support read orc with schema config to cast type (#6531)
- [Improve][Jdbc] Support custom case-sensitive config for dameng (#6510)
- [Improve][Jdbc] Increase tyepe converter when auto creating tables (#6617)
- [Improve][CDC] Optimize split state memory allocation in increment phase (#6554)
- [Improve][CDC] Improve read performance when record not contains schema field (#6571)
- [Improve][Jdbc] Add quote identifier for sql (#6669)
- [Improve] Add disable 2pc in SelectDB cloud sink (#6266)
- [Doc][Improve] Add Support Kerberos Auth For Kafka Connector #6653

### CI

- [CI] Fix error repository name in ci config files (#4795)

### Zeta(ST-Engine)

- [Improve][Zeta] Add classloader cache mode to fix metaspace leak (#6355)
- [Improve][Test] Fix test unstable on `ResourceManger` and `EventReport` module (#6620)
- [Improve][Test] Run all test when code merged into dev branch (#6609)
- [Improve][Test] Make classloader cache testing more stable (#6597)
- [Improve][Zeta][storage] update hdfs configuration, support more parameters (#6547)
- [Improve][Zeta]Optimize the logic of RestHttpGetCommandProcessor#getSeaTunnelServer()  (#6666)


### Transformer

- [Improve][Transform] Sql transform support inner strucy query (#6484)
- [Improve][Transform] Remove Fallback during parsing Transform process (#6644)
- [Improve][Transform] Remove can't find field exception  (#6691)


## Feature

### Core

- [Feature][Tool] Add connector check script for issue 6199 (#6635)
- [Feature][Core] Support listening for message delayed events in cdc source (#6634)
- [Feature][Core] Support event listener for job (#6419)

### Connector-V2

- [Feature][connector-v2] add xugudb connector (#6561)
- [Feature][Connector-V2] Support multi-table sink feature for paimon #5652 (#6449)
- [Feature][Connectors-V2][File]support assign encoding for file source/sink (#6489)
- [Feature][Connector]update pgsql-cdc publication for add table (#6309)
- [Feature][Paimon] Support specify paimon table write properties, partition keys and primary keys (#6535)
- [Feature][Feature] Support nanosecond in Doris DateTimeV2 type (#6358)
- [Feature][Feature] Support nanosecond in SelectDB DateTimeV2 type (#6332)
- [Feature][Feature] Supports iceberg sink #6198 (#6265)

### Zeta(ST-Engine)

- [Zeta] Support config job retry times in job config (#6690)


## Docs 

- [Docs] fix kafka format typo (#6633)
- [Fix][Doc] Fixed links in some documents (#6673)
- [Fix][Doc] Fix some spell errors (#6628)
- [Fix][Doc] Fixed typography error in starrocks sink document (#6579)
- [Hotfix][Doc][Chinese] Fix invalid link about configure logging related parameters (#6442)
- [Fix][Doc]Seatunnel Engine/checkpoint-storage.md doc error(#6369)