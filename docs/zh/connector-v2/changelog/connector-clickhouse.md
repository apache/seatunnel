<details><summary> Change Log </summary>

| Change | Commit | Version |
| --- | --- | --- |
|[Fix] [Clickhouse] Parallelism makes data duplicate (#8916)|https://github.com/apache/seatunnel/commit/45345f2738| dev |
|[Fix][Connector-V2]Fix Descriptions for CUSTOM_SQL in Connector (#8778)|https://github.com/apache/seatunnel/commit/96b610eb7e| dev |
|[improve] update clickhouse connector config option (#8755)|https://github.com/apache/seatunnel/commit/b964189b75| dev |
|[Fix][Connector-V2] fix starRocks automatically creates tables with comment (#8568)|https://github.com/apache/seatunnel/commit/c4cb1fc4a3| dev |
|[Fix][Connector-V2] Fixed adding table comments (#8514)|https://github.com/apache/seatunnel/commit/edca75b0d6| dev |
|[hotfix] fix exceptions caused by operator priority in connector-clickhouse when using sharding_key (#8162)|https://github.com/apache/seatunnel/commit/5560e3dab2|2.3.9|
|[Imporve][ClickhouseFile] Directly connect to each shard node to obtain the corresponding path (#8449)|https://github.com/apache/seatunnel/commit/757641bada|2.3.9|
|[Feature][ClickhouseFile] Support add publicKey to identity (#8351)|https://github.com/apache/seatunnel/commit/287b8c8219|2.3.9|
|[Improve][ClickhouseFile] Improve rsync log output (#8332)|https://github.com/apache/seatunnel/commit/179223e3c2|2.3.9|
|[Improve][ClickhouseFile] Added attach sql log for better debugging (#8315)|https://github.com/apache/seatunnel/commit/ade428c5fa|2.3.9|
|[Chore] delete chinese desc in code (#8306)|https://github.com/apache/seatunnel/commit/a50a8b925f|2.3.9|
|[Improve][ClickhouseFile Connector] Unified specifying clickhouse file generation path (#8302)|https://github.com/apache/seatunnel/commit/455f1ed760|2.3.9|
|[Improve][ClickhouseFile] Clickhouse supports option configuration when connecting to shard nodes (#8297)|https://github.com/apache/seatunnel/commit/1ded1b6206|2.3.9|
|[Imporve][ClickhouseFile] Improve clickhousefile generation parameter configuration (#8293)|https://github.com/apache/seatunnel/commit/753e058fee|2.3.9|
|[Improve][ClickhouseFile] ClickhouseFile Connector&#x27;s rsync transmission supports specifying users (#8236)|https://github.com/apache/seatunnel/commit/e012bd0a4f|2.3.9|
|[Feature][Clickhouse] Support sink savemode  (#8086)|https://github.com/apache/seatunnel/commit/e6f92fd79b|2.3.9|
|[Improve][dist]add shade check rule (#8136)|https://github.com/apache/seatunnel/commit/51ef800016|2.3.9|
|[Fix][Connecotr-V2] Fix clickhouse sink does not support composite primary key (#8021)|https://github.com/apache/seatunnel/commit/24d0542595|2.3.9|
|[Improve] update clickhouse connector, use factory to create source/sink (#7946)|https://github.com/apache/seatunnel/commit/b69fceceee|2.3.9|
|[Fix][Connector-V2] Fixed clickhouse connectors cannot stop under multiple parallelism (#7921)|https://github.com/apache/seatunnel/commit/8d9c6a3714|2.3.9|
|Bump commons-io:commons-io from 2.11.0 to 2.14.0 in /seatunnel-connectors-v2/connector-clickhouse (#7784)|https://github.com/apache/seatunnel/commit/f4393a02bf|2.3.9|
|[Feature][Restapi] Allow metrics information to be associated to logical plan nodes (#7786)|https://github.com/apache/seatunnel/commit/6b7c53d03c|2.3.9|
|[Improve] Improve some connectors prepare check error message (#7465)|https://github.com/apache/seatunnel/commit/6930a25edd|2.3.8|
|[Improve][Connector-V2] Close all ResultSet after used (#7389)|https://github.com/apache/seatunnel/commit/853e973212|2.3.8|
|[Feature][Connector-V2][Clickhouse] Add clickhouse.config to the source connector (#7143)|https://github.com/apache/seatunnel/commit/f7994d9ae9|2.3.6|
|[Improve] Make ClickhouseFileSinker support tables containing materialized columns (#6956)|https://github.com/apache/seatunnel/commit/87c6adcc2e|2.3.6|
|[Improve] [Clickhouse] Remove check when set allow_experimental_lightweight_delete false(#6727) (#6728)|https://github.com/apache/seatunnel/commit/b25e1b1ae5|2.3.6|
|[Improve][Common] Adapt `FILE_OPERATION_FAILED` to `CommonError` (#5928)|https://github.com/apache/seatunnel/commit/b3dc0bbc21|2.3.4|
|[Improve][Connector-V2] Replace CommonErrorCodeDeprecated.JSON_OPERATION_FAILED (#5978)|https://github.com/apache/seatunnel/commit/456cd17714|2.3.4|
|[Feature][Core] Upgrade flink source translation (#5100)|https://github.com/apache/seatunnel/commit/5aabb14a94|2.3.4|
|[Improve] Speed up ClickhouseFile Local generate a mmap  object (#5822)|https://github.com/apache/seatunnel/commit/cf39e29dad|2.3.4|
|[Improve][Common] Introduce new error define rule (#5793)|https://github.com/apache/seatunnel/commit/9d1b2582b2|2.3.4|
|[Improve] Remove use `SeaTunnelSink::getConsumedType` method and mark it as deprecated (#5755)|https://github.com/apache/seatunnel/commit/8de7408100|2.3.4|
|[Hotfix][connector-v2][clickhouse] Fixed an out-of-order BUG with output data fields of clickhouse-sink (#5346)|https://github.com/apache/seatunnel/commit/fce9ddaa2b|2.3.4|
|[Bugfix][Clickhouse] Fix clickhouse sink flush bug (#5448)|https://github.com/apache/seatunnel/commit/cef03f6673|2.3.4|
|[Hotfix][Clickhouse] Fix clickhouse old version compatibility (#5326)|https://github.com/apache/seatunnel/commit/1da49f5a2b|2.3.4|
|[Improve][CheckStyle] Remove useless &#x27;SuppressWarnings&#x27; annotation of checkstyle. (#5260)|https://github.com/apache/seatunnel/commit/51c0d709ba|2.3.4|
|[Hotfix] Fix com.google.common.base.Preconditions to seatunnel shade one (#5284)|https://github.com/apache/seatunnel/commit/ed5eadcf73|2.3.3|
|[Feature][Connector-V2][Clickhouse] Add clickhouse connector time zone key,default system time zone (#5078)|https://github.com/apache/seatunnel/commit/309b58d12d|2.3.3|
|[Bugfix]fix clickhouse source connector read Nullable() type is not null,example:Nullable(Float64) while value is null the result is 0.0 (#5080)|https://github.com/apache/seatunnel/commit/cf3d0bba2e|2.3.3|
|[Feature][Connector-V2][Clickhouse] clickhouse writes with checkpoints (#4999)|https://github.com/apache/seatunnel/commit/f8fefa1e57|2.3.3|
|[Hotfix][Connector-V2][ClickhouseFile] Fix ClickhouseFile write file failed when field value is null (#4937)|https://github.com/apache/seatunnel/commit/06671474ca|2.3.3|
|[Hotfix][connector-clickhouse] fix get clickhouse local table name with closing bracket from distributed table engineFull (#4710)|https://github.com/apache/seatunnel/commit/e5e0cba26d|2.3.2|
|[Bug] [Connector-V2] Clickhouse File Connector failed to sink to table with settings like storage_policy (#4172)|https://github.com/apache/seatunnel/commit/e120dc44bc|2.3.1|
|[Improve][build] Give the maven module a human readable name (#4114)|https://github.com/apache/seatunnel/commit/d7cd601051|2.3.1|
|[Improve][Project] Code format with spotless plugin. (#4101)|https://github.com/apache/seatunnel/commit/a2ab166561|2.3.1|
|[Bug] [Connector-V2] Clickhouse File Connector not support split mode for write data to all shards of distributed table (#4035)|https://github.com/apache/seatunnel/commit/3f1dcfc915|2.3.1|
|[Hotfix][Connector-V2] Fix connector source snapshot state NPE (#4027)|https://github.com/apache/seatunnel/commit/e39c4988cc|2.3.1|
|[Hotfix][Connector-v2][Clickhouse] Fix clickhouse write cdc changelog update event (#3951)|https://github.com/apache/seatunnel/commit/67e6027970|2.3.1|
|[Feature][shade][Jackson] Add seatunnel-jackson module (#3947)|https://github.com/apache/seatunnel/commit/5d8862ec9c|2.3.1|
|[Improve][Connector-V2][Clickhouse] Improve performance (#3910)|https://github.com/apache/seatunnel/commit/aeceb855f6|2.3.1|
|[Improve] [Connector-V2] Remove Clickhouse Fields Config (#3826)|https://github.com/apache/seatunnel/commit/74704c362a|2.3.1|
|[Improve][Connector-V2][clickhouse] Special characters in column names are supported (#3881)|https://github.com/apache/seatunnel/commit/9069609c17|2.3.1|
|[Feature][Connector] add get source method to all source connector (#3846)|https://github.com/apache/seatunnel/commit/417178fb84|2.3.1|
|[Improve] [Connector-V2] Change Connector Custom Config Prefix To Map (#3719)|https://github.com/apache/seatunnel/commit/ef1b8b1bb5|2.3.1|
|[Feature][API &amp; Connector &amp; Doc] add parallelism and column projection interface (#3829)|https://github.com/apache/seatunnel/commit/b9164b8ba1|2.3.1|
|[Bug] [Connector-V2] Fix ClickhouseFile Committer Serializable Problems (#3803)|https://github.com/apache/seatunnel/commit/1b26192cb3|2.3.1|
|[feature][connector-v2][clickhouse] Support write cdc changelog event in clickhouse sink (#3653)|https://github.com/apache/seatunnel/commit/6093c213bf|2.3.0|
|[Connector-V2] [Clickhouse] Improve Clickhouse File Connector (#3416)|https://github.com/apache/seatunnel/commit/e07e9a7cc2|2.3.0|
|[Hotfix][OptionRule] Fix option rule about all connectors (#3592)|https://github.com/apache/seatunnel/commit/226dc6a119|2.3.0|
|[Improve][Connector-V2][Clickhouse] Unified exception for Clickhouse source &amp; sink connector (#3563)|https://github.com/apache/seatunnel/commit/04e1743d9e|2.3.0|
|options in conditional need add to required or optional options (#3501)|https://github.com/apache/seatunnel/commit/51d5bcba10|2.3.0|
|[Feature][Connector-V2][Clickhouse]Optimize clickhouse connector data type inject (#3471)|https://github.com/apache/seatunnel/commit/9bd0fc8ee2|2.3.0|
|[improve][connector-v2][clickhouse] Fix DoubleInjectFunction (#3441)|https://github.com/apache/seatunnel/commit/9781a6a385|2.3.0|
|[feature][api] add option validation for the ReadonlyConfig (#3417)|https://github.com/apache/seatunnel/commit/4f824fea36|2.3.0|
|[improve][connector] The Factory#factoryIdentifier must be consistent with PluginIdentifierInterface#getPluginName (#3328)|https://github.com/apache/seatunnel/commit/d9519d696a|2.3.0|
|[Improve][Connector-V2] Add Clickhouse and Assert Source/Sink Factory (#3306)|https://github.com/apache/seatunnel/commit/9e4a128381|2.3.0|
|[Improve][Clickhouse-V2] Clickhouse Support Geo type (#3141)|https://github.com/apache/seatunnel/commit/01cdc4e336|2.3.0|
|[Improve][Connector-V2][Clickhouse] Support nest type and array (#3047)|https://github.com/apache/seatunnel/commit/97b5727ec6|2.3.0|
|[Feature][Connector-V2-Clickhouse] Clickhouse Source random use host when config multi-host (#3108)|https://github.com/apache/seatunnel/commit/c9583b7f63|2.3.0-beta|
|[Improve] [Clickhouse-V2] Clickhouse Support Int128,Int256 Type (#3067)|https://github.com/apache/seatunnel/commit/e118ccea0a|2.3.0-beta|
|[Improve][all] change Log to @Slf4j (#3001)|https://github.com/apache/seatunnel/commit/6016100f12|2.3.0-beta|
|[Connector-V2] [Clickhouse] Fix Clickhouse Type Mapping and Spark Map reconvert Bug (#2767)|https://github.com/apache/seatunnel/commit/f0a1f5013a|2.2.0-beta|
|[DEV][Api] Replace SeaTunnelContext with JobContext and remove singleton pattern (#2706)|https://github.com/apache/seatunnel/commit/cbf82f755c|2.2.0-beta|
|[#2606]Dependency management split (#2630)|https://github.com/apache/seatunnel/commit/fc047be69b|2.2.0-beta|
|[Feature][Connector-V1 &amp; V2] Support unauthorized ClickHouse (#2393)|https://github.com/apache/seatunnel/commit/0e4e2b1230|2.2.0-beta|
|[Feature][connector] clickhousefile sink connector support non-root username for fileTransfer (#2263)|https://github.com/apache/seatunnel/commit/704661f1fd|2.2.0-beta|
|StateT of SeaTunnelSource should extend `Serializable` (#2214)|https://github.com/apache/seatunnel/commit/8c426ef850|2.2.0-beta|
|[Bug] [connector-v2] When outputting data to clickhouse, a ClassCastException was encountered (#2160)|https://github.com/apache/seatunnel/commit/a3a2b5d189|2.2.0-beta|
|[API-DRAFT] [MERGE] fix merge error|https://github.com/apache/seatunnel/commit/736ac01c89|2.2.0-beta|
|merge dev to api-draft|https://github.com/apache/seatunnel/commit/d265597c64|2.2.0-beta|
|[api-draft][connector] support Rsync to transfer clickhouse data file (#2080)|https://github.com/apache/seatunnel/commit/02a41902a8|2.2.0-beta|
|[api-draft][Optimize] Optimize module name (#2062)|https://github.com/apache/seatunnel/commit/f79e3112b1|2.2.0-beta|

</details>
