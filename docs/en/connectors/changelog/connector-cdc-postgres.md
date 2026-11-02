<details><summary> Change Log </summary>

| Change | Commit | Version |
| --- | --- | --- |
|[Fix][Connector-V2] Exclude columns not published by pgoutput from the Postgres-CDC schema (#12561)|https://github.com/apache/seatunnel/commit/56b063c1d8|3.1.0|
|[Fix][Connector-V2] Stream PostgreSQL sampling results with JDBC cursor (#12731)|https://github.com/apache/seatunnel/commit/77ba9ada9d|3.1.0|
|[Fix][Connector-V2] Resolve late PostgreSQL CDC type names by search path (#12836)|https://github.com/apache/seatunnel/commit/89a5598eb5|3.1.0|
|[Docs][Connector-V2] Audit PostgreSQL CDC bounded-read feasibility (#12359)|https://github.com/apache/seatunnel/commit/82940077c2|3.1.0|
|[Fix][Connector-V2] Fail fast when the Postgres-CDC replication slot has been invalidated (#12587)|https://github.com/apache/seatunnel/commit/3926d8784f|3.1.0|
|[Fix][E2E] Fix task recovery hangs and PostgreSQL CDC LSN handling (#12454)|https://github.com/apache/seatunnel/commit/0278a6a747|3.1.0|
|[Fix][Connector-V2] Apply Postgres-CDC ADD COLUMN on the first RELATION with pgjdbc 42.7.5+ (#12547)|https://github.com/apache/seatunnel/commit/7c4f2d7720|3.1.0|
|[Feature][Connector-V2] Introduce PG base CDC abstraction for Postgres and OpenGauss (#11029)|https://github.com/apache/seatunnel/commit/9ad05cd740|3.1.0|
|[Fix][Connector-V2] Support PostgreSQL enum columns in the Postgres catalog and CDC (#12588)|https://github.com/apache/seatunnel/commit/eb2eb1ddc8|3.1.0|
|[Fix][Connector-V2] Filter Postgres CDC database discovery (#11399)|https://github.com/apache/seatunnel/commit/446ff62668|3.1.0|
|[Fix][Connector-V2] Deduplicate table ids in PostgreSQL CDC table discovery to support HighGo (#12435)|https://github.com/apache/seatunnel/commit/4648c9956e|3.1.0|
|[Fix][Connector-V2] Preserve disabled CDC schema behavior (#12346)|https://github.com/apache/seatunnel/commit/ea3df16677|3.1.0|
|[Fix][Connector-V2] Fix Postgres-CDC committed-offset recovery skipping rows after savepoint (#11864)|https://github.com/apache/seatunnel/commit/5af8d789aa|3.0.0|
|[Feature][Connector-V2] Support PostgreSQL CDC ADD COLUMN schema evolution (#11922)|https://github.com/apache/seatunnel/commit/8bea8c681c|3.0.0|
|[Fix][Connector-CDC] Implement MySQL CDC stop.mode = &quot;specific&quot; (bounded read) (#11618)|https://github.com/apache/seatunnel/commit/d7dd5d5f5c|3.0.0|
|[Feature][Connector-V2][PostgreSQL-CDC] Add committed-offset and snapshot-only startup modes (#11380)|https://github.com/apache/seatunnel/commit/ddcc764d20|3.0.0|
|[Feature][CDC/JDBC] Add enable_concurrent_read option to skip split analysis for tables without indexes (#10837)|https://github.com/apache/seatunnel/commit/844d9c3630|3.0.0|
|[Feature][Connector-V2] Foundation for per-connector Debezium version management (staged) (#10799)|https://github.com/apache/seatunnel/commit/d617b31304|3.0.0|
|[Fix][Core] Extend serialVersionUID checker coverage (#11257)|https://github.com/apache/seatunnel/commit/95c4ea206a|3.0.0|
|[Bug] [Postgres-CDC] Fix snapshot table schema lookup (#10843)|https://github.com/apache/seatunnel/commit/03346dc4df|3.0.0|
|[Feature][Connector-v2][Postgres-CDC] Allow replica identity other than full (#10334)|https://github.com/apache/seatunnel/commit/65793d174b|3.0.0|
|[Feature][CDC/JDBC] Add option to disable sampling-based sharding (#10604)|https://github.com/apache/seatunnel/commit/e4780bb1fb|3.0.0|
|[Fix][Connector-V2][Postgres-CDC] Skip createReplicationSlot when slotInfo is present (#10416)|https://github.com/apache/seatunnel/commit/b4a284ccc6|3.0.0|
|[Fix][Connector-V2] Fix NPE when heartbeat.action.query is configured (#10278)|https://github.com/apache/seatunnel/commit/1cfffc7094|2.3.13|
|[Improve] cdc related options (#10372)|https://github.com/apache/seatunnel/commit/106c369db2|2.3.13|
|[Feature][Core] Add plugin directory support for each connector (#9650)|https://github.com/apache/seatunnel/commit/4beb2b9336|2.3.12|
|[improve] jdbc options (#9541)|https://github.com/apache/seatunnel/commit/d041e5fb32|2.3.12|
|[Feature][Connectors-v2] Optimize the size of CDC JAR Files (#9546)|https://github.com/apache/seatunnel/commit/1dd19c6823|2.3.12|
|[Fix][Connector-V2] Fix postgres cdc with debezium_json format can not parse number without scale (#9052)|https://github.com/apache/seatunnel/commit/29cf3a76c7|2.3.11|
|[Improve][CDC] Extract duplicate code (#8906)|https://github.com/apache/seatunnel/commit/b922bb90e6|2.3.10|
|[Improve] restruct connector common options (#8634)|https://github.com/apache/seatunnel/commit/f3499a6eeb|2.3.10|
|[Improve][dist]add shade check rule (#8136)|https://github.com/apache/seatunnel/commit/51ef800016|2.3.9|
|[Improve][Connector-V2] Add pre-check for table enable cdc (#8152)|https://github.com/apache/seatunnel/commit/9a5da78176|2.3.9|
|[Feature][Connector-V2]Jdbc chunk split add  snapshotSplitColumn config #7794 (#7840)|https://github.com/apache/seatunnel/commit/b6c6dc0438|2.3.9|
|[Feature][Core] Support cdc task ddl restore for zeta (#7463)|https://github.com/apache/seatunnel/commit/8e322281ed|2.3.9|
|[Improve][PostgreSQL CDC]-PostgresSourceOptions description error (#7813)|https://github.com/apache/seatunnel/commit/57f47c2064|2.3.9|
|[Improve][CDC] Bump the version of debezium to 1.9.8.Final (#6740)|https://github.com/apache/seatunnel/commit/c3ac953524|2.3.6|
|[Improve][CDC] Close idle subtasks gorup(reader/writer) in increment phase (#6526)|https://github.com/apache/seatunnel/commit/454c339b9c|2.3.6|
|[Improve][JDBC Source] Fix Split can not be cancel (#6825)|https://github.com/apache/seatunnel/commit/ee3b7c3723|2.3.6|
|[Hotfix][Postgres-CDC/OpenGauss-CDC] Fix read data missing when restore (#6785)|https://github.com/apache/seatunnel/commit/67c32607e7|2.3.6|
|[Hotfix][Jdbc/CDC] Fix postgresql uuid type in jdbc read (#6684)|https://github.com/apache/seatunnel/commit/868ba4d7c7|2.3.6|
|[Improve] Improve read table schema in cdc connector (#6702)|https://github.com/apache/seatunnel/commit/a8c6cc6e0c|2.3.6|
|[Improve][Jdbc] Add quote identifier for sql (#6669)|https://github.com/apache/seatunnel/commit/849d748d3d|2.3.5|
|[Fix][Connector-V2] Fix connector support SPI but without no args constructor (#6551)|https://github.com/apache/seatunnel/commit/5f3c9c36a5|2.3.5|
|[Improve][CDC-Connector]Fix CDC option rule. (#6454)|https://github.com/apache/seatunnel/commit/1ea27afa87|2.3.5|
|[Improve][CDC] Optimize memory allocation for snapshot split reading (#6281)|https://github.com/apache/seatunnel/commit/4856645837|2.3.5|
|[Improve][API] Unify type system api(data &amp; type) (#5872)|https://github.com/apache/seatunnel/commit/b38c7edcc9|2.3.5|
|[Feature][Connector]update pgsql-cdc publication for add table (#6309)|https://github.com/apache/seatunnel/commit/2ad7d65236|2.3.5|
|[Improve][Postgres-CDC] Fix name typos (#6248)|https://github.com/apache/seatunnel/commit/2462f1c5f7|2.3.4|
|[Improve][Postgres-CDC] Update jdbc fetchsize (#6245)|https://github.com/apache/seatunnel/commit/c25beb9f8a|2.3.4|
| [Feature][Connector-V2][Postgres-cdc]Support for Postgres cdc (#5986)|https://github.com/apache/seatunnel/commit/97438b9402|2.3.4|

</details>
