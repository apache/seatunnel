## 3.1.0 Release Note

This is another important release for Apache SeaTunnel following version 3.0.0!

Version 3.1.0 includes 395 PRs and 11 new connectors, with a focus on Zeta engine stability and Connector-V2 data correctness. It also improves deployment, operations, and data transformation to make production data integration jobs more reliable.

Thank you to all contributors for their hard work on this update. We believe version 3.1.0 will bring the community a more stable and reliable data integration experience.

## Highlights

*   Native Kubernetes Application Mode: Deploy a dedicated Master and a fixed number of Workers per job, with automatic resource cleanup when the job finishes, providing resource isolation and on-demand resource release for one-off data processing jobs and teams running large-scale batch workloads.
*   Minimum Java version raised to 11, with support up to Java 17.

## New Connectors

*   connector-azure-event-hubs
*   connector-cdc-oceanbase
*   connector-file-smb
*   connector-google-analytics4
*   connector-http-asana
*   connector-http-intercom
*   connector-http-paypal
*   connector-http-splunk
*   connector-http-typeform
*   connector-syslog
*   connector-websocket

## Feature

*   [Feature][Connector-V2] Support Elasticsearch 9 in the Elasticsearch connector ([#13090](https://github.com/apache/seatunnel/pull/13090))
*   [Feature][Zeta] Support environment-variable object storage credential ([#12885](https://github.com/apache/seatunnel/pull/12885))
*   [Feature][STIP-41][Zeta] Add Native Zeta Application Mode on Kubernetes ([#12460](https://github.com/apache/seatunnel/pull/12460))
*   [Feature][Connector-V2][Iceberg] Support Iceberg table format v3 ([#12777](https://github.com/apache/seatunnel/pull/12777))
*   [Feature][Connector-V2] Add Typeform source connector ([#12844](https://github.com/apache/seatunnel/pull/12844))
*   [Feature][Connector-V2] Add Intercom source connector ([#12831](https://github.com/apache/seatunnel/pull/12831))
*   [Feature][Client] Add Java detection utility to Python SDK ([#12740](https://github.com/apache/seatunnel/pull/12740))
*   [Feature][Connector-V2] Add metadata-only connectivity dry-run for MongoDB source ([#12399](https://github.com/apache/seatunnel/pull/12399))
*   [Feature][seatunnel-cli] Diagnose a gradual slowdown from checkpoint history ([#12688](https://github.com/apache/seatunnel/pull/12688))
*   [Feature][Connector-V2] Add Sentry project error-events source ([#12451](https://github.com/apache/seatunnel/pull/12451))
*   [Feature][Connector-V2] Add ClickHouse sink connectivity dry-run ([#12686](https://github.com/apache/seatunnel/pull/12686))
*   [Feature][Connector-V2] Fix HTTP credential handling - add scheme check and exclude token from toString ([#12040](https://github.com/apache/seatunnel/pull/12040))
*   [Feature][Connector-V2] support websocket connector source ([#12465](https://github.com/apache/seatunnel/pull/12465))
*   [Feature][Connector-V2] Add Kafka sink connectivity dry-run ([#12400](https://github.com/apache/seatunnel/pull/12400))
*   [Feature][Connector-V2] Add checkpoint-aware ActiveMQ queue source ([#12341](https://github.com/apache/seatunnel/pull/12341))
*   [Feature][Connector-V2] Introduce PG base CDC abstraction for Postgres and OpenGauss ([#11029](https://github.com/apache/seatunnel/pull/11029))
*   [Feature][Connector-V2] Add Neo4j source connectivity dry-run ([#12433](https://github.com/apache/seatunnel/pull/12433))
*   [Feature][Transform-V2] Add GET_JSON_OBJECT SQL function ([#12666](https://github.com/apache/seatunnel/pull/12666))
*   [Feature][Connector-V2][AmazonDynamoDB] Support multi-table source reads ([#12665](https://github.com/apache/seatunnel/pull/12665))
*   [Feature][Connector-V2] Add MongoDB sink connectivity dry-run ([#12687](https://github.com/apache/seatunnel/pull/12687))
*   [Feature][Transform-V2] Add MD5 hash SQL function ([#12564](https://github.com/apache/seatunnel/pull/12564))
*   [Feature][Connector-V2] Add RabbitMQ source connectivity dry-run ([#12432](https://github.com/apache/seatunnel/pull/12432))
*   [Feature][Transform-V2] Add AES_ENCRYPT/AES_DECRYPT SQL functions ([#12403](https://github.com/apache/seatunnel/pull/12403))
*   [Feature][Transform-V2] Support connector-declared metadata fields in Metadata transform ([#12057](https://github.com/apache/seatunnel/pull/12057))
*   [Feature][Connector-V2] Expose MaxCompute client timeout and retry options ([#12390](https://github.com/apache/seatunnel/pull/12390))
*   [Feature][Connector-V2] Support --dry-run connect for Redis source and sink ([#12431](https://github.com/apache/seatunnel/pull/12431))
*   [Feature][seatunnel-cli] Parse job failures into structured fields ([#12537](https://github.com/apache/seatunnel/pull/12537))
*   [Feature][Zeta] Support Google Cloud Storage for checkpoint and IMap storage ([#12551](https://github.com/apache/seatunnel/pull/12551))
*   [Feature][Connector-V2] Add Azure Event Hubs source ([#12048](https://github.com/apache/seatunnel/pull/12048))
*   [Feature][Zeta] Show worker resource snapshots in the Web UI ([#12398](https://github.com/apache/seatunnel/pull/12398))
*   [Feature][Connector-V2] Add SMB source&sink connector ([#12297](https://github.com/apache/seatunnel/pull/12297))
*   [Feature][Connector-V2] Add OceanBase CDC source connector ([#11060](https://github.com/apache/seatunnel/pull/11060))
*   [Feature][Connector-V2] Support OpenMLDB multi-table source ([#12303](https://github.com/apache/seatunnel/pull/12303))
*   [Feature][connector-http] Add Splunk Source Connector and Unit Tests ([#12281](https://github.com/apache/seatunnel/pull/12281))
*   [Feature][Firebase] sink connector ([#12232](https://github.com/apache/seatunnel/pull/12232))
*   [Feature][Connector-V2][LocalFile] Support append-only text tailing ([#11819](https://github.com/apache/seatunnel/pull/11819))
*   [Feature][Connector-V2] Add PayPal Transaction Search source ([#12282](https://github.com/apache/seatunnel/pull/12282))
*   [Feature][Connector-V2] Add Google Analytics 4 report source ([#12280](https://github.com/apache/seatunnel/pull/12280))
*   [Feature][Connector-V2] Add RFC 5424 Syslog sink over TLS ([#12279](https://github.com/apache/seatunnel/pull/12279))
*   [Feature][Connector-V2] Support ADD COLUMN schema evolution for BigQuery ([#11577](https://github.com/apache/seatunnel/pull/11577))
*   [Improve][Core] Upgrade all SeaTunnel Shade dependencies to 3.0.1 ([#13265](https://github.com/apache/seatunnel/pull/13265))
*   [Improve][Zeta] Exclude engine_checkpoint_monitor from IMap persistence ([#13015](https://github.com/apache/seatunnel/pull/13015))
*   [Improve][API] Run multi-table timer flushes in parallel ([#13201](https://github.com/apache/seatunnel/pull/13201))
*   [Improve][API] Reduce queue handoff overhead in MultiTableSinkWriter ([#13196](https://github.com/apache/seatunnel/pull/13196))
*   [Improve][Connector-V2] Validate DB2 CDC source URL ([#12931](https://github.com/apache/seatunnel/pull/12931))
*   [Improve][Connector-V2][Doris] Warn when TIMESTAMP_TZ columns use the JVM default zone ([#12945](https://github.com/apache/seatunnel/pull/12945))
*   [Improve] Update snapshot version to 3.1.0 ([#12950](https://github.com/apache/seatunnel/pull/12950))
*   [Improve][Transform-V2] Add fast path for plain column projections in Zeta SQL engine ([#12528](https://github.com/apache/seatunnel/pull/12528))
*   [Improve][CI] Run labeler and Build-check workflows on workflow_run instead of pull_request_target ([#12725](https://github.com/apache/seatunnel/pull/12725))
*   [Improve][Zeta] Add generatedAt to the checkpoint overview response ([#12794](https://github.com/apache/seatunnel/pull/12794))
*   [Improve][CI] Increase `mysql-cdc-connector-it` ci timeout to 180 minutes ([#12946](https://github.com/apache/seatunnel/pull/12946))
*   [Improve][Zeta] Make the checkpoint monitor history size configurable ([#12789](https://github.com/apache/seatunnel/pull/12789))
*   [Improve][CI] Run the seatunnel-python-sdk test suite in CI ([#12857](https://github.com/apache/seatunnel/pull/12857))
*   [Improve][Transform-V2] Add opt-in validation for LLM boolean output ([#12194](https://github.com/apache/seatunnel/pull/12194))
*   [Improve][Connector-V2] Validate nonblank SQL Server CDC source URL ([#12764](https://github.com/apache/seatunnel/pull/12764))
*   [Improve][CI] Run only the E2E engines affected by the change ([#12812](https://github.com/apache/seatunnel/pull/12812))
*   [Improve][Connector-V2][JDBC] Derive the MySQL UNSIGNED guard from the converter constants ([#12742](https://github.com/apache/seatunnel/pull/12742))
*   [Improve][Connector-V2][Vitess] Migrate CDC source option validation to declarative OptionRule ([#12746](https://github.com/apache/seatunnel/pull/12746))
*   [Improve][Format] Optimize Debezium JSON SerDe allocation and throughput ([#12172](https://github.com/apache/seatunnel/pull/12172))
*   [Improve][Connector-V2] Validate HTTP sink URL is not blank ([#12773](https://github.com/apache/seatunnel/pull/12773))
*   [Improve][Connector-V2][MongoDB] Migrate CDC source option validation to declarative OptionRule ([#12735](https://github.com/apache/seatunnel/pull/12735))
*   [Improve][Connector-V2] Validate nonblank MaxCompute source endpoint ([#12690](https://github.com/apache/seatunnel/pull/12690))
*   [Improve][Connector-V2][Paimon] Add declarative notBlank validation to required options ([#12573](https://github.com/apache/seatunnel/pull/12573))
*   [Improve][Connector-V2] Validate IoTDB source SQL is not blank ([#12603](https://github.com/apache/seatunnel/pull/12603))
*   [Improve][Connector-V2] Improve ActiveMQ sink: fix resource leak and add config options ([#12698](https://github.com/apache/seatunnel/pull/12698))
*   [Improve] Update snapshot version to 3.0.1 ([#12682](https://github.com/apache/seatunnel/pull/12682))
*   [Improve][CI] Allow manual fork-build retrigger and clarify detection-failure guidance ([#12720](https://github.com/apache/seatunnel/pull/12720))
*   [Improve][CI] Skip updated-modules IT shards that have no modules ([#12475](https://github.com/apache/seatunnel/pull/12475))
*   [Improve][CLI] Install source setup in a project virtual environment ([#12464](https://github.com/apache/seatunnel/pull/12464))
*   [Improve][seatunnel-cli] Route engine REST calls through one module ([#12629](https://github.com/apache/seatunnel/pull/12629))
*   [Improve][Core] Raise minimum Java runtime to 11 and move CI to JDK 11/17 ([#11545](https://github.com/apache/seatunnel/pull/11545))
*   [Improve][seatunnel-cli] Keep the root cause when reporting a job failure ([#12628](https://github.com/apache/seatunnel/pull/12628))
*   [Improve][CLI] Classify planner intent by outcome rather than by verb ([#12604](https://github.com/apache/seatunnel/pull/12604))
*   [Improve][seatunnel-cli] Add a fast benchmark preset and keep static runs off Docker ([#12538](https://github.com/apache/seatunnel/pull/12538))
*   [Improve][Connector-V2] Correct two RocketMQ comments that no longer hold ([#12438](https://github.com/apache/seatunnel/pull/12438))
*   [Improve][Zeta] Bound the size of REST log-content responses ([#12299](https://github.com/apache/seatunnel/pull/12299))
*   [Improve][API] Use HashUtils.bucketIndex in MultiTableSinkWriter routing ([#12290](https://github.com/apache/seatunnel/pull/12290))
*   [Improve][Connector-V2] Validate ClickHouse source host as nonblank ([#12574](https://github.com/apache/seatunnel/pull/12574))
*   [Improve][Connector-V2] Validate SelectDB Cloud required options ([#12351](https://github.com/apache/seatunnel/pull/12351))
*   [Improve][Connector-V2][Easysearch] Declarative optionRule value constraints for source and sink ([#12414](https://github.com/apache/seatunnel/pull/12414))
*   [Improve][Connector-V2][Fake] Declarative optionRule value constraints for fake source ([#12404](https://github.com/apache/seatunnel/pull/12404))
*   [Improve][Connector-V2] Validate RabbitMQ sink queue_name as nonblank ([#12519](https://github.com/apache/seatunnel/pull/12519))
*   [Improve][CI] Upload test reports and list failed tests when a test job fails ([#12470](https://github.com/apache/seatunnel/pull/12470))
*   [Improve][Connector-V2] Migrate SensorsData validation to OptionRule ([#12315](https://github.com/apache/seatunnel/pull/12315))
*   [Improve][Connector-V2] Validate SLS required options ([#12352](https://github.com/apache/seatunnel/pull/12352))
*   [Improve][Zeta] Narrow daily core benchmark suite ([#12307](https://github.com/apache/seatunnel/pull/12307))
*   [Improve][Zeta] Optimize WAL recovery memory usage ([#11494](https://github.com/apache/seatunnel/pull/11494))
*   [Improve][Connector-V2] Validate Tablestore connection options ([#12424](https://github.com/apache/seatunnel/pull/12424))
*   [Improve][Connector-V2] Validate Fluss sink bootstrap servers ([#12422](https://github.com/apache/seatunnel/pull/12422))
*   [Improve][Connector-V2] Note the multi-topic early return in currentOffsets ([#12417](https://github.com/apache/seatunnel/pull/12417))
*   [Improve][Connector-V2] Use HashUtils.bucketIndex in AmazonDocumentDB enumerator ([#12395](https://github.com/apache/seatunnel/pull/12395))
*   [Improve][Connector-V2][Prometheus] Retry retryable remote-write failures and tolerate duplicate/out-of-order rejections ([#11974](https://github.com/apache/seatunnel/pull/11974))
*   [Improve][E2E] Reuse SeaTunnel container for selected test classes ([#11626](https://github.com/apache/seatunnel/pull/11626))
*   [Improve][CI] Run the seatunnel-cli Python test suite in CI ([#12300](https://github.com/apache/seatunnel/pull/12300))
*   [Improve][Zeta] Apply the finished-jobs page before the per-job metrics and DAG lookups ([#12130](https://github.com/apache/seatunnel/pull/12130))

## Bug Fix

*   [Fix][Zeta] Remove obsolete checkpoint overview storage benchmark ([#13255](https://github.com/apache/seatunnel/pull/13255))
*   [Fix][Connector-V2] Encode the HTTP source response body as UTF-8 ([#13011](https://github.com/apache/seatunnel/pull/13011))
*   [Fix][Connector-V2] Fix Doris source duplicate table detection and error code typo ([#13172](https://github.com/apache/seatunnel/pull/13172))
*   [Fix][Tools] Support 3.x and later in connector changelogs ([#13206](https://github.com/apache/seatunnel/pull/13206))
*   [Fix][Build] Correct source distribution contents ([#13098](https://github.com/apache/seatunnel/pull/13098))
*   [Fix][Transform-V2] Rebind Replace fields on schema refresh ([#12365](https://github.com/apache/seatunnel/pull/12365))
*   [Fix][Zeta] Isolate REST test HTTP ports from Hazelcast discovery ([#13030](https://github.com/apache/seatunnel/pull/13030))
*   [Fix][Connector-V2] Preserve NULL transitions in DB2 upserts ([#13037](https://github.com/apache/seatunnel/pull/13037))
*   [Fix][Format] Preserve Avro byte buffer bounds and row ownership ([#12906](https://github.com/apache/seatunnel/pull/12906))
*   [Fix][Connector-V2][JDBC] Prevent checkpoint success after losing uncommitted batches ([#13054](https://github.com/apache/seatunnel/pull/13054))
*   [Fix][Connector-V2] Keep the column of a null field in the Druid sink ([#13046](https://github.com/apache/seatunnel/pull/13046))
*   [Fix][License] Restore canonical Apache License appendix ([#13064](https://github.com/apache/seatunnel/pull/13064))
*   [Fix][Connector-V2] Stop emitting a Databend TIME column type ([#13040](https://github.com/apache/seatunnel/pull/13040))
*   [Fix][Transform-V2] Preserve input row kind when extracting CDC metadata ([#12580](https://github.com/apache/seatunnel/pull/12580))
*   [Fix][Connector-V2][JDBC] Make the SQL structure guards locale independent ([#12996](https://github.com/apache/seatunnel/pull/12996))
*   [Fix][Zeta] Read submit-job options from the query string only ([#12921](https://github.com/apache/seatunnel/pull/12921))
*   [Fix][Format][Protobuf] Produce no row for a tombstone instead of failing ([#12953](https://github.com/apache/seatunnel/pull/12953))
*   [Fix][Connector-V2] Add opt-in collision-safe Elasticsearch IDs ([#13003](https://github.com/apache/seatunnel/pull/13003))
*   [Fix][Connector-V2] Restore legacy RabbitMQ checkpoint state ([#11872](https://github.com/apache/seatunnel/pull/11872))
*   [Fix][Zeta] Preserve idle reader state after master switch ([#12042](https://github.com/apache/seatunnel/pull/12042))
*   [Fix][Connector-V2] Keep TIMESTAMP microseconds in the Lance sink ([#13043](https://github.com/apache/seatunnel/pull/13043))
*   [Fix][Transform-V2] Widen vector operands before arithmetic ([#12337](https://github.com/apache/seatunnel/pull/12337))
*   [Fix][E2E] Wait for LocalStack starter script copy before execution ([#13029](https://github.com/apache/seatunnel/pull/13029))
*   [Fix][Connector-V2] Exclude columns not published by pgoutput from the Postgres-CDC schema ([#12561](https://github.com/apache/seatunnel/pull/12561))
*   [Fix][Transform-V2] Fail CAST to BIGINT on out-of-range numeric input ([#12899](https://github.com/apache/seatunnel/pull/12899))
*   [Fix][CI] Pin the Python SDK workflow to ubuntu-24.04 so Python 3.9 can be installed ([#12994](https://github.com/apache/seatunnel/pull/12994))
*   [Fix][Zeta] Hand the source checkpoint lock to barrier injectors instead of racing them ([#12316](https://github.com/apache/seatunnel/pull/12316))
*   [Fix][Connector-V2][Jdbc] Handle empty sink primary keys ([#11893](https://github.com/apache/seatunnel/pull/11893))
*   [Fix][Connector-V2] Preserve MySQL DATETIME values across time zones ([#13025](https://github.com/apache/seatunnel/pull/13025))
*   [Fix][Connector-V2] Fix MongoDB CDC non-exactly-once snapshot deadlock ([#13018](https://github.com/apache/seatunnel/pull/13018))
*   [Fix][CDC] Keep per-table restored offsets when tables are added across a savepoint ([#13006](https://github.com/apache/seatunnel/pull/13006))
*   [Fix][Connector-V2] Preserve numeric JDBC partition bounds ([#12584](https://github.com/apache/seatunnel/pull/12584))
*   [Fix][API] Forward restoreCommit to each table in MultiTableSinkAggregatedCommitter ([#12807](https://github.com/apache/seatunnel/pull/12807))
*   [Fix][Transform-V2] Preserve exact numeric comparisons in SQL predicates ([#12318](https://github.com/apache/seatunnel/pull/12318))
*   [Fix][Connector-V2] Complete batch Kafka splits after control records ([#11486](https://github.com/apache/seatunnel/pull/11486))
*   [Fix][Zeta] Fix lite member (worker) cannot join cluster when started before master ([#12755](https://github.com/apache/seatunnel/pull/12755))
*   [Fix][Core] Fix application CLI validation and status handling ([#12898](https://github.com/apache/seatunnel/pull/12898))
*   [Fix][E2E] Wait for MySQL latest-stop completion before checking sink values ([#12997](https://github.com/apache/seatunnel/pull/12997))
*   [Fix][E2E] Retry Fluss readiness connection failures ([#13007](https://github.com/apache/seatunnel/pull/13007))
*   [Fix][Zeta] Wait for completion callback in task finish test ([#12980](https://github.com/apache/seatunnel/pull/12980))
*   [Fix][Connector-V2] Read a null struct or map cell as null ([#12969](https://github.com/apache/seatunnel/pull/12969))
*   [Fix][Core] Fix ServerExecuteCommand rejects explicit master_and_worker cluster role ([#12963](https://github.com/apache/seatunnel/pull/12963))
*   [Fix][Format] Produce no row for a kafka connect json tombstone ([#12967](https://github.com/apache/seatunnel/pull/12967))
*   [Fix][Transform-V2] Rebuild chained transform routing after deserialization ([#12990](https://github.com/apache/seatunnel/pull/12990))
*   [Fix][Connector-V2] Truncate the table path database ([#12877](https://github.com/apache/seatunnel/pull/12877))
*   [Fix][CI] Isolate Hazelcast test port ranges in additional E2E tests ([#12778](https://github.com/apache/seatunnel/pull/12778))
*   [Fix][Zeta] Align Hazelcast join port try count with port range ([#11868](https://github.com/apache/seatunnel/pull/11868))
*   [Fix][Connector-V2][JDBC] Report the declared length of Dameng NVARCHAR and NVARCHAR2 ([#12882](https://github.com/apache/seatunnel/pull/12882))
*   [Fix][Zeta] Harden slot allocation recovery state ([#12948](https://github.com/apache/seatunnel/pull/12948))
*   [Fix][Connector-V2] Restore bounded file splits after reader registration ([#12942](https://github.com/apache/seatunnel/pull/12942))
*   [Fix][Transform-V2] Make SQL transform keyword lookup locale independent ([#12495](https://github.com/apache/seatunnel/pull/12495))
*   [Fix][Connector-V2] Fix CDC pre-1970 sub-millisecond timestamp conversion ([#12957](https://github.com/apache/seatunnel/pull/12957))
*   [Fix][Connector-V2] Read every arrow record batch, not only the first ([#12650](https://github.com/apache/seatunnel/pull/12650))
*   [Fix][Connector-V2] Fix Arrow microsecond timestamp with timezone conversion ([#12616](https://github.com/apache/seatunnel/pull/12616))
*   [Fix][Zeta] Exec the JVM in seatunnel-cluster.sh so SIGTERM reaches it ([#12710](https://github.com/apache/seatunnel/pull/12710))
*   [Fix][Connector-V2][Jdbc] Write the net effect of each key when reducing the sink batch ([#12808](https://github.com/apache/seatunnel/pull/12808))
*   [Fix][Zeta] Preserve cancellation after worker failure ([#12377](https://github.com/apache/seatunnel/pull/12377))
*   [Fix][Zeta] Keep IMap entry expiration times across restarts in the file map store ([#12973](https://github.com/apache/seatunnel/pull/12973))
*   [Fix][Connector-V2] Remove incorrect HTTP entity content encoding ([#11281](https://github.com/apache/seatunnel/pull/11281))
*   [Fix][Zeta] Reset metrics after pipeline recovery ([#11495](https://github.com/apache/seatunnel/pull/11495))
*   [Fix][Format] Add opt-in nullable Avro ROW layout ([#12900](https://github.com/apache/seatunnel/pull/12900))
*   [Fix][API] Correct nested row byte accounting ([#12305](https://github.com/apache/seatunnel/pull/12305))
*   [Fix][Connector-V2][Kafka] Skip a Kafka tombstone instead of failing the Avro source ([#12927](https://github.com/apache/seatunnel/pull/12927))
*   [Fix][Connector-V2] Skip CSV preamble before parsing the header ([#12936](https://github.com/apache/seatunnel/pull/12936))
*   [Fix][Format] Reject incompatible JSON array and map shapes ([#12962](https://github.com/apache/seatunnel/pull/12962))
*   [Fix][E2E] Skip suites when all selected containers are disabled ([#12895](https://github.com/apache/seatunnel/pull/12895))
*   [Fix][CI] Fix backend checks on Ubuntu 26.04 runners ([#12964](https://github.com/apache/seatunnel/pull/12964))
*   [Fix][CDC] Wait for enumerator ack before clearing finished snapshot splits ([#12163](https://github.com/apache/seatunnel/pull/12163))
*   [Fix][Connector-V2] Stream PostgreSQL sampling results with JDBC cursor ([#12731](https://github.com/apache/seatunnel/pull/12731))
*   [Fix][Connector-File] Fix child_ids output inconsistent with the expected result ([#12861](https://github.com/apache/seatunnel/pull/12861))
*   [Fix][seatunnel-cli] Correct the --diagnose docs and two out-of-order restore-rule branches ([#12784](https://github.com/apache/seatunnel/pull/12784))
*   [Fix][Transform-V2] Reject array input for the MD5 function ([#12717](https://github.com/apache/seatunnel/pull/12717))
*   [Fix][Zeta] Restore jobs from their own latest checkpoint on master failover ([#12387](https://github.com/apache/seatunnel/pull/12387))
*   [Fix][Transform-V2] Restore archived LLM wrapper deserialization ([#12938](https://github.com/apache/seatunnel/pull/12938))
*   [Fix][Zeta] Restore legacy job status WAL entries ([#11781](https://github.com/apache/seatunnel/pull/11781))
*   [Fix][Transform-V2] Validate array length by element count ([#12320](https://github.com/apache/seatunnel/pull/12320))
*   [Fix][Transform-V2] Preserve exact numeric array extrema ([#12364](https://github.com/apache/seatunnel/pull/12364))
*   [Fix][E2E] Recreate Fluss test tables that never get a bucket leader ([#12852](https://github.com/apache/seatunnel/pull/12852))
*   [Fix][Connector-V2] Fix NPE when reading Parquet source on Spark (missing tableId) ([#11516](https://github.com/apache/seatunnel/pull/11516))
*   [Fix][Connector-V2] Reject unsupported Map columns in the Lance sink ([#12846](https://github.com/apache/seatunnel/pull/12846))
*   [Fix][Connector-V2] Run the final Databend CDC merge when the writer closes ([#12848](https://github.com/apache/seatunnel/pull/12848))
*   [Fix][Connector-V2][RocketMQ] Close consumer client before interrupting its executor ([#12099](https://github.com/apache/seatunnel/pull/12099))
*   [Fix][Connector-V2] Preserve ClickHouse sink rows with incomplete keys ([#12840](https://github.com/apache/seatunnel/pull/12840))
*   [Fix][Connector-V2][Jdbc] Add numeric_special_value_as_null option for PostgreSQL ([#12792](https://github.com/apache/seatunnel/pull/12792))
*   [Fix][Connector-V2][MySQL-CDC] Keep NULL in nullable unique key columns of tables without a primary key ([#12608](https://github.com/apache/seatunnel/pull/12608))
*   [Fix][Connector-File] Fix Excel column mapping and read_columns projection ([#12768](https://github.com/apache/seatunnel/pull/12768))
*   [Fix][Zeta] Compare basic auth credentials without a timing discrepancy ([#12595](https://github.com/apache/seatunnel/pull/12595))
*   [Fix][Transform-V2] Fail CAST to INT on out-of-range numeric input ([#12605](https://github.com/apache/seatunnel/pull/12605))
*   [Fix][Connector-V2] Declare missing source options in file source factories ([#12817](https://github.com/apache/seatunnel/pull/12817))
*   [Fix][Connector-V2] Resolve late PostgreSQL CDC type names by search path ([#12836](https://github.com/apache/seatunnel/pull/12836))
*   [Fix][Connector-V2] Add opt-in collision-safe Easysearch and Typesense IDs ([#12839](https://github.com/apache/seatunnel/pull/12839))
*   [Fix][Connector-V2][File] Pin the INT96 timestamp calendar to Locale.ROOT ([#12826](https://github.com/apache/seatunnel/pull/12826))
*   [Fix][Connector-File] Reuse the connector FileSystem for Parquet and ORC reads/writes ([#12368](https://github.com/apache/seatunnel/pull/12368))
*   [Fix][Connector-V2] Apply Qdrant API key to source and sink clients ([#12843](https://github.com/apache/seatunnel/pull/12843))
*   [Fix][Connector-V2] Support Dameng local time zone and interval types ([#12791](https://github.com/apache/seatunnel/pull/12791))
*   [Fix][Transform-V2] Accept table_transform-only configurations ([#12804](https://github.com/apache/seatunnel/pull/12804))
*   [Fix][Connector-V2][Doris] Support IPV4 and IPV6 columns ([#12761](https://github.com/apache/seatunnel/pull/12761))
*   [Fix][Connector-V2] Support Dameng national character types in JDBC connector ([#11684](https://github.com/apache/seatunnel/pull/11684))
*   [Fix][Connector-V2] Fix Arrow memory leak by correcting close order in ArrowToSeatunnelRowReader ([#11929](https://github.com/apache/seatunnel/pull/11929))
*   [Fix][Connector-CDC] Reassign restored splits to waiting readers after checkpoint recovery ([#11677](https://github.com/apache/seatunnel/pull/11677))
*   [Fix][Connector-V2] Verify StarRocks label state for Publish Timeout before releasing batch ([#12692](https://github.com/apache/seatunnel/pull/12692))
*   [Fix][Connector-V2] Fix InfluxDB sink retention policy, null timestamp fallback and flush retry loop ([#12741](https://github.com/apache/seatunnel/pull/12741))
*   [Fix][Connector-V2] Fix Hudi catalog schema conversion for MAP and ARRAY columns ([#12770](https://github.com/apache/seatunnel/pull/12770))
*   [Fix][Connector-V2] Fix Tablestore sink ClassCastException for numeric columns ([#12786](https://github.com/apache/seatunnel/pull/12786))
*   [Fix][Connector-V2] Do not merge Jdbc sink rows whose key contains NULL ([#12803](https://github.com/apache/seatunnel/pull/12803))
*   [Fix][Zeta] Restore the Web UI route when the URL fragment is percent-encoded ([#12775](https://github.com/apache/seatunnel/pull/12775))
*   [Fix][Connector-V2] Apply date, time and datetime formats in the XML file sink ([#12801](https://github.com/apache/seatunnel/pull/12801))
*   [Fix][Connector-V2] Declare all Jdbc source and sink options read by the connector ([#12800](https://github.com/apache/seatunnel/pull/12800))
*   [Fix][API] Reject multi_table_sink_replica below 1 with a diagnosable error ([#12568](https://github.com/apache/seatunnel/pull/12568))
*   [Fix][CI] Fix missed detections and false positives in seatunnel-ci-tools checks ([#12676](https://github.com/apache/seatunnel/pull/12676))
*   [Fix][API] Copy the arrays passed to the SeaTunnelRowType constructor ([#12527](https://github.com/apache/seatunnel/pull/12527))
*   [Fix][Connector-V2][CDC] Do not use a nullable column as the snapshot split column ([#12597](https://github.com/apache/seatunnel/pull/12597))
*   [Fix][seatunnel-cli] Default the engine REST base URL to the Jetty HTTP port ([#12722](https://github.com/apache/seatunnel/pull/12722))
*   [Fix][Connector-V2] Wait for Doris data visibility ([#11562](https://github.com/apache/seatunnel/pull/11562))
*   [Fix][Connector-V2][JDBC] Accept the synthetic SET UNSIGNED type name for MySQL SET columns ([#10453](https://github.com/apache/seatunnel/pull/10453))
*   [Fix][Connector-V2][JDBC] Derive the MySQL UNSIGNED attribute only for numeric columns ([#12333](https://github.com/apache/seatunnel/pull/12333))
*   [Fix][Connector-V2] Support YashanDB VARCHAR2 and NVARCHAR2 type conversion ([#11686](https://github.com/apache/seatunnel/pull/11686))
*   [Fix][Connector-CDC] Resolve stop.mode=latest stop offset at binlog phase start ([#11885](https://github.com/apache/seatunnel/pull/11885))
*   [Fix][Connector-V2] Fail the ClickHouse stream reader on interrupt instead of polling forever ([#12727](https://github.com/apache/seatunnel/pull/12727))
*   [Fix][Connector-V2] Fail fast when the Postgres-CDC replication slot has been invalidated ([#12587](https://github.com/apache/seatunnel/pull/12587))
*   [Fix][Connector-V2][Doris] Close the previous split's value reader before the next split ([#12718](https://github.com/apache/seatunnel/pull/12718))
*   [Fix][Connector-V2][Lance] Write one fragment per flush instead of one per buffered row ([#12738](https://github.com/apache/seatunnel/pull/12738))
*   [Fix][E2E] Prevent direct memory exhaustion in Flink 1.20 Cassandra tests ([#12756](https://github.com/apache/seatunnel/pull/12756))
*   [Fix][Zeta][Connector-V2] Cancel in-flight JDBC source queries ([#12420](https://github.com/apache/seatunnel/pull/12420))
*   [Fix][Transform-V2] Make calcite, nlpmodel and validator case conversion locale independent ([#12618](https://github.com/apache/seatunnel/pull/12618))
*   [Fix][Connector-V2][TiDB-CDC] Emit null instead of NPE for columns missing from live table info ([#12696](https://github.com/apache/seatunnel/pull/12696))
*   [Fix][Connector-V2][CDC] Validate startup.specific-offset.* options are only bound under STARTUP_MODE = SPECIFIC ([#12708](https://github.com/apache/seatunnel/pull/12708))
*   [Fix][Connector-V2] Fix file sink checkpoint write race ([#11103](https://github.com/apache/seatunnel/pull/11103))
*   [Fix][Connector-V2] Fix MySQL CDC snapshot split error with non-consecutive primary key IDs ([#12463](https://github.com/apache/seatunnel/pull/12463))
*   [Fix][E2E] Fix task recovery hangs and PostgreSQL CDC LSN handling ([#12454](https://github.com/apache/seatunnel/pull/12454))
*   [Fix][Connector-V2] Apply Postgres-CDC ADD COLUMN on the first RELATION with pgjdbc 42.7.5+ ([#12547](https://github.com/apache/seatunnel/pull/12547))
*   [Fix][Connector-V2][JDBC] Stream OceanBase Oracle source reads instead of buffering the full result set ([#12530](https://github.com/apache/seatunnel/pull/12530))
*   [Fix][Connector-V2] Preserve Cassandra bootstrap contact points ([#12429](https://github.com/apache/seatunnel/pull/12429))
*   [Fix][Connector-V2][MySQL-CDC] Keep the SET/ENUM option list when rebuilding the source type ([#12355](https://github.com/apache/seatunnel/pull/12355))
*   [Fix][Connector-V2] Close the Iceberg table loader even when the record writer fails ([#12326](https://github.com/apache/seatunnel/pull/12326))
*   [Fix][Connector-V2] Stop the Doris reader polling forever when the async fetch fails ([#12325](https://github.com/apache/seatunnel/pull/12325))
*   [Fix][Connector-V2] Surface ClickHouse stream read failures instead of ending the split ([#12324](https://github.com/apache/seatunnel/pull/12324))
*   [Fix][Format] Serialize TIMESTAMP_TZ as session-timezone wall-clock for Doris sink ([#11524](https://github.com/apache/seatunnel/pull/11524))
*   [Fix][Transform-V2] Handle signed collection expressions ([#12636](https://github.com/apache/seatunnel/pull/12636))
*   [Fix][Assert] Use input schema for omitted field_type ([#12671](https://github.com/apache/seatunnel/pull/12671))
*   [Fix][Connector-V2] Handle concurrent SFTP directory creation ([#12689](https://github.com/apache/seatunnel/pull/12689))
*   [Fix][CLI] Resume the most recently active session ([#12632](https://github.com/apache/seatunnel/pull/12632))
*   [Fix][Connector-V2] Generate DuckDB SQL with locale independent digits ([#12576](https://github.com/apache/seatunnel/pull/12576))
*   [Fix][Connector-V2] Preserve DuckDB TIME microseconds ([#12541](https://github.com/apache/seatunnel/pull/12541))
*   [Fix][Connector-V2] Keep RocketMQ group offsets when a later topic has no retry route ([#12648](https://github.com/apache/seatunnel/pull/12648))
*   [Fix][CLI] Redact credentials in conversation summaries ([#12633](https://github.com/apache/seatunnel/pull/12633))
*   [Fix][CLI] Clear pending clarification on session changes ([#12634](https://github.com/apache/seatunnel/pull/12634))
*   [Fix][Connector-V2] Quote SQL Server table path in DROP TABLE ([#12596](https://github.com/apache/seatunnel/pull/12596))
*   [Fix][CI] Ignore deepwiki.com in the dead-link check ([#12639](https://github.com/apache/seatunnel/pull/12639))
*   [Fix][CI] Build only the current and latest released docs in the website check ([#12614](https://github.com/apache/seatunnel/pull/12614))
*   [Fix][Connector-V2][Assert] Evaluate row-count rules once per table after the last writer closes ([#12216](https://github.com/apache/seatunnel/pull/12216))
*   [Fix][Connector-V2][JDBC] Support column names containing spaces in named-parameter SQL ([#12529](https://github.com/apache/seatunnel/pull/12529))
*   [Fix][Connector-V2] Support PostgreSQL enum columns in the Postgres catalog and CDC ([#12588](https://github.com/apache/seatunnel/pull/12588))
*   [Fix][Transform-V2] Preserve vector input buffers during SQL evaluation ([#12335](https://github.com/apache/seatunnel/pull/12335))
*   [Fix][Connector-V2] Guard null MongoDB session during committer close ([#12600](https://github.com/apache/seatunnel/pull/12600))
*   [Fix][Connector-V2] Preserve unknown DuckDB BIT and ENUM lengths ([#12593](https://github.com/apache/seatunnel/pull/12593))
*   [Fix][Connector-V2] Escape DuckDB catalog metadata literals ([#12503](https://github.com/apache/seatunnel/pull/12503))
*   [Fix][Connector-V2] Match DuckDB type names independently of locale ([#12582](https://github.com/apache/seatunnel/pull/12582))
*   [Fix][Connector-V2] Omit empty UPDATE SET in JDBC MERGE for all-key tables ([#11730](https://github.com/apache/seatunnel/pull/11730))
*   [Fix][Connector-V2] Support special characters in column names for ClickHouse sink named-parameter SQL ([#12526](https://github.com/apache/seatunnel/pull/12526))
*   [Fix][E2E] Always stop E2E engine containers in teardown ([#12481](https://github.com/apache/seatunnel/pull/12481))
*   [Fix][CI] Stop the nightly backend run and dev push builds from cancelling each other ([#12467](https://github.com/apache/seatunnel/pull/12467))
*   [Fix][CI] Retry transient Maven Central errors and back off the wrapper download ([#12468](https://github.com/apache/seatunnel/pull/12468))
*   [Fix][seatunnel-cli] Treat an exported empty value as a resolved placeholder ([#12536](https://github.com/apache/seatunnel/pull/12536))
*   [Fix][Connector-V2] Fix Kudu client deadlock in Flink JobManager on JDK 11 ([#12480](https://github.com/apache/seatunnel/pull/12480))
*   [Fix][CI] Use selected tag for Docker publishing ([#12550](https://github.com/apache/seatunnel/pull/12550))
*   [Fix][CI] Keep the mirrored Build check accurate for re-runs and timeouts ([#12473](https://github.com/apache/seatunnel/pull/12473))
*   [Fix][CI] Check only changed markdown files for dead links on branch pushes ([#12469](https://github.com/apache/seatunnel/pull/12469))
*   [Fix][CI] Route engine, UDF and standalone-module changes to their tests ([#12474](https://github.com/apache/seatunnel/pull/12474))
*   [Fix][Connector-V2] Filter Postgres CDC database discovery ([#11399](https://github.com/apache/seatunnel/pull/11399))
*   [Fix][Connector-V2] Support FTP append data mode ([#11554](https://github.com/apache/seatunnel/pull/11554))
*   [Fix][E2E] Stabilize checkpoint trigger dispatch failure test ([#12449](https://github.com/apache/seatunnel/pull/12449))
*   [Fix][Connector-V2] Stop bundling debezium-core in the OceanBase CDC jar ([#12477](https://github.com/apache/seatunnel/pull/12477))
*   [Fix][CI] Wait for MinIO readiness in S3FileWithFilterIT and fix the benchmark MinIO images ([#12478](https://github.com/apache/seatunnel/pull/12478))
*   [Fix][Zeta] Resolve each member's bound REST HTTP port ([#12298](https://github.com/apache/seatunnel/pull/12298))
*   [Fix][CDC] Keep the live schema on restore when schema change propagation is disabled ([#12321](https://github.com/apache/seatunnel/pull/12321))
*   [Fix][Connector-V2] Deduplicate table ids in PostgreSQL CDC table discovery to support HighGo ([#12435](https://github.com/apache/seatunnel/pull/12435))
*   [Fix][CI] Give jdbc-connectors-it-part-1 and transform-v2-it-part-2 headroom over their real durations ([#12445](https://github.com/apache/seatunnel/pull/12445))
*   [Fix][Flink] Process same-timestamp schema restore events ([#12306](https://github.com/apache/seatunnel/pull/12306))
*   [Fix][Zeta] Render Zeta health metrics with a stable locale and bound REST fanout ([#12161](https://github.com/apache/seatunnel/pull/12161))
*   [Fix][Connector-V2] Honor Redis named-user authentication ([#12402](https://github.com/apache/seatunnel/pull/12402))
*   [Fix][E2E] Retry and route-guard the remaining RocketMqIT offset reads ([#12392](https://github.com/apache/seatunnel/pull/12392))
*   [Fix][Connector-V2] Surface RocketMQ group-offset lookup failures instead of rewinding ([#12349](https://github.com/apache/seatunnel/pull/12349))
*   [Fix][Connector-V2] Preserve disabled CDC schema behavior ([#12346](https://github.com/apache/seatunnel/pull/12346))
*   [Fix][Connector-V2] Make MqttSourceConfig serializable ([#12406](https://github.com/apache/seatunnel/pull/12406))
*   [Fix][Zeta] Skip connector jar operations on master-only nodes ([#12023](https://github.com/apache/seatunnel/pull/12023))
*   [Fix][Format] Handle heterogeneous numeric JSON fields ([#11415](https://github.com/apache/seatunnel/pull/11415))
*   [Fix][Connector-V2] Parse offset timestamps in Iceberg delete predicates ([#12286](https://github.com/apache/seatunnel/pull/12286))
*   [Fix][E2E] Warm the RocketMQ topic route at every point of use ([#12323](https://github.com/apache/seatunnel/pull/12323))
*   [Fix][Connector-File] Clear fileSystem reference on HadoopFileSystemProxy close ([#12369](https://github.com/apache/seatunnel/pull/12369))
*   [Fix][CI] Give the all-modules unit-test job headroom over its real duration ([#12374](https://github.com/apache/seatunnel/pull/12374))
*   [Fix][Connector-V2] Propagate HTTP sink write failures ([#12312](https://github.com/apache/seatunnel/pull/12312))
*   [Fix][Core] Exclude node_modules from src distribution ([#12389](https://github.com/apache/seatunnel/pull/12389))
*   [Fix][Build] Enforce consistent line endings ([#12379](https://github.com/apache/seatunnel/pull/12379))
*   [Fix][Connector-V2] Fix JDBC batch data error masking ([#11474](https://github.com/apache/seatunnel/pull/11474))
*   [Fix][CI] Pin Docker publishing actions to approved SHAs ([#12356](https://github.com/apache/seatunnel/pull/12356))
*   [Fix][Connector-V2][Paimon] Register writer state serializer ([#11978](https://github.com/apache/seatunnel/pull/11978))
*   [Fix][E2E] Unify testcontainers version to 1.21.4 across all e2e connectors ([#11201](https://github.com/apache/seatunnel/pull/11201))
*   [Fix][Zeta] Bound retries for metrics snapshot updates ([#12227](https://github.com/apache/seatunnel/pull/12227))
*   [Fix][Connector-V2] Support secure passive RabbitMQ connections ([#11576](https://github.com/apache/seatunnel/pull/11576))
*   [Fix][CI] Use MinIO mirror for S3 dry-run tests ([#12302](https://github.com/apache/seatunnel/pull/12302))
*   [Fix][Zeta] Checkpoint restore remap duplicates subtask state for parallelism > 1 ([#12195](https://github.com/apache/seatunnel/pull/12195))
*   [Bug][Zeta] Surface periodic checkpoint trigger failures ([#12448](https://github.com/apache/seatunnel/pull/12448))
*   [Bug][Connector-V2][Http] Release the checkpoint lock while waiting out poll_interval_millis ([#12412](https://github.com/apache/seatunnel/pull/12412))
*   [Bug][Connector-V2] Ensure generated MySQL primary keys are not null ([#12425](https://github.com/apache/seatunnel/pull/12425))

## Docs

*   [Docs][Connector-V2] Improve PostgreSql Rabbitmq Sls Hudi and Qdrant connector docs ([#11779](https://github.com/apache/seatunnel/pull/11779))
*   [Docs] Fix connector and common-option doc inaccuracies verified against source code ([#13062](https://github.com/apache/seatunnel/pull/13062))
*   [Docs] Split incompatible-changes.md into per-release sections ([#13065](https://github.com/apache/seatunnel/pull/13065))
*   [Docs] Fix missing sections in Chinese docs to align with English documentation ([#13019](https://github.com/apache/seatunnel/pull/13019))
*   [Docs][Connector-V2] Improve Paimon StarRocks and RabbitMQ connector docs ([#11705](https://github.com/apache/seatunnel/pull/11705))
*   [Docs][Connector-V2] Clarify LocalFile CSV header semantics ([#11606](https://github.com/apache/seatunnel/pull/11606))
*   [Docs][Connector-V2] Improve Clickhouse Http MongoDB Redis and Easysearch connector docs ([#11694](https://github.com/apache/seatunnel/pull/11694))
*   [Docs] Fix DuckDB, Kingbase, SmbFile, Rabbitmq and other connector doc inaccuracies verified against source code ([#12951](https://github.com/apache/seatunnel/pull/12951))
*   [Docs] Fix connector, transform and common-option doc inaccuracies verified against source code ([#12941](https://github.com/apache/seatunnel/pull/12941))
*   [Docs] Fix getting-started and engine docs verified against source code ([#12693](https://github.com/apache/seatunnel/pull/12693))
*   [Docs] Fix connector doc inaccuracies verified against source code ([#12615](https://github.com/apache/seatunnel/pull/12615))
*   [Docs] Fix option and config inaccuracies across transform, Zeta engine and connector docs ([#12423](https://github.com/apache/seatunnel/pull/12423))
*   [Docs] Fix CDC, InfluxDB, file and IM connector doc inaccuracies verified against source code ([#12771](https://github.com/apache/seatunnel/pull/12771))
*   [Docs] Fix Kafka, Hive, Jdbc and OLAP connector doc inaccuracies verified against source code ([#12911](https://github.com/apache/seatunnel/pull/12911))
*   [Docs][Connector-V2] Fix the dead InterSystems IRIS JDBC driver link ([#12965](https://github.com/apache/seatunnel/pull/12965))
*   [Docs] Fix Zeta REST API, engine ops, transform and FAQ doc inaccuracies verified against source code ([#12932](https://github.com/apache/seatunnel/pull/12932))
*   [Docs] Fix IoTDB, Http, Paimon, SelectDB, Neo4j and Phoenix doc inaccuracies ([#12758](https://github.com/apache/seatunnel/pull/12758))
*   [Docs][Zeta] Document Oracle JDBC secure random blocking ([#12888](https://github.com/apache/seatunnel/pull/12888))
*   [Docs][Transform-V2] Improve Filter transform documentation ([#12816](https://github.com/apache/seatunnel/pull/12816))
*   [Docs] Fix missing, misplaced and divergent connector documentation verified against source code ([#12621](https://github.com/apache/seatunnel/pull/12621))
*   [Docs] Fix connector and transform doc inaccuracies verified against source code ([#12833](https://github.com/apache/seatunnel/pull/12833))
*   [Docs] Fix connector option docs verified against source code ([#12570](https://github.com/apache/seatunnel/pull/12570))
*   [Docs] Self-host Ask DeepWiki badge image to fix broken rendering ([#12830](https://github.com/apache/seatunnel/pull/12830))
*   [Docs][Connector-V2] Audit PostgreSQL CDC bounded-read feasibility ([#12359](https://github.com/apache/seatunnel/pull/12359))
*   [Docs][CDC] Clarify partial update limitations ([#12782](https://github.com/apache/seatunnel/pull/12782))
*   [Docs][Connector-V2] Fix CosFile source doc table format: add missing description column ([#12514](https://github.com/apache/seatunnel/pull/12514))
*   [Docs] Clarify S3File credential providers for container environments ([#11423](https://github.com/apache/seatunnel/pull/11423))
*   [Docs] Fix connector option defaults, malformed tables, broken links, and add missing zh Splunk doc ([#12347](https://github.com/apache/seatunnel/pull/12347))
*   [Docs][Connector-V2] Fix Mqtt field_delimiter table rendering and align en/zh source docs ([#12675](https://github.com/apache/seatunnel/pull/12675))
*   [Docs] Fix transform and configuration doc inaccuracies verified against source code ([#12609](https://github.com/apache/seatunnel/pull/12609))
*   [Docs] Fix reversed embedding field mappings in RAG examples ([#12611](https://github.com/apache/seatunnel/pull/12611))
*   [Docs] Sync outdated connector option tables between English and Chinese docs ([#12553](https://github.com/apache/seatunnel/pull/12553))
*   [Docs] Move newly added connector docs into navigable directory and sync en/zh sink feature checklists ([#12378](https://github.com/apache/seatunnel/pull/12378))
*   [Docs][Connector-V2] Correct DuckDB XA support claim ([#12505](https://github.com/apache/seatunnel/pull/12505))
*   [Docs] Fix transform doc inaccuracies verified against source code ([#12476](https://github.com/apache/seatunnel/pull/12476))
*   [Docs] Fix file connector doc inaccuracies, malformed tables and missing option rows verified against source code ([#12499](https://github.com/apache/seatunnel/pull/12499))
*   [Docs][Connector-V2] Fix connector doc inaccuracies and broken Doris option key verified against source code ([#12487](https://github.com/apache/seatunnel/pull/12487))
*   [Docs] Add RAG ready data processing guide ([#12488](https://github.com/apache/seatunnel/pull/12488))
*   [docs][jdbc] Fix sample sharding default on Postgres and Kingbase pages ([#12427](https://github.com/apache/seatunnel/pull/12427))
*   [Hotfix][Docs] Replace unavailable RDS MySQL links ([#12452](https://github.com/apache/seatunnel/pull/12452))
*   [Docs] Fix connector doc inaccuracies and add missing option rows verified against source code ([#12447](https://github.com/apache/seatunnel/pull/12447))
*   [Docs] Document DolphinScheduler integration with Docker Compose deployments ([#11691](https://github.com/apache/seatunnel/pull/11691))
*   [Docs] Fix option inconsistencies and broken anchors in connector docs ([#12331](https://github.com/apache/seatunnel/pull/12331))
*   [Docs] Add test class conventions to AGENTS.md ([#12330](https://github.com/apache/seatunnel/pull/12330))
*   [Docs][Connector-V2] Improve Paimon RocketMQ and Pulsar connector docs ([#12187](https://github.com/apache/seatunnel/pull/12187))

## Others

*   [Test][E2E] Cover a kafka connect json tombstone in the Kafka format IT ([#13010](https://github.com/apache/seatunnel/pull/13010))
*   [Test][Connector-V2] Check that the Jdbc source still rejects unknown option keys ([#12890](https://github.com/apache/seatunnel/pull/12890))
*   [Test][Connector-V2][Iceberg] Poll for CDC capture and checkpoint commits instead of fixed sleeps ([#13067](https://github.com/apache/seatunnel/pull/13067))
*   [Test][E2E] Poll for job visibility and running state before cancel operations ([#13068](https://github.com/apache/seatunnel/pull/13068))
*   [Test][Zeta] Poll for worker registration and system-load collection in allocate strategy ITs ([#13066](https://github.com/apache/seatunnel/pull/13066))
*   [Test][Connector-V2] Synchronize Python source close test ([#12987](https://github.com/apache/seatunnel/pull/12987))
*   [Test][E2E] Give HANA and OceanBase e2e containers realistic startup readiness ([#12128](https://github.com/apache/seatunnel/pull/12128))
*   [Test][Zeta] Warm up the checkpoint scheduling benchmark by trigger count ([#12896](https://github.com/apache/seatunnel/pull/12896))
*   [Test][Transform-V2] Add SQL-level coverage for CEIL/FLOOR/TRUNC on TINYINT columns ([#12007](https://github.com/apache/seatunnel/pull/12007))
*   chore(ci): bump actions/checkout to v4 in CodeQL workflow ([#12913](https://github.com/apache/seatunnel/pull/12913))
*   [Test][E2E] Make Opengauss CDC restore tests and Nebula startup tolerant of slow CI runners ([#12380](https://github.com/apache/seatunnel/pull/12380))
*   chore: optimize AGENTS.md ([#12811](https://github.com/apache/seatunnel/pull/12811))
*   [Connector-V2][Asana] Asana Source Connector ([#12719](https://github.com/apache/seatunnel/pull/12719))
*   Improve/Support JSON params via CLI ([#12491](https://github.com/apache/seatunnel/pull/12491))
*   [Test][E2E] Isolate the RocketMqIT sink topics and wait for row visibility before reading ([#12701](https://github.com/apache/seatunnel/pull/12701))
*   [Test][E2E][MySQL-CDC] Cover the SET/ENUM DDL a same-dialect MySQL sink executes ([#12373](https://github.com/apache/seatunnel/pull/12373))
*   [Test][Connector-V2][Elasticsearch] Poll for index visibility in ElasticsearchIT slice tests ([#12711](https://github.com/apache/seatunnel/pull/12711))
*   [Test][Zeta] Replace fixed waits with condition-based waits in engine e2e tests ([#12652](https://github.com/apache/seatunnel/pull/12652))
*   [Test][Connector-V2] Poll for storage readiness instead of fixed sleeps in e2e tests ([#12653](https://github.com/apache/seatunnel/pull/12653))
*   [Test][Connector-V2] Poll for CDC and RabbitMQ data readiness instead of fixed sleeps in e2e tests ([#12654](https://github.com/apache/seatunnel/pull/12654))
*   [Test][Zeta] Poll for checkpoint metrics instead of fixed sleeps in CommittedMetricsIT ([#12646](https://github.com/apache/seatunnel/pull/12646))
*   [Test][Connector-V2][Elasticsearch] Poll for index refresh instead of fixed sleeps in ElasticsearchIT ([#12642](https://github.com/apache/seatunnel/pull/12642))
*   [Test][Connector-V2][Easysearch] Poll for index refresh instead of fixed sleeps in EasysearchIT ([#12644](https://github.com/apache/seatunnel/pull/12644))
*   [Test][Zeta] Preserve file rediscovery assertion diagnostics ([#12637](https://github.com/apache/seatunnel/pull/12637))
*   [Test][Zeta] Isolate REST config shade fixture discovery ([#12338](https://github.com/apache/seatunnel/pull/12338))
*   [Test][Zeta] Isolate task execution fixture identities ([#12394](https://github.com/apache/seatunnel/pull/12394))
*   [Test][Zeta] Await asynchronous logical checkpoint metrics ([#12453](https://github.com/apache/seatunnel/pull/12453))
*   [Test][Zeta] Drive FileCollectReader idle close and rediscovery with a manual clock ([#12472](https://github.com/apache/seatunnel/pull/12472))
*   [Test][Connector-V2] Tolerate the aborted body write in GA4 oversized-response test ([#12471](https://github.com/apache/seatunnel/pull/12471))
*   [Test] Widen timing budgets in stall-sensitive unit tests ([#12381](https://github.com/apache/seatunnel/pull/12381))
*   [Test][Zeta] Add a checkpoint scheduling delay benchmark ([#12418](https://github.com/apache/seatunnel/pull/12418))
*   [Chart] Disable unauthenticated Zeta REST API v1 in the Kubernetes Helm chart defaults ([#12513](https://github.com/apache/seatunnel/pull/12513))
*   [Chore] Refresh active collaborators ([#12532](https://github.com/apache/seatunnel/pull/12532))
*   [Test][Connector-V2] Release PayPalClientTest's arrival latch before the response body write ([#12444](https://github.com/apache/seatunnel/pull/12444))
*   Add declarative required-option validation for TDengine ([#12490](https://github.com/apache/seatunnel/pull/12490))
*   [Test][CDC] Cover checkpoint offsets around fetcher failure ([#12360](https://github.com/apache/seatunnel/pull/12360))
*   [Test][Connector-V2] Cover the MqttSink serialization boundary ([#12455](https://github.com/apache/seatunnel/pull/12455))
*   [Test][Transform-V2] Cover the MOD divisor type contract and its overflow bounds ([#12182](https://github.com/apache/seatunnel/pull/12182))
*   [Test][Connector-V2] Strengthen the restored MqttSource assertions ([#12436](https://github.com/apache/seatunnel/pull/12436))
*   [Test][E2E] Add MqttSourceIT covering the MQTT source on Zeta ([#12408](https://github.com/apache/seatunnel/pull/12408))
*   [Test][Connector-V2] Complete the MQTT serialization field coverage ([#12416](https://github.com/apache/seatunnel/pull/12416))
*   [Test][Connector-V2] Strengthen the MQTT source serialization coverage ([#12410](https://github.com/apache/seatunnel/pull/12410))
*   [Test][E2E] Add multi-pipeline restore slot-contention probe for Zeta ([#12109](https://github.com/apache/seatunnel/pull/12109))
*   [Test][E2E] Isolate the RocketMqIT tag tests with unique topic and consumer group ([#12396](https://github.com/apache/seatunnel/pull/12396))
*   [Test][E2E] Remove RocketMqIT.deleteTopicIfExist, which never deleted anything ([#12393](https://github.com/apache/seatunnel/pull/12393))
*   [Test][E2E] Retry NebulaPool init to absorb graphd's status-vs-RPC-port startup race ([#12329](https://github.com/apache/seatunnel/pull/12329))
*   [Test][CLI] Add opt-in paraphrase benchmark suite ([#12241](https://github.com/apache/seatunnel/pull/12241))
*   [Test][E2E] Count each stored message once in RocketMqIT's restore verification ([#12375](https://github.com/apache/seatunnel/pull/12375))
*   [Test][E2E] Start the RocketMQ broker with a single identity and wait for name-server routes ([#12115](https://github.com/apache/seatunnel/pull/12115))
*   [Test][Connector-V2] Widen PayPalClientTest's loopback HTTP wait budget for slower CI runners ([#12370](https://github.com/apache/seatunnel/pull/12370))

For complete upgrade notes, see the [Incompatible Changes documentation](https://github.com/apache/seatunnel/blob/98613bcbb43d0db7f15224863244243ecaa9c146/docs/en/introduction/concepts/incompatible-changes.md#dev).

Contributors (in no particular order):

*   88fantasy
*   Arvin
*   chovygo
*   Claire
*   corgy-w
*   Daniel
*   Daniel Carter
*   David Zollo
*   Doyeon Kim
*   FatMan
*   Gangavarapu Vivek
*   Goutam Adwant
*   Guobin Sun
*   Gökhan Elbistan
*   hutiefang76
*   inchang-ing
*   JacobZheng0927
*   Jast
*   JeremyXin
*   jinniiii233
*   Joel Shearon
*   José Guilherme Souza
*   junvelop
*   Kareem Ahmed
*   Karthik Thota
*   kta1kri
*   Li Haodong
*   Li Zhi
*   lifeng
*   lw
*   MOBIN
*   nankeChen75
*   NganWave
*   Nikhil kumar
*   Palm
*   Pravin Jaywantrao Maske
*   Rango
*   Ricky Makhija
*   Rishavbhattarai
*   Ryan Yang
*   Saloni Gupta
*   Satyam Pandey
*   Sepuri Sai Krishna
*   SEZ
*   shown
*   Steven Zhao
*   Stochastic
*   Sudarshan Kumar Kaushik
*   sunoj
*   Supidan
*   surafel
*   tengri
*   TianHengZhuang
*   tzy
*   WPNG
*   xuepeng
*   Yiğitcan Öztürk
*   Yu, Wang
*   yusiwen
*   yuzhou
*   yzeng1618
*   zhangdonghao
*   zhengxiang378928908-code
*   zhiweiniu
*   ZIHAN DAI
*   zzk
*   雷炯

Installation package download link: [Apache SeaTunnel Downloads](https://seatunnel.apache.org/download)
