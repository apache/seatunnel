# Incompatible Changes

This document records the incompatible updates between each version.
You need to check this document before you upgrade to related version.

## dev

### MySQL CDC Schema-Change Parsing

- **Behavior change: DDL parser listener errors are propagated**
  - **Affected component**: `connector-cdc-mysql`
  - **Description**: Errors raised while processing a parsed DDL are no longer swallowed and
    treated as a no-op. They are now propagated as parsing failures so that a CDC job cannot
    silently skip a schema change.
  - **Impact**: A job may fail on a DDL statement that was previously ignored after an internal
    parser/listener error. Review the source DDL and update it to syntax supported by the
    connector before restarting the job. This change does not alter checkpoint or savepoint
    formats.

### JDBC Connector

- **Breaking Change: Mapping of timezone-aware timestamp columns to `TIMESTAMP_TZ` type**
  - **Affected component**: `seatunnel-connectors-v2/connector-jdbc`, `seatunnel-connectors-v2/connector-iceberg`, `seatunnel-connectors-v2/connector-cdc-base`, `seatunnel-connectors-v2/connector-cdc-tidb`, `seatunnel-connectors-v2/connector-starrocks`, `seatunnel-connectors-v2/connector-hudi`, `seatunnel-connectors-v2/connector-snowflake` (via JDBC dialect)
  - **Description**: Previously, JDBC sources mapped both timezone-naive (e.g., MySQL `DATETIME`) and timezone-aware (e.g., MySQL `TIMESTAMP`) timestamp columns to SeaTunnel's internal `TIMESTAMP` type. Now, timezone-aware columns like MySQL `TIMESTAMP`, PostgreSQL `timestamptz`, Oracle `TIMESTAMP WITH LOCAL TIME ZONE`, SQL Server `datetimeoffset`, Snowflake `TIMESTAMP_LTZ/TZ`, and others are explicitly mapped to `TIMESTAMP_TZ`. This ensures that timezone semantics are accurately preserved when writing to formats like Iceberg, where `TIMESTAMP` is saved as `timestamp` (without timezone) and `TIMESTAMP_TZ` is saved as `timestamptz` (with timezone).
  - **Impact**: If your downstream Sink relies on receiving `TIMESTAMP` types and does not support `TIMESTAMP_TZ` natively, you may encounter type mismatch errors. For Iceberg users, this means columns previously written as `timestamp` (without timezone) may now be written as `timestamptz` (with timezone) and change the table schema. You may need to cast the column in sql transform or update your sink configurations. (#10685)
  - **Connector-specific behavior changes**:
    - **Snowflake**: `TIMESTAMP_LTZ` and `TIMESTAMP_TZ` columns are now mapped to `OFFSET_DATE_TIME_TYPE` (`TIMESTAMP_TZ`) instead of `LOCAL_DATE_TIME_TYPE`. This affects both Source and Sink paths for Snowflake.
    - **StarRocks**: `TIMESTAMP_TZ` values written to StarRocks Sink are stored as `DATETIME` (wall-clock only, timezone offset is dropped) due to StarRocks not having a native timezone-aware datetime type.
    - **Hudi**: `TIMESTAMP_TZ` is now mapped to Avro `timestampMillis` (UTC epoch). Existing Hudi tables written with the old schema may need to be re-created if schema evolution is not supported.
    - **CDC (Debezium-based, TiDB)**: CDC connectors now correctly handle `TIMESTAMP_TZ` type in the Debezium deserialization layer. Previously, `TIMESTAMP_TZ` was unsupported and would throw `UnsupportedOperationException`. Users who were previously unable to use timezone-aware columns in CDC pipelines can now do so.
    - **Iceberg (existing tables)**: Before this PR, SeaTunnel's `TIMESTAMP` type was incorrectly written to Iceberg as `timestamp` with timezone (`withZone()`). After this PR, `TIMESTAMP` is written as `timestamp` without timezone (`withoutZone()`), and Iceberg `withZone()` columns are read back as `TIMESTAMP_TZ`. **Upgrade impact**: If you have existing Iceberg tables where timestamp columns were created by an older SeaTunnel version, those columns are stored as `withZone()`. After upgrading, SeaTunnel will read them as `TIMESTAMP_TZ` instead of `TIMESTAMP`. Downstream sinks or transforms that expected `TIMESTAMP` may encounter type mismatch errors. **Migration**: Re-create the affected Iceberg table with the new schema, or use a SQL Transform to cast `TIMESTAMP_TZ` back to `TIMESTAMP` in your pipeline configuration.
    - **TIMESTAMP_TZ downgrade contract**: SeaTunnel applies a two-tier serialization contract for `TIMESTAMP_TZ` depending on what the sink format can represent:
      - **DB column-typed sinks without native timezone support (Doris, StarRocks, Xugu)**: The timezone offset is dropped and the wall-clock value (local datetime) is stored. For example, `2024-01-01T03:00:00+09:00` is stored as `2024-01-01 03:00:00`. This is a lossy operation — the original UTC instant cannot be recovered from the stored value alone.
      - **String/text-based sinks (Text file, Kafka, Pulsar, RocketMQ, RabbitMQ, Redis, etc.)**: The full ISO 8601 offset is preserved (e.g., `"2024-01-01T03:00:00+09:00"`). These formats can represent timezone offsets as strings, so no information is lost. If you need wall-clock behavior for a string sink, use a SQL Transform to cast `TIMESTAMP_TZ` to `TIMESTAMP` before writing.
    - **Xugu TIMESTAMP_TZ (lossy)**: Xugu `TIMESTAMP WITH TIME ZONE` columns are exposed as `TIMESTAMP_TZ` at the type layer, but the actual write path drops the timezone offset and stores only the wall-clock value due to a Xugu JDBC driver batch limitation (bug [E19138]). A warning is logged on the first write.

### API Changes

- **Breaking Change: Engine REST table metrics key format**
  - **Affected component**: SeaTunnel Engine REST API (job metrics in `/job-info`)
  - **Description**: To support multiple Sources/Sinks/Transforms processing the same table, the key format of table-level metrics has changed from `{tableName}` to `{VertexIdentifier}.{tableName}` (for example, `Sink[0].fake.user_table`).
  - **Impact**: Existing Grafana dashboards, Prometheus alert rules, and custom monitoring integrations that reference the old keys must be updated.

  **Before**
  ```json
  {
    "TableSinkWriteCount": {
      "fake.user_table": "15"
    }
  }
  ```

  **After**
  ```json
  {
    "TableSinkWriteCount": {
      "Sink[0].fake.user_table": "10",
      "Sink[1].fake.user_table": "5"
    }
  }
  ```

- **Breaking Change: An unknown log level is rejected by the runtime log level endpoint**
  - **Affected component**: SeaTunnel Engine REST API — `POST /hazelcast/rest/maps/log-level`
  - **Description**: The endpoint answered `200` with `{"status":"SUCCESS"}` for every request, including a level name it could not resolve (`DEBUGG`, `verbose`, a lowercase name of a level that does not exist, an empty value). Nothing was applied in that case, and the unresolved level was handed to log4j2 as `null`, which removes the explicit level of the logger instead of leaving it alone — so the logger silently fell back to its parent, or to `ERROR` for the root logger. An unknown level, a blank level and a missing `level` parameter are now rejected with `400` and a message listing the valid levels; a level name is still accepted in any letter case.
  - **Impact**: Scripts and automation that only check the HTTP status now see `400` where they used to see `200`, for requests that never took effect in the first place. Requests with a resolvable level are unchanged.
  - **Migration Guide**: Send a level log4j2 knows (`OFF`, `FATAL`, `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE`, `ALL`, or a level registered by the configuration). The response body of a rejected request names the levels the node accepts.

- **Breaking Change: `Condition.of(option, null)` no longer allowed**
  - **Affected component**: `seatunnel-api` — `org.apache.seatunnel.api.configuration.util.Condition`
  - **Description**: The `Condition` constructor now validates that binary literal operators (such as `EQUAL`, `NOT_EQUAL`, `GREATER_THAN`, etc.) must have a non-null `expectValue`. Previously, `Condition.of(option, null)` was silently accepted; it now throws `IllegalArgumentException` at construction time.
  - **Impact**: No production code in the main repository uses `Condition.of(option, null)`, so the practical impact is zero. However, any custom or third-party connector code that relied on this pattern will need to be updated.
  - **Migration Guide**: If you need to check whether an option is absent or unset, use `Conditions.notBlank(option)` (for strings) or handle the absence at the `OptionRule.Builder` level with `optional(...)` instead of passing `null` as the expected value.

- **Breaking Change: `OptionValidationException` message format changed to structured aggregation**
  - **Affected component**: `seatunnel-api` — `org.apache.seatunnel.api.configuration.util.ConfigValidator`
  - **Description**: `ConfigValidator.validate(OptionRule)` now collects all structural and value constraint errors and throws a single `OptionValidationException` with a structured multi-line message instead of failing on the first error.

  **Before (fail-fast, single error)**
  ```
  ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - There are unconfigured options, the options('host') are required.
  ```

  **After (aggregated, structured)**
  ```
  ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - Option validation failed (2 errors):
    [1] option: 'host'
        type: required
        constraint: required option is not configured
    [2] option: 'port'
        type: value
        constraint: 'port' >= 1
  ```
  - **Impact**: Code that parses the exception message by matching substrings like `"are required"` or assumes a single-error format will need to be updated. The error code (`API-02`) and the `" - "` separator between the code prefix and the body remain unchanged.
  - **Migration Guide**: Update any string-matching logic on `OptionValidationException.getMessage()` to handle the new multi-line numbered format. Use `getRawMessage()` to get the body without the `ErrorCode` prefix if needed.

### Configuration Changes

- **Breaking Change: Released connector installation defaults to direct HTTPS downloads**
  - **Affected component**: `bin/install-plugin.sh` on Linux and macOS
  - **Description**: Fixed release versions are now downloaded directly from Maven Central over HTTPS and verified with a published SHA-512 or SHA-1 checksum. Previously, every connector was resolved through the bundled Maven Wrapper.
  - **Impact**: Existing environments that depend on Maven `settings.xml` for mirrors, authenticated repositories, proxies, or custom TLS policies may no longer install released connectors with the default command.
  - **Migration Guide**: Set `SEATUNNEL_PLUGIN_DOWNLOAD_METHOD=maven` when running `install-plugin.sh` to preserve the previous Maven resolution behavior. Alternatively, set `SEATUNNEL_MAVEN_REPOSITORY` to an HTTPS Maven-compatible mirror that publishes connector checksum files.

- **Breaking Change: CatalogFactory creation path now validates `optionRule()`**
  - **Affected component**: `seatunnel-api` — `FactoryUtil.createOptionalCatalog()`
  - **Description**: The `FactoryUtil.createOptionalCatalog()` method now calls `ConfigValidator.validate(catalogFactory.optionRule())` before creating a catalog instance. Previously, no validation was performed on the catalog factory's option rules during catalog creation.
  - **Impact**: Catalog factories whose `optionRule()` declares options as `required` that are not always present in the config passed to `createOptionalCatalog()` will now throw `OptionValidationException`. This primarily affects the JDBC connector path via `JdbcCatalogUtils.findCatalog()`.
  - **Migration Guide**: If you have a custom `CatalogFactory` implementation, ensure that its `optionRule()` accurately reflects which options are truly mandatory vs optional in the config that reaches it at runtime.


### Connector Changes

- **Breaking Change: BigQuery Sink Connector — default schema save mode introduces automatic table creation**
  - **Affected component**: `seatunnel-connectors-v2/connector-bigquery`
  - **Description**: The BigQuery sink connector (`connector-bigquery`) now implements `SupportSaveMode` with support for `schema_save_mode` and `data_save_mode`. The default `schema_save_mode` is set to `CREATE_SCHEMA_WHEN_NOT_EXIST`.
  - **Impact**: Upgrading existing pipelines targeting a non-existent table will now automatically create the table in BigQuery with the source schema instead of failing fast at the BigQuery API layer.
  - **Migration Guide**: To preserve the legacy fail-fast behavior, explicitly configure `schema_save_mode = "ERROR_WHEN_SCHEMA_NOT_EXIST"` in your BigQuery sink configuration.

- **Breaking Change: ORC file sink preserves case of nested struct field names**
  - **Affected component**: `seatunnel-connectors-v2/connector-file/connector-file-base` (used by all File/HDFS/S3/OSS ORC sinks that share `OrcWriteStrategy`)
  - **Description**: Previously, `OrcWriteStrategy.buildFieldWithRowType(...)` forced every nested `ROW` (struct) field name to lowercase when building the ORC schema, so a nested field declared as `MD5` was persisted as `md5` in the file footer. Downstream consumers that read the column by its declared original-case name received null/missing values. The `.toLowerCase()` call has been removed from the recursive nested-field branch, so nested struct field names are now written verbatim in the file schema.
  - **Impact**: ORC files written by SeaTunnel after this change embed the original-case nested field names in their schema footer. Users that adapted to the old behavior (for example, case-sensitive ORC readers with `orc.schema.evolution.case.sensitive=true`, Spark with `spark.sql.caseSensitive=true`, or pipelines that expected `md5` rather than `MD5`) will see the inverse problem: null values or schema mismatches when reading new files. Directories that mix pre-upgrade files (lowercase nested names) with post-upgrade files (original case) will contain inconsistent nested-schema shapes for the same logical column, which case-sensitive schema merging cannot reconcile.
  - **Migration Guide**:
    - **Mixed-version directories**: Re-materialize the directory so every file is produced by the new version, or write pre- and post-upgrade files into separate directories and read them independently.
    - **Case-sensitive consumers**: Configure the reader for case-insensitive schema evolution where supported, or remap the column at read time.
    - **Case-only sibling fields** (for example `MD5` and `md5` in the same struct): now representable; case-insensitive downstream consumers (such as Hive) may treat them as ambiguous — disambiguate at the source if needed.

- **Breaking Change: Google Bigtable Source `scan_row_limit` is now a per-split cap**
  - **Affected component**: `seatunnel-connectors-v2/connector-google-bigtable`
  - **Description**: The enumerator now partitions a table (or the configured `start_rowkey` / `end_rowkey` range) into tablet-sized splits via `sampleRowKeys`. `scan_row_limit` is still applied with `query.limit(...)` once per split in the reader. Before this change the source always produced exactly one split, so `scan_row_limit` acted as a table-wide row cap. After this change a table with multiple tablets yields multiple splits even when `parallelism = 1` (the single reader is assigned every split), and the job-level upper bound is about `scan_row_limit × split count`. See [Google Bigtable Source](../../connectors/source/GoogleBigtable.md#scan_row_limit-int).
  - **Impact**: Existing jobs that set `scan_row_limit` to bound total output (sampling, testing, cost control, or downstream capacity) can read far more rows after upgrade with no config change.
  - **Migration Guide**: If you need a table-wide cap, narrow the scan with `start_rowkey` / `end_rowkey`, or lower `scan_row_limit` so that `scan_row_limit × expected split count` stays within the previous budget. To keep the previous single-split behavior, the connector still falls back to one split when sampling fails, returns no keys, or the intersection is empty — that is not a supported way to pin the old cap. (#11876)

- **Breaking Change: Iceberg Connector — source table primary key is no longer silently inherited**
  - **Affected component**: `seatunnel-connectors-v2/connector-iceberg`
  - **Description**: `SchemaUtils.toIcebergSchema()` previously fell back to the CDC source
    table's primary key when `iceberg.table.primary-keys` was not explicitly configured. This
    silently set `identifier-field-ids` on auto-created Iceberg tables, activating equality-delete
    semantics and causing silent INSERT data loss in append-only CDC pipelines
    (see [#10747](https://github.com/apache/seatunnel/issues/10747)). The fallback has been
    removed.
  - **Impact**: Jobs that set `iceberg.table.upsert-mode-enabled=true` without an explicit
    `iceberg.table.primary-keys` will now fail at startup with a clear `IllegalArgumentException`.
    Jobs that relied on implicit PK inheritance to drive upsert semantics must now set
    `iceberg.table.primary-keys` explicitly.
  - **Migration Guide**:
    - **Upsert mode jobs**: Add `iceberg.table.primary-keys = "<your key columns>"` to the Iceberg
      sink config.
    - **Append-only CDC jobs**: No action needed — omitting `iceberg.table.primary-keys` now
      correctly routes writes through the pure append writer with no equality deletes.
    - **Existing Iceberg tables** that already have `identifier-field-ids` stored in their
      Glue/Hive metastore schema are not affected at runtime; only newly auto-created tables change
      behavior.

- **Breaking Change: File source connectors reject POI-engine Excel files larger than `poi_excel_max_file_size` (default 50 MB)**
  - **Affected component**: `seatunnel-connectors-v2/connector-file` (LocalFile, HdfsFile, S3File, FtpFile, SftpFile, OssFile, OssJindoFile, ObsFile, CosFile)
  - **Description**: Apache POI fully materializes an Excel workbook into memory before any row can be read, which can drive a Zeta worker into heavy GC pressure or OOM on large `.xls`/`.xlsx` files. A new `poi_excel_max_file_size` option (default 50 MB) now makes POI reject an Excel file that exceeds the limit before the workbook is built. The guard covers both plain and archived (ZIP/TAR/TAR_GZ/GZ) Excel entries, and applies only when `excel_engine = POI` (the default); the streaming `excel_engine = EasyExcel` path is not bound by this limit.
  - **Impact**: Existing jobs that read POI-engine Excel files larger than 50 MB - which previously succeeded at the cost of heavy memory pressure - will now fail fast with a `FileConnectorException` instead of potentially OOMing the worker.
  - **Migration Guide**: For POI jobs that must read large Excel files and have sufficient worker memory, raise the limit with `poi_excel_max_file_size = <bytes>`. Otherwise switch to `excel_engine = EasyExcel`, which streams rows lazily and is not subject to the limit.

- **Breaking Change: Prometheus Sink `flush_interval` option removed**
  - **Affected component**: `seatunnel-connectors-v2/connector-prometheus`
  - **Description**: The Prometheus Sink no longer starts its own background flush thread. The connector-level `flush_interval` option has been removed. Timer-based flushing is now driven by the engine through `sink.flush.interval` in the job `env` block, which is **supported only by the Zeta engine**.
  - **Impact**:
    - **Spark and Flink lose sub-checkpoint timer-based flushing.** The removed `flush_interval` scheduler was a plain connector-owned thread that ran on all engines. Its replacement, `sink.flush.interval`, is a Zeta engine primitive; the Spark and Flink sink writer contexts do not implement it, so there is no periodic timer flush on those engines. On Spark and Flink the buffer is flushed when it reaches `batch_size`, on checkpoint (the sink flushes in `prepareCommit()`), and when the writer is closed. Buffered points are therefore bounded by the checkpoint interval rather than held until the job stops; for lower latency between checkpoints, tune `batch_size` accordingly.
    - A leftover `flush_interval` key in the `Prometheus` sink block is rejected only when the config is validated with `--check` / `--dry-run=static` / `--dry-run=connect` (which run `validateUnknownKeys`). A directly submitted job silently ignores the stray key; the connector logs a warning once per sink writer at startup instead (so a job with parallelism N, multiple tables, or replicas logs it multiple times).
  - **Migration Guide**: Remove `flush_interval` from the `Prometheus` sink block. To keep timer-based flushing on Zeta, set `sink.flush.interval` (milliseconds) in the job `env` block. On Spark and Flink, buffered points are flushed on each checkpoint; tune `batch_size` for lower latency between checkpoints. The `batch_size` trigger and the final flush on writer close are unchanged on all engines.

- **Breaking Change: File connectors reject `DOCTYPE` declarations in XML input (XXE hardening)**
  - **Affected component**: `seatunnel-connectors-v2/connector-file/connector-file-base` (`XmlReadStrategy`), and every file source built on it: LocalFile, HdfsFile, S3File, OssFile, OssJindoFile, CosFile, FtpFile, SftpFile (`file_format_type = xml`)
  - **Description**: The XML reader previously parsed user-supplied files with a default dom4j `SAXReader`, leaving DTD processing and external entity resolution at their JAXP defaults. A crafted `DOCTYPE`/external-entity payload could disclose local worker-node files, trigger SSRF-style fetches, or exhaust memory via entity expansion ("billion laughs"). `XmlReadStrategy` now routes every parse through a hardened reader that enables JAXP secure processing, rejects any `<!DOCTYPE ...>` declaration outright, disables external general/parameter entities and external DTD loading, and installs a deny-all `EntityResolver` as a parser-agnostic backstop.
  - **Impact**: XML files that previously parsed successfully only because they carried a `<!DOCTYPE ...>` declaration — even a benign one with no external `SYSTEM`/`PUBLIC` reference — now fail with `FileConnectorException(FILE_READ_FAILED)`. There is no configuration option to opt back into the previous behavior.
  - **Migration Guide**: Remove the `DOCTYPE` declaration from XML files before ingesting them with SeaTunnel, or pre-process/re-export the file without it. Well-formed XML without a `DOCTYPE` declaration is unaffected. (#11250)

### Transform Changes

- **[BREAKING]** SQL Transform `PARSEDATETIME`, `TO_DATE`, and `IS_DATE` functions now only accept whitelisted datetime format patterns. Custom format patterns that were previously accepted will now fail at runtime. The supported patterns are:
  - DateTime: `yyyy-MM-dd HH:mm:ss`, `yyyy-MM-dd HH:mm:ss.SSS`, `yyyy-MM-dd'T'HH:mm:ss`, `yyyy-MM-dd'T'HH:mm:ss.SSS`, `yyyy/MM/dd HH:mm:ss`, `yyyy/MM/dd HH:mm:ss.SSS`, `yyyyMMddHHmmss`
  - Date: `yyyy-MM-dd`, `yyyy/MM/dd`, `yyyyMMdd`
  - Time: `HH:mm:ss`, `HH:mm:ss.SSS`, `HHmmss`

  **Exception Type Change**: Invalid datetime format patterns now throw `SeaTunnelRuntimeException` instead of `TransformException`. If you have error handling or monitoring systems that catch `TransformException` for datetime parsing errors, you will need to update them to handle `SeaTunnelRuntimeException`.

  **Migration Guide**: If you are using custom datetime format patterns in `PARSEDATETIME`, `TO_DATE`, or `IS_DATE` functions, you must update your queries to use one of the supported patterns above. If your data uses a different format, you may need to preprocess the input data to match a supported format, or use string manipulation functions to transform the format before parsing.
- DataValidator transform: In `row_error_handle_way = ROUTE_TO_TABLE` mode, the routed error row `table_id` now includes the upstream database/schema prefix (for example, `db1.ffp` / `db1.schema1.ffp` instead of `ffp`).
- **[BREAKING]** Several transform plugins now perform stricter submission-time config validation via declarative `OptionRule`. Configs that previously passed submission but failed at runtime will now be rejected at submission time with a descriptive `OptionValidationException`:

  | Transform | Newly Rejected Config | Previous Behavior | Migration |
  |-----------|----------------------|-------------------|-----------|
  | `DefineSinkType` | `columns` entries with null/empty `column` or `type` | Runtime NPE or undefined behavior | Ensure every entry has non-empty `column` and `type` fields |
  | `DefineSinkType` | `columns` with duplicate column names | Silent override or runtime conflict | Remove duplicate column entries |
  | `FieldEncrypt` | `max_field_length` set to ≤ 0 | Ignored or unexpected truncation | Set `max_field_length` to a positive integer, or remove the option to use the default |
  | `DynamicCompile` | `compile_pattern = SOURCE_CODE` without a non-blank `source_code` | Runtime compilation failure | Provide `source_code` when using `SOURCE_CODE` pattern |
  | `DynamicCompile` | `compile_pattern = ABSOLUTE_PATH` without a non-blank `absolute_path` | Runtime file-read failure | Provide `absolute_path` when using `ABSOLUTE_PATH` pattern |

  **Migration Guide**: Review your transform configs against the table above. If any of your existing configs match a "Newly Rejected" pattern, update them before upgrading. The error messages at submission time now clearly identify which option is invalid and why.
- Adjusted SQL Transform date & time functions:
  - `DATEDIFF(<start>, <end>, 'MONTH')` now returns the total number of months between the two dates across years (for example, from `2023-01-01` to `2024-03-01` returns `14` instead of `15`).
  - `WEEK(<datetime>)` now returns the ISO week number directly (previous behavior added an extra `+1` to the ISO week value).
- **[BREAKING]** SQL Transform `CEIL` / `CEILING`, `FLOOR` and `TRUNC` / `TRUNCATE` now return the data type of their
  argument, as their documentation has always specified. Previously `CEIL` and `FLOOR` declared `INT` and `TRUNC`
  declared `DOUBLE` regardless of the input type, which silently produced wrong values:

  | Expression | Input | Previous result | Current result |
  |------------|-------|-----------------|----------------|
  | `CEIL(bigint_col)` | `9007199254740993` | `1` | `9007199254740993` |
  | `FLOOR(double_col)` | `1.0E18` | `2147483647` | `1.0E18` |
  | `TRUNC(bigint_col)` | `9007199254740993` | declared `DOUBLE`, returned a `Long` | `9007199254740993` |

  **Migration Guide**: If a downstream sink column was created against the old `INT` / `DOUBLE` output type, widen it to
  match the source column type (for example `BIGINT` for `CEIL(bigint_col)`), or wrap the expression in an explicit
  `CAST(... AS INT)` to keep the previous schema. Expressions over `INT` columns are unaffected.
- **[BREAKING]** SQL Transform `ROUND`, `TRUNC` / `TRUNCATE` and `MOD` no longer round-trip their arguments through
  `double`, so `DECIMAL` and large `BIGINT` values keep full precision. For example
  `ROUND(CAST('12345678901234567890.987654321' AS DECIMAL(38,9)), 2)` previously returned
  `12345678901234567000.00` and now returns `12345678901234567890.99`, and `MOD(9007199254740993, 2)` previously
  returned `0` and now returns `1`. Jobs that (intentionally or not) depended on the old lossy values will see
  different — now correct — output.
- **[BREAKING]** SQL Transform arithmetic on `DECIMAL` columns is now exact, and division rounds to nearest:
  - Operands of `+`, `-`, `*` and `/` were previously converted with `BigDecimal.valueOf(value.doubleValue())`, which collapsed them to a `double` and discarded everything beyond ~17 significant digits. Values now keep full precision — for example, on `DECIMAL(38,2)` columns `123456789012345678.99 + 0.01` returns `123456789012345679.00` instead of `123456789012345680.01`.
  - Division now uses `RoundingMode.HALF_UP` instead of `RoundingMode.UP`. `UP` always rounded away from zero, so at scale 2 `10 / 3` returned `3.34` instead of `3.33`, and `1 / 1000` returned `0.01` instead of `0.00`.
  - `%` (`MOD`) is unaffected; it already delegated to the `MOD` function rather than converting operands itself.
  - `*` now rounds its result to the scale declared for the output column (`HALF_UP`), the same way `/` already did. Exact multiplication produces a result whose scale is the sum of the operand scales, while the column is declared as `DECIMAL(max(precision), max(scale))`; emitting the wider value would break sinks that encode against the declared schema. On `DECIMAL(38,2)` columns `10.25 * 3.75` returns `38.44`, where the old lossy conversion happened to return `38.4375` for these particular values.
  - Dividing by a zero `DECIMAL` now fails with a `TransformException` naming the operation, where the underlying cause was previously `java.lang.ArithmeticException("/ by zero")`. The failing expression was already reported either way, since the SQL engine wraps anything thrown while evaluating an expression; only the cause type changed. This matches how `MOD` by zero has always been reported.

  **Migration Guide**: Results that were previously inflated by the old rounding mode, or truncated by the `double` conversion, will change. Multiplication results may now carry *fewer* decimal places than before: the old conversion sometimes emitted a value wider than the declared column scale, and that value is now rounded down to it, so a job reading `38.4375` from a `DECIMAL(38,2)` column will read `38.44` after upgrading. Any code that inspects the *cause* of a division failure and matches on `ArithmeticException` should be updated to expect `TransformException`. If a downstream system was reconciled against the old values, re-baseline it after upgrading. Any workaround that compensated for the old behavior (for example subtracting a correction term after a division) should be removed.
- **[BREAKING]** SQL Transform `ABS`, and `ROUND` / `CEIL` / `CEILING` / `FLOOR` with a negative digit count, now
  fail with a `TransformException` when the result does not fit the argument's own data type, instead of silently
  wrapping around to a wrong — usually negative — value:

  | Expression | Argument type | Previous result | Current result |
  |------------|---------------|-----------------|----------------|
  | `ABS(-2147483648)` | `INT` | `-2147483648` | `TransformException` |
  | `ABS(-9223372036854775808)` | `BIGINT` | `-9223372036854775808` | `TransformException` |
  | `ROUND(2147483647, -1)` | `INT` | `-2147483646` | `TransformException` |
  | `ROUND(9223372036854775807, -1)` | `BIGINT` | `-9223372036854775806` | `TransformException` |
  | `CEIL(32767, -1)` | `SMALLINT` | `-32766` | `TransformException` |
  | `FLOOR(-2147483648, -1)` | `INT` | `2147483646` | `TransformException` |

  `ABS` has always been documented this way — "ABS(-2147483648) should be 2147483648, but this value is not allowed
  for this data type. It leads to an exception" — the implementation simply never did it. `TRUNC` / `TRUNCATE` round
  toward zero and so can never grow a value out of its own range; they are unaffected, as are `FLOAT`, `DOUBLE` and
  `DECIMAL` arguments.

  **Migration Guide**: A job that previously emitted these wrapped values now fails on the row that overflows. Cast
  the argument to a wider type to keep the job running — `ABS(CAST(int_col AS BIGINT))` or
  `ROUND(CAST(int_col AS BIGINT), -1)` — or filter the offending rows out upstream. If a downstream system was
  reconciled against the old wrapped values, re-baseline it after upgrading.

- **[BREAKING]** SQL Transform now dispatches `TINYINT` and `SMALLINT` arguments correctly in the numeric
  functions that previously omitted them. `ROUND` / `CEIL` / `CEILING` / `FLOOR` / `TRUNC` / `TRUNCATE` had no
  `TINYINT` branch, so a `TINYINT` argument fell through the type switch and was returned unrounded, with no
  exception and no log line. `ABS` and `SIGN` had no `TINYINT` or `SMALLINT` branch and rejected those columns
  outright:

  | Expression | Argument type | Previous result | Current result |
  |------------|---------------|-----------------|----------------|
  | `ROUND(44, -1)` | `TINYINT` | `44`, silently not rounded | `40` |
  | `CEIL(44, -1)` | `TINYINT` | `44`, silently not rounded | `50` |
  | `ROUND(127, -1)` | `TINYINT` | `127`, silently not rounded | `TransformException`, `130` exceeds `TINYINT` |
  | `ABS(-44)` | `TINYINT` | `TransformException`, "Unsupported arg type" | `44` |
  | `ABS(-300)` | `SMALLINT` | `TransformException`, "Unsupported arg type" | `300` |
  | `SIGN(-44)` | `TINYINT` | `TransformException`, "Unsupported arg type" | `-1` |

  The same type switch also gained a `default` branch, so any numeric type it does not handle now fails with a
  `TransformException` instead of being returned unrounded. `SIGN` on a `DECIMAL` argument now uses
  `BigDecimal.signum()` rather than a `double` conversion, so a value smaller than `Double.MIN_VALUE` reports its
  true sign instead of `0`.

  **Migration Guide**: A job with a `TINYINT` column that silently skipped rounding now receives the rounded value;
  if a downstream system was reconciled against the old unrounded output, re-baseline it after upgrading. If a
  rounded `TINYINT` no longer fits its own type, cast the argument to a wider type — `ROUND(CAST(tiny_col AS INT), -1)`
  — or filter the offending rows out upstream. Queries that worked around the `ABS` / `SIGN` rejection by casting
  (`ABS(CAST(tiny_col AS INT))`) continue to work unchanged and can be simplified at your convenience.

### Engine Behavior Changes

- **Breaking Change: Zeta master health and telemetry fields now report the active SeaTunnel coordinator**
  - **Affected component**: SeaTunnel Zeta health output, REST cluster health responses, and Prometheus `cluster_info` metrics.
  - **Description**: In separated master/worker deployments, worker-only nodes can temporarily hold Hazelcast mastership even though they cannot act as the SeaTunnel coordinator. The legacy `isMaster` health field and the Prometheus `cluster_info{master=...}` label now report the active SeaTunnel coordinator instead of the raw Hazelcast master. The REST cluster health response also exposes `nodeRole`, `coordinator`, and `worker` fields to show the statically configured node capability.
  - **Impact**: Existing dashboards, alert rules, or scripts that treated `isMaster` or `cluster_info{master=...}` as Hazelcast mastership may observe a value change after upgrade in separated master/worker clusters.
  - **Migration Guide**: Use `isMaster` and `cluster_info{master=...}` for active SeaTunnel coordinator routing. Use `nodeRole`, `coordinator`, and `worker` from the cluster health response when you need to distinguish configured node capability from the active coordinator.

### Dependency Upgrades
