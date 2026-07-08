# Incompatible Changes

This document records the incompatible updates between each version.
You need to check this document before you upgrade to related version.

## dev

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

- **Breaking Change: CatalogFactory creation path now validates `optionRule()`**
  - **Affected component**: `seatunnel-api` — `FactoryUtil.createOptionalCatalog()`
  - **Description**: The `FactoryUtil.createOptionalCatalog()` method now calls `ConfigValidator.validate(catalogFactory.optionRule())` before creating a catalog instance. Previously, no validation was performed on the catalog factory's option rules during catalog creation.
  - **Impact**: Catalog factories whose `optionRule()` declares options as `required` that are not always present in the config passed to `createOptionalCatalog()` will now throw `OptionValidationException`. This primarily affects the JDBC connector path via `JdbcCatalogUtils.findCatalog()`.
  - **Migration Guide**: If you have a custom `CatalogFactory` implementation, ensure that its `optionRule()` accurately reflects which options are truly mandatory vs optional in the config that reaches it at runtime.


### Connector Changes

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

### Engine Behavior Changes

### Dependency Upgrades
