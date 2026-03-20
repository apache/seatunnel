# Incompatible Changes

This document records the incompatible updates between each version.
You need to check this document before you upgrade to related version.

## dev

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

### Configuration Changes

### Connector Changes

### Transform Changes

- **[BREAKING]** SQL Transform `PARSEDATETIME`, `TO_DATE`, and `IS_DATE` functions now only accept whitelisted datetime format patterns. Custom format patterns that were previously accepted will now fail at runtime. The supported patterns are:
  - DateTime: `yyyy-MM-dd HH:mm:ss`, `yyyy-MM-dd HH:mm:ss.SSS`, `yyyy-MM-dd'T'HH:mm:ss`, `yyyy-MM-dd'T'HH:mm:ss.SSS`, `yyyy/MM/dd HH:mm:ss`, `yyyy/MM/dd HH:mm:ss.SSS`, `yyyyMMddHHmmss`
  - Date: `yyyy-MM-dd`, `yyyy/MM/dd`, `yyyyMMdd`
  - Time: `HH:mm:ss`, `HH:mm:ss.SSS`, `HHmmss`

  **Exception Type Change**: Invalid datetime format patterns now throw `SeaTunnelRuntimeException` instead of `TransformException`. If you have error handling or monitoring systems that catch `TransformException` for datetime parsing errors, you will need to update them to handle `SeaTunnelRuntimeException`.

  **Migration Guide**: If you are using custom datetime format patterns in `PARSEDATETIME`, `TO_DATE`, or `IS_DATE` functions, you must update your queries to use one of the supported patterns above. If your data uses a different format, you may need to preprocess the input data to match a supported format, or use string manipulation functions to transform the format before parsing.
- DataValidator transform: In `row_error_handle_way = ROUTE_TO_TABLE` mode, the routed error row `table_id` now includes the upstream database/schema prefix (for example, `db1.ffp` / `db1.schema1.ffp` instead of `ffp`).
- Adjusted SQL Transform date & time functions:
  - `DATEDIFF(<start>, <end>, 'MONTH')` now returns the total number of months between the two dates across years (for example, from `2023-01-01` to `2024-03-01` returns `14` instead of `15`).
  - `WEEK(<datetime>)` now returns the ISO week number directly (previous behavior added an extra `+1` to the ISO week value).

### Engine Behavior Changes

### Dependency Upgrades
