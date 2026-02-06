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

- DataValidator transform: In `row_error_handle_way = ROUTE_TO_TABLE` mode, the routed error row `table_id` now includes the upstream database/schema prefix (for example, `db1.ffp` / `db1.schema1.ffp` instead of `ffp`).
- Adjusted SQL Transform date & time functions:
  - `DATEDIFF(<start>, <end>, 'MONTH')` now returns the total number of months between the two dates across years (for example, from `2023-01-01` to `2024-03-01` returns `14` instead of `15`).
  - `WEEK(<datetime>)` now returns the ISO week number directly (previous behavior added an extra `+1` to the ISO week value).

### Engine Behavior Changes

### Dependency Upgrades
