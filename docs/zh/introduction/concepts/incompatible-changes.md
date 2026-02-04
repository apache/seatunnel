# 不向前兼容的更新

本文档记录了各版本之间不兼容的更新内容。在升级到相关版本前，请检查本文档。

## dev

### API 变更

- **破坏性变更：Engine REST 表级指标 key 格式变化**
  - **影响范围**：SeaTunnel Engine REST API（`/job-info` 返回的 job metrics 中的表级指标）
  - **变更说明**：为支持多个 Source/Sink/Transform 同时处理同一张表，表级指标的 key 格式从 `{tableName}` 变更为 `{VertexIdentifier}.{tableName}`（例如 `Sink[0].fake.user_table`）。
  - **影响**：依赖旧 key 的 Grafana 仪表盘、Prometheus 告警规则以及自定义监控解析逻辑需要同步修改，否则升级后会出现指标查询/告警静默失效。

  **变更前**
  ```json
  {
    "TableSinkWriteCount": {
      "fake.user_table": "15"
    }
  }
  ```

  **变更后**
  ```json
  {
    "TableSinkWriteCount": {
      "Sink[0].fake.user_table": "10",
      "Sink[1].fake.user_table": "5"
    }
  }
  ```

### 配置变更

### 连接器变更

### 转换变更

- DataValidator 转换：当 `row_error_handle_way = ROUTE_TO_TABLE` 时，路由到错误表的行 `table_id` 现在会携带上游的 database/schema 前缀（例如从 `ffp` 变为 `db1.ffp` / `db1.schema1.ffp`）。
### 引擎行为变更

### 依赖升级
