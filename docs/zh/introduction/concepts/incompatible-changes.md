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

- **[BREAKING]** SQL Transform 的 `PARSEDATETIME`、`TO_DATE` 和 `IS_DATE` 函数现在只接受白名单中的日期时间格式模式。以前接受的自定义格式模式现在将在运行时失败。支持的模式有：
  - DateTime: `yyyy-MM-dd HH:mm:ss`, `yyyy-MM-dd HH:mm:ss.SSS`, `yyyy-MM-dd'T'HH:mm:ss`, `yyyy-MM-dd'T'HH:mm:ss.SSS`, `yyyy/MM/dd HH:mm:ss`, `yyyy/MM/dd HH:mm:ss.SSS`, `yyyyMMddHHmmss`
  - Date: `yyyy-MM-dd`, `yyyy/MM/dd`, `yyyyMMdd`
  - Time: `HH:mm:ss`, `HH:mm:ss.SSS`, `HHmmss`

  **异常类型变更**: 无效的日期时间格式模式现在会抛出 `SeaTunnelRuntimeException` 而不是 `TransformException`。如果您的错误处理或监控系统捕获 `TransformException` 来处理日期时间解析错误，您需要更新它们以处理 `SeaTunnelRuntimeException`。

  **迁移指南**: 如果您在 `PARSEDATETIME`、`TO_DATE` 或 `IS_DATE` 函数中使用自定义日期时间格式模式，您必须更新查询以使用上述支持的模式之一。如果您的数据使用不同的格式，您可能需要预处理输入数据以匹配支持的格式，或使用字符串操作函数在解析之前转换格式。

- DataValidator 转换：当 `row_error_handle_way = ROUTE_TO_TABLE` 时，路由到错误表的行 `table_id` 现在会携带上游的 database/schema 前缀（例如从 `ffp` 变为 `db1.ffp` / `db1.schema1.ffp`）。
### 引擎行为变更

### 依赖升级
