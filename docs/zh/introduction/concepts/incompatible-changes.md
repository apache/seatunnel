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

- **破坏性变更：`Condition.of(option, null)` 不再允许**
  - **影响范围**：`seatunnel-api` — `org.apache.seatunnel.api.configuration.util.Condition`
  - **变更说明**：`Condition` 构造器新增校验：二元字面量操作符（如 `EQUAL`、`NOT_EQUAL`、`GREATER_THAN` 等）的 `expectValue` 不能为 null。此前 `Condition.of(option, null)` 会被静默接受，现在会在构造时抛出 `IllegalArgumentException`。
  - **影响**：主仓库中没有任何生产代码使用 `Condition.of(option, null)`，实际影响为零。但如果自定义或第三方连接器代码依赖了这一用法，则需要修改。
  - **迁移指南**：如需检测某个 option 是否缺省或未配置，请使用 `Conditions.notBlank(option)`（针对字符串类型）或在 `OptionRule.Builder` 层面使用 `optional(...)` 来处理缺失情况，而不是将 `null` 作为期望值传入。

- **破坏性变更：`OptionValidationException` 消息格式变为结构化聚合**
  - **影响范围**：`seatunnel-api` — `org.apache.seatunnel.api.configuration.util.ConfigValidator`
  - **变更说明**：`ConfigValidator.validate(OptionRule)` 现在会收集所有结构性错误和值约束错误，一次性抛出包含结构化多行消息的 `OptionValidationException`，而非遇到第一个错误就失败。

  **变更前（快速失败，单条错误）**
  ```
  ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - There are unconfigured options, the options('host') are required.
  ```

  **变更后（聚合、结构化）**
  ```
  ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - Option validation failed (2 errors):
    [1] option: 'host'
        type: required
        constraint: required option is not configured
    [2] option: 'port'
        type: value
        constraint: 'port' >= 1
  ```
  - **影响**：通过子字符串匹配（如 `"are required"`）解析异常消息，或假定单行错误格式的代码需要更新。错误码（`API-02`）和代码前缀与消息体之间的 `" - "` 分隔符保持不变。
  - **迁移指南**：更新对 `OptionValidationException.getMessage()` 的字符串匹配逻辑以适配新的多行编号格式。可使用 `getRawMessage()` 获取不含 `ErrorCode` 前缀的消息体。

### 配置变更

### 连接器变更

- **破坏性变更：Iceberg 连接器 — 不再自动继承源表主键**
  - **影响范围**：`seatunnel-connectors-v2/connector-iceberg`
  - **变更说明**：当未显式配置 `iceberg.table.primary-keys` 时，`SchemaUtils.toIcebergSchema()`
    以前会回退使用 CDC 源表的主键。这会静默地将 `identifier-field-ids` 设置到自动创建的 Iceberg
    表上，激活等值删除语义，导致 append-only CDC 管道中的 INSERT 数据静默丢失
    （详见 [#10747](https://github.com/apache/seatunnel/issues/10747)）。该回退行为已被移除。
  - **影响**：使用 `iceberg.table.upsert-mode-enabled=true` 但未显式配置
    `iceberg.table.primary-keys` 的任务，启动时将抛出 `IllegalArgumentException` 并快速失败。
    依赖隐式 PK 继承来实现 upsert 语义的任务，需要显式设置 `iceberg.table.primary-keys`。
  - **迁移指南**：
    - **Upsert 模式任务**：在 Iceberg sink 配置中添加
      `iceberg.table.primary-keys = "<主键列名>"`。
    - **Append-only CDC 任务**：无需任何操作 — 不配置 `iceberg.table.primary-keys`
      现在会正确使用纯 append writer，不会产生等值删除文件。
    - **已存在的 Iceberg 表**（Glue/Hive 元数据中已有 `identifier-field-ids`）在运行时不受影响；
      只有 sink 新建的表会改变行为。

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
