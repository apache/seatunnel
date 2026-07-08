# 不向前兼容的更新

本文档记录了各版本之间不兼容的更新内容。在升级到相关版本前，请检查本文档。

## dev

### JDBC Connector

- **破坏性变更：带时区的时间戳列映射为 `TIMESTAMP_TZ` 类型**
  - **影响范围**：`seatunnel-connectors-v2/connector-jdbc`、`seatunnel-connectors-v2/connector-iceberg`、`seatunnel-connectors-v2/connector-cdc-base`、`seatunnel-connectors-v2/connector-cdc-tidb`、`seatunnel-connectors-v2/connector-starrocks`、`seatunnel-connectors-v2/connector-hudi`、`seatunnel-connectors-v2/connector-snowflake`（通过 JDBC 方言）
  - **变更说明**：以前，JDBC Source 将无时区（如 MySQL `DATETIME`）和带时区（如 MySQL `TIMESTAMP`）的时间戳列都映射为 SeaTunnel 内部的 `TIMESTAMP` 类型。现在，带时区的列（如 MySQL `TIMESTAMP`、PostgreSQL `timestamptz`、Oracle `TIMESTAMP WITH LOCAL TIME ZONE`、SQL Server `datetimeoffset`、Snowflake `TIMESTAMP_LTZ/TZ` 等）被显式映射为 `TIMESTAMP_TZ`。这确保了在写入 Iceberg 等格式时，时区语义得到准确保留（在 Iceberg 中 `TIMESTAMP` 存为无时区的 `timestamp`，`TIMESTAMP_TZ` 存为带时区的 `timestamptz`）。
  - **影响**：如果您的下游 Sink 依赖接收 `TIMESTAMP` 类型且不支持 `TIMESTAMP_TZ`，您可能会遇到类型不匹配错误。对于 Iceberg 用户，这意味着以前作为 `timestamp`（无时区）写入的列现在可能会作为 `timestamptz`（带时区）写入，从而改变表结构。您可能需要在 SQL Transform 中转换该列或更新您的 Sink 配置。(#10685)
  - **各连接器具体行为变更**：
    - **Snowflake**：`TIMESTAMP_LTZ` 和 `TIMESTAMP_TZ` 列现在映射为 `OFFSET_DATE_TIME_TYPE`（`TIMESTAMP_TZ`），而不是原来的 `LOCAL_DATE_TIME_TYPE`。这同时影响 Snowflake 的 Source 和 Sink 路径。
    - **StarRocks**：写入 StarRocks Sink 的 `TIMESTAMP_TZ` 值以 `DATETIME`（仅保留时钟时间，时区偏移量丢失）形式存储，这是由于 StarRocks 不支持原生带时区的日期时间类型。
    - **Hudi**：`TIMESTAMP_TZ` 现在映射为 Avro `timestampMillis`（UTC 纪元时间）。如果 Hudi 表不支持 Schema Evolution，以旧 Schema 写入的现有表可能需要重新创建。
    - **CDC（基于 Debezium，TiDB）**：CDC 连接器现在可以正确处理 Debezium 反序列化层中的 `TIMESTAMP_TZ` 类型。以前，`TIMESTAMP_TZ` 不受支持，会抛出 `UnsupportedOperationException`。现在，在 CDC 管道中使用带时区列的用户可以正常使用。
    - **Iceberg（已有表）**：在本 PR 之前，SeaTunnel 的 `TIMESTAMP` 类型错误地以带时区（`withZone()`）的形式写入 Iceberg。本 PR 之后，`TIMESTAMP` 写为不带时区（`withoutZone()`），而 Iceberg `withZone()` 列读取时返回 `TIMESTAMP_TZ`。**升级影响**：如果您的 Iceberg 表是由旧版 SeaTunnel 创建的，其时间戳列以 `withZone()` 形式存储。升级后，SeaTunnel 会将其读取为 `TIMESTAMP_TZ` 而非 `TIMESTAMP`，下游 Sink 或 Transform 若期望 `TIMESTAMP` 类型可能遇到类型不匹配错误。**迁移方案**：重新创建受影响的 Iceberg 表，或在管道配置中使用 SQL Transform 将 `TIMESTAMP_TZ` 转换回 `TIMESTAMP`。
    - **TIMESTAMP_TZ 写入约定**：SeaTunnel 根据 Sink 格式的表达能力，对 `TIMESTAMP_TZ` 采用两级序列化约定：
      - **不支持原生时区类型的 DB 列类型 Sink（Doris、StarRocks、Xugu）**：丢弃时区偏移，保留时钟时间（wall-clock）。例如，`2024-01-01T03:00:00+09:00` 将存储为 `2024-01-01 03:00:00`。这是有损操作——仅凭存储值无法还原原始 UTC 时刻。
      - **基于字符串/文本的 Sink（Text 文件、Kafka、Pulsar、RocketMQ、RabbitMQ、Redis 等）**：保留完整的 ISO 8601 偏移（例如 `"2024-01-01T03:00:00+09:00"`）。这些格式可以用字符串表示时区偏移，不会丢失信息。如果需要在这类 Sink 中使用 wall-clock 行为，请在写入前通过 SQL Transform 将 `TIMESTAMP_TZ` 转换为 `TIMESTAMP`。
    - **Xugu TIMESTAMP_TZ（有损写入）**：Xugu `TIMESTAMP WITH TIME ZONE` 列在类型层面暴露为 `TIMESTAMP_TZ`，但由于 Xugu JDBC 驱动批量执行缺陷（[E19138]），实际写入时会丢弃时区偏移，仅存储时钟时间。首次写入时会输出 WARN 日志。

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

- **破坏性变更：CatalogFactory 创建路径现在会校验 `optionRule()`**
  - **影响范围**：`seatunnel-api` — `FactoryUtil.createOptionalCatalog()`
  - **变更说明**：`FactoryUtil.createOptionalCatalog()` 方法现在在创建 catalog 实例之前会调用 `ConfigValidator.validate(catalogFactory.optionRule())` 进行校验。此前，catalog 创建路径不会对 catalog factory 的 option rules 执行任何校验。
  - **影响**：如果 catalog factory 的 `optionRule()` 将某些选项声明为 `required`，而传入 `createOptionalCatalog()` 的配置中这些选项并不总是存在，则会抛出 `OptionValidationException`。这主要影响通过 `JdbcCatalogUtils.findCatalog()` 触发的 JDBC 连接器路径。
  - **迁移指南**：如果您有自定义的 `CatalogFactory` 实现，请确保其 `optionRule()` 准确反映在运行时到达它的配置中，哪些选项是真正必填的，哪些是可选的。


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
- **[BREAKING]** 多个转换插件现在通过声明式 `OptionRule` 在提交时执行更严格的配置校验。以前在提交时能通过但运行时失败的配置，现在会在提交时被拒绝，并抛出描述清晰的 `OptionValidationException`：

  | 转换插件 | 新增拒绝的配置 | 以前的行为 | 迁移方式 |
  |---------|--------------|-----------|---------|
  | `DefineSinkType` | `columns` 条目中 `column` 或 `type` 为空 | 运行时 NPE 或未定义行为 | 确保每个条目都有非空的 `column` 和 `type` 字段 |
  | `DefineSinkType` | `columns` 中存在重复列名 | 静默覆盖或运行时冲突 | 移除重复的列条目 |
  | `FieldEncrypt` | `max_field_length` 设置为 ≤ 0 | 被忽略或产生意外截断 | 设置为正整数，或移除该选项以使用默认值 |
  | `DynamicCompile` | `compile_pattern = SOURCE_CODE` 但 `source_code` 为空 | 运行时编译失败 | 使用 `SOURCE_CODE` 模式时提供 `source_code` |
  | `DynamicCompile` | `compile_pattern = ABSOLUTE_PATH` 但 `absolute_path` 为空 | 运行时文件读取失败 | 使用 `ABSOLUTE_PATH` 模式时提供 `absolute_path` |

  **迁移指南**：升级前请对照上表检查您的转换配置。如果现有配置匹配了"新增拒绝的配置"中的情况，请在升级前修改。提交时的错误消息会清楚标明哪个选项无效及原因。

### 引擎行为变更

### 依赖升级
