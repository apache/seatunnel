# 不向前兼容的更新

本文档记录了各版本之间不兼容的更新内容。在升级到相关版本前，请检查本文档。

## dev

### MySQL CDC Schema-Change 解析

- **行为变更：向上传播 DDL 解析监听器错误**
  - **影响范围**：`connector-cdc-mysql`
  - **变更说明**：处理已解析 DDL 时发生的错误不再被吞掉并当作无操作处理，而是作为解析失败向上抛出，避免 CDC 作业静默跳过 schema 变更。
  - **影响**：某些过去在内部解析器或监听器出错后被忽略的 DDL，升级后可能导致作业失败。请检查源端 DDL，修改为连接器支持的语法后再重启作业。本变更不修改 checkpoint 或 savepoint 格式。

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

- **破坏性变更：运行期日志级别接口拒绝无法识别的级别**
  - **影响范围**：SeaTunnel Engine REST API — `POST /hazelcast/rest/maps/log-level`
  - **变更说明**：该接口此前对任何请求都返回 `200` 和 `{"status":"SUCCESS"}`，包括无法识别的级别名（`DEBUGG`、`verbose`、不存在的级别、空值）。这类请求实际上什么都没有生效，并且无法识别的级别会以 `null` 传给 log4j2，而 `null` 并不是"保持不变"：它会清除该 logger 上显式设置的级别，于是 logger 静默回退到父级别，root logger 则回退到 `ERROR`。现在无法识别的级别、空级别以及缺少 `level` 参数都会返回 `400`，并在响应中列出有效级别；级别名仍然不区分大小写。
  - **影响**：只检查 HTTP 状态码的脚本和自动化流程，对于原本就没有生效的请求，会从 `200` 变为 `400`。能够正确识别级别的请求行为不变。
  - **升级指南**：请传入 log4j2 能识别的级别（`OFF`、`FATAL`、`ERROR`、`WARN`、`INFO`、`DEBUG`、`TRACE`、`ALL`，或配置中注册的自定义级别）。被拒绝请求的响应体会列出该节点接受的级别。

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

- **破坏性变更：正式发布版本的连接器默认改为通过 HTTPS 直接下载**
  - **影响范围**：Linux 和 macOS 上的 `bin/install-plugin.sh`
  - **变更说明**：对于固定的正式发布版本，脚本现在默认从 Maven Central 通过 HTTPS 直接下载连接器，并使用仓库发布的 SHA-512 或 SHA-1 校验文件验证完整性。此前所有连接器都通过发行包内置的 Maven Wrapper 解析。
  - **影响**：如果现有环境依赖 Maven `settings.xml` 中配置的镜像、认证仓库、代理或自定义 TLS 策略，升级后使用默认命令安装正式版本连接器可能失败。
  - **迁移指南**：运行 `install-plugin.sh` 时设置 `SEATUNNEL_PLUGIN_DOWNLOAD_METHOD=maven`，即可保留原有的 Maven 解析行为。也可以通过 `SEATUNNEL_MAVEN_REPOSITORY` 指定一个发布连接器校验文件的 HTTPS Maven 兼容镜像。

- **破坏性变更：CatalogFactory 创建路径现在会校验 `optionRule()`**
  - **影响范围**：`seatunnel-api` — `FactoryUtil.createOptionalCatalog()`
  - **变更说明**：`FactoryUtil.createOptionalCatalog()` 方法现在在创建 catalog 实例之前会调用 `ConfigValidator.validate(catalogFactory.optionRule())` 进行校验。此前，catalog 创建路径不会对 catalog factory 的 option rules 执行任何校验。
  - **影响**：如果 catalog factory 的 `optionRule()` 将某些选项声明为 `required`，而传入 `createOptionalCatalog()` 的配置中这些选项并不总是存在，则会抛出 `OptionValidationException`。这主要影响通过 `JdbcCatalogUtils.findCatalog()` 触发的 JDBC 连接器路径。
  - **迁移指南**：如果您有自定义的 `CatalogFactory` 实现，请确保其 `optionRule()` 准确反映在运行时到达它的配置中，哪些选项是真正必填的，哪些是可选的。


### 连接器变更

- **破坏性变更：ORC 文件 Sink 保留嵌套 Struct 字段名的大小写**
  - **影响范围**：`seatunnel-connectors-v2/connector-file/connector-file-base`（所有共享 `OrcWriteStrategy` 的 File/HDFS/S3/OSS ORC Sink）
  - **变更说明**：此前，`OrcWriteStrategy.buildFieldWithRowType(...)` 在构建 ORC Schema 时，会将每个嵌套 `ROW`（struct）字段名强制转为小写，因此声明为 `MD5` 的嵌套字段在文件 footer 中被持久化为 `md5`。下游消费者按原始大小写名称读取该列时会得到 null/缺失值。本次移除了递归嵌套字段分支上的 `.toLowerCase()` 调用，嵌套 struct 字段名将按原始大小写写入文件 Schema。
  - **影响**：升级后由 SeaTunnel 写入的 ORC 文件，其 Schema footer 中的嵌套字段名保留原始大小写。已经适配旧行为的用户（例如使用 `orc.schema.evolution.case.sensitive=true` 的 ORC Reader、设置 `spark.sql.caseSensitive=true` 的 Spark、或预期读到 `md5` 而非 `MD5` 的下游管道）将面临相反的问题：读取新文件时出现空值或 Schema 不匹配。同一目录下混合旧版本（小写嵌套字段名）和新版本（原始大小写）的文件时，相同逻辑列对应的嵌套 Schema 形态不一致，大小写敏感的 Schema 合并无法调和。
  - **迁移指南**：
    - **混合版本目录**：将目录重新物化，使所有文件都由新版本写入；或将旧版本与新版本文件分别写入不同目录，独立读取。
    - **大小写敏感的下游**：在支持的情况下将 Reader 配置为大小写不敏感的 Schema Evolution，或在读取时重映射该列。
    - **仅大小写不同的同名兄弟字段**（例如同一 struct 中同时存在 `MD5` 和 `md5`）：现在可被表达；大小写不敏感的下游（如 Hive）可能将其视为歧义字段，如需保留请在源头消歧。

- **破坏性变更：Google Bigtable Source 的 `scan_row_limit` 变为每个 split 的上限**
  - **影响范围**：`seatunnel-connectors-v2/connector-google-bigtable`
  - **变更说明**：Enumerator 现在通过 `sampleRowKeys` 按 tablet 边界把表（或配置的 `start_rowkey` / `end_rowkey` 区间）切成多个 split。Reader 仍对每个 split 调用一次 `query.limit(...)`。此前 Source 始终只产生 1 个 split，因此 `scan_row_limit` 等价于整表行数上限。升级后，只要表有多个 tablet，即使 `parallelism = 1`（唯一 reader 会拿到全部 split），作业级上限约为 `scan_row_limit × split 数`。详见 [Google Bigtable Source](../../connectors/source/GoogleBigtable.md#scan_row_limit-int)。
  - **影响**：依赖 `scan_row_limit` 限制总输出量的存量作业（抽样、测试、成本控制、下游容量）在升级后、配置不变的情况下，可能读出远超以前的行数。
  - **迁移指南**：若仍需要整表级上限，请用 `start_rowkey` / `end_rowkey` 收窄扫描范围，或下调 `scan_row_limit`，使 `scan_row_limit × 预期 split 数` 不超过原预算。采样失败、无采样点或求交为空时仍会回退为单个 split，但这不是用来锁定旧语义的受支持方式。(#11876)

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

- **破坏性变更：File 源连接器拒绝大于 `poi_excel_max_file_size`（默认 50 MB）的 POI 引擎 Excel 文件**
  - **影响范围**：`seatunnel-connectors-v2/connector-file`（LocalFile、HdfsFile、S3File、FtpFile、SftpFile、OssFile、OssJindoFile、ObsFile、CosFile）
  - **变更说明**：Apache POI 在读取任何行之前会将整个 Excel 工作簿完全加载到内存，对于较大的 `.xls`/`.xlsx` 文件可能导致 Zeta worker 严重 GC 压力甚至 OOM。新增 `poi_excel_max_file_size` 选项（默认 50 MB），POI 在构建工作簿之前会拒绝超过该限制的 Excel 文件。该校验同时覆盖普通 Excel 文件和归档（ZIP/TAR/TAR_GZ/GZ）中的 Excel 条目，且仅在 `excel_engine = POI`（默认值）时生效；流式读取的 `excel_engine = EasyExcel` 路径不受此限制。
  - **影响**：此前以 POI 引擎读取大于 50 MB Excel 文件的任务（虽然成功但伴随严重内存压力）现在会以 `FileConnectorException` 快速失败，而不再可能导致 worker OOM。
  - **迁移指南**：对于必须读取大 Excel 文件且 worker 内存充足的 POI 任务，可通过 `poi_excel_max_file_size = <字节数>` 调高限制；否则切换为 `excel_engine = EasyExcel`，该引擎惰性流式读取行，不受此限制约束。

- **破坏性变更：移除 Prometheus Sink 的 `flush_interval` 选项**
  - **受影响组件**：`seatunnel-connectors-v2/connector-prometheus`
  - **变更说明**：Prometheus Sink 不再启动自己的后台刷新线程，连接器级的 `flush_interval` 选项已被移除。定时刷新改为由引擎通过作业 `env` 中的 `sink.flush.interval` 驱动，**仅 Zeta 引擎支持**。
  - **影响**：
    - **Spark 和 Flink 会失去检查点之间的定时刷新。** 被移除的 `flush_interval` 调度器是连接器自己的线程，在所有引擎上都能工作；其替代者 `sink.flush.interval` 是 Zeta 引擎的能力，Spark 和 Flink 的 Sink 写入器上下文并未实现它，因此这两个引擎上没有周期性定时刷新。在 Spark 和 Flink 上，缓存会在达到 `batch_size`、检查点时（Sink 在 `prepareCommit()` 中刷新）以及写入器关闭时被刷新。因此缓存的采样点最多保留一个检查点间隔，而不会一直保存到作业停止；如需降低检查点之间的延迟，请相应调整 `batch_size`。
    - 只有在使用 `--check` / `--dry-run=static` / `--dry-run=connect` 校验配置时（会执行 `validateUnknownKeys`），`Prometheus` sink 中残留的 `flush_interval` 键才会被拒绝。直接提交的作业会静默忽略该残留键；连接器会在每个 Sink 写入器启动时各打印一次告警作为替代提示（因此并行度为 N、多表或多副本的作业会多次打印）。
  - **迁移指南**：从 `Prometheus` sink 中移除 `flush_interval`。如需在 Zeta 上继续使用定时刷新，请在作业 `env` 中设置 `sink.flush.interval`（毫秒）。在 Spark 和 Flink 上，缓存会在每个检查点被刷新；如需降低检查点之间的延迟，请调整 `batch_size`。`batch_size` 触发和写入器关闭时的最后一次刷新在所有引擎上保持不变。

- **破坏性变更：File 连接器拒绝 XML 输入中的 `DOCTYPE` 声明（XXE 加固）**
  - **影响范围**：`seatunnel-connectors-v2/connector-file/connector-file-base`（`XmlReadStrategy`），以及所有基于该模块构建的 File Source：LocalFile、HdfsFile、S3File、OssFile、OssJindoFile、CosFile、FtpFile、SftpFile（`file_format_type = xml`）
  - **变更说明**：此前 XML 读取器使用默认的 dom4j `SAXReader` 解析用户提供的文件，DTD 处理和外部实体解析均保持 JAXP 默认行为。精心构造的 `DOCTYPE`/外部实体载荷可能导致 worker 节点本地文件泄露、SSRF 式请求，或通过实体展开（"billion laughs"）耗尽内存。现在 `XmlReadStrategy` 的所有解析都会经过加固后的 reader：启用 JAXP 安全处理特性、彻底拒绝任何 `<!DOCTYPE ...>` 声明、禁用外部通用/参数实体及外部 DTD 加载，并额外安装一个拒绝一切解析请求的 `EntityResolver` 作为与具体解析器实现无关的兜底防护。
  - **影响**：此前仅因携带 `<!DOCTYPE ...>` 声明才能被解析的 XML 文件——即使该声明是不引用任何外部 `SYSTEM`/`PUBLIC` 资源的良性声明——现在会以 `FileConnectorException(FILE_READ_FAILED)` 失败。该行为没有配置项可以恢复为旧版本的处理方式。
  - **迁移指南**：在使用 SeaTunnel 读取前，移除 XML 文件中的 `DOCTYPE` 声明，或对文件做预处理/重新导出。不带 `DOCTYPE` 声明的合法 XML 文件不受影响。(#11250)

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
- **[BREAKING]** SQL 转换的 `CEIL` / `CEILING`、`FLOOR` 与 `TRUNC` / `TRUNCATE` 现在返回与参数相同的数据类型，
  这与文档中一直声明的行为一致。此前无论输入类型如何，`CEIL` 和 `FLOOR` 都声明为 `INT`、`TRUNC` 声明为 `DOUBLE`，
  从而静默产生错误的值：

  | 表达式 | 输入 | 以前的结果 | 现在的结果 |
  |-------|------|-----------|-----------|
  | `CEIL(bigint_col)` | `9007199254740993` | `1` | `9007199254740993` |
  | `FLOOR(double_col)` | `1.0E18` | `2147483647` | `1.0E18` |
  | `TRUNC(bigint_col)` | `9007199254740993` | 声明为 `DOUBLE`，实际返回 `Long` | `9007199254740993` |

  **迁移指南**：如果下游 Sink 的列是按旧的 `INT` / `DOUBLE` 输出类型创建的，请将其放宽为与源列一致的类型
  （例如 `CEIL(bigint_col)` 对应 `BIGINT`），或使用显式的 `CAST(... AS INT)` 保持原有 schema。
  对 `INT` 列的表达式不受影响。
- **[BREAKING]** SQL 转换的 `ROUND`、`TRUNC` / `TRUNCATE` 和 `MOD` 不再将参数经由 `double` 中转，
  因此 `DECIMAL` 和大 `BIGINT` 值可以保留完整精度。例如
  `ROUND(CAST('12345678901234567890.987654321' AS DECIMAL(38,9)), 2)` 以前返回
  `12345678901234567000.00`，现在返回 `12345678901234567890.99`；`MOD(9007199254740993, 2)` 以前返回 `0`，
  现在返回 `1`。依赖旧的精度丢失结果的作业，其输出会发生变化（现在是正确的）。
- **[BREAKING]** SQL Transform 对 `DECIMAL` 列的算术运算现在保持精确，并且除法改为四舍五入：
  - `+`、`-`、`*`、`/` 的操作数之前通过 `BigDecimal.valueOf(value.doubleValue())` 转换，会先退化为 `double`，丢弃约 17 位有效数字之后的全部内容。现在会保留完整精度——例如在 `DECIMAL(38,2)` 列上，`123456789012345678.99 + 0.01` 返回 `123456789012345679.00`，而不是 `123456789012345680.01`。
  - 除法现在使用 `RoundingMode.HALF_UP` 而不是 `RoundingMode.UP`。`UP` 总是向远离零的方向进位，因此在 scale 为 2 时，`10 / 3` 返回 `3.34` 而不是 `3.33`，`1 / 1000` 返回 `0.01` 而不是 `0.00`。
  - `%`（`MOD`）不受影响，它本来就委托给 `MOD` 函数，没有自行转换操作数。
  - `*` 现在会将结果舍入到输出列声明的 scale（`HALF_UP`），与 `/` 的既有行为一致。精确乘法得到的结果 scale 等于两个操作数 scale 之和，而该列声明的类型是 `DECIMAL(max(precision), max(scale))`；若直接输出更宽的值，会导致按声明 schema 编码的 Sink 写入失败。在 `DECIMAL(38,2)` 列上，`10.25 * 3.75` 返回 `38.44`，而旧的有损转换对这组特定的值恰好返回 `38.4375`。
  - 除数为零的 `DECIMAL` 除法现在抛出标明该运算的 `TransformException`，而此前底层原因是 `java.lang.ArithmeticException("/ by zero")`。两种情况下出错的表达式本来就会被报告（SQL 引擎会包装表达式求值过程中抛出的任何异常），变化的只是 cause 的类型。这与 `MOD` 除零一直以来的报错方式保持一致。

  **迁移指南**：之前被旧舍入模式抬高、或被 `double` 转换截断的结果都会发生变化。乘法结果的小数位数可能比以前*更少*：旧的转换有时会输出比列声明 scale 更宽的值，现在该值会被舍入到声明的 scale，因此原先从 `DECIMAL(38,2)` 列读到 `38.4375` 的作业，升级后会读到 `38.44`。如果下游系统已按旧值对账，升级后需要重新校准。任何为兼容旧行为而做的补偿（例如在除法后减去一个修正值）都应当移除。如果有代码检查除法失败的 cause 并匹配 `ArithmeticException`，需要改为 `TransformException`。
- **[BREAKING]** SQL 转换的 `ABS`，以及使用负数位数的 `ROUND` / `CEIL` / `CEILING` / `FLOOR`，现在当结果无法用参数自身的数据类型表示时，
  会抛出 `TransformException`，而不再静默回绕成一个错误的（通常为负数的）值：

  | 表达式 | 参数类型 | 之前的结果 | 当前的结果 |
  |--------|----------|------------|------------|
  | `ABS(-2147483648)` | `INT` | `-2147483648` | `TransformException` |
  | `ABS(-9223372036854775808)` | `BIGINT` | `-9223372036854775808` | `TransformException` |
  | `ROUND(2147483647, -1)` | `INT` | `-2147483646` | `TransformException` |
  | `ROUND(9223372036854775807, -1)` | `BIGINT` | `-9223372036854775806` | `TransformException` |
  | `CEIL(32767, -1)` | `SMALLINT` | `-32766` | `TransformException` |
  | `FLOOR(-2147483648, -1)` | `INT` | `2147483646` | `TransformException` |

  `ABS` 的文档一直是这样描述的——“ABS(-2147483648) 应该是 2147483648，但是这个值对于这个数据类型是不允许的。这会导致异常”——只是实现从未真正这么做。
  `TRUNC` / `TRUNCATE` 向零舍入，绝不会把值撑出自身的取值范围，因此不受影响；`FLOAT`、`DOUBLE` 和 `DECIMAL` 参数同样不受影响。

  **迁移指南**：之前会输出这些回绕值的作业，现在会在发生溢出的那一行失败。可以把参数转换为更宽的类型以保持作业运行——例如
  `ABS(CAST(int_col AS BIGINT))` 或 `ROUND(CAST(int_col AS BIGINT), -1)`——或者在上游过滤掉这些行。如果下游系统已按旧的回绕值对账，
  升级后需要重新校准。

- **[BREAKING]** SQL 转换现在能正确处理数值函数中此前被遗漏的 `TINYINT` 和 `SMALLINT` 参数。
  `ROUND` / `CEIL` / `CEILING` / `FLOOR` / `TRUNC` / `TRUNCATE` 缺少 `TINYINT` 分支，因此 `TINYINT` 参数会直接穿过类型
  switch 并被原样返回，既不舍入，也没有异常和日志。`ABS` 和 `SIGN` 缺少 `TINYINT` 与 `SMALLINT` 分支，会直接拒绝这些列：

  | 表达式 | 参数类型 | 之前的结果 | 当前的结果 |
  |--------|----------|------------|------------|
  | `ROUND(44, -1)` | `TINYINT` | `44`，静默未舍入 | `40` |
  | `CEIL(44, -1)` | `TINYINT` | `44`，静默未舍入 | `50` |
  | `ROUND(127, -1)` | `TINYINT` | `127`，静默未舍入 | `TransformException`，`130` 超出 `TINYINT` |
  | `ABS(-44)` | `TINYINT` | `TransformException`，“Unsupported arg type” | `44` |
  | `ABS(-300)` | `SMALLINT` | `TransformException`，“Unsupported arg type” | `300` |
  | `SIGN(-44)` | `TINYINT` | `TransformException`，“Unsupported arg type” | `-1` |

  该类型 switch 同时补上了 `default` 分支，因此任何未被处理的数值类型现在会抛出 `TransformException`，而不再被原样返回。
  `SIGN` 处理 `DECIMAL` 参数时改用 `BigDecimal.signum()` 而非 `double` 转换，因此小于 `Double.MIN_VALUE` 的值会返回真实
  符号，而不是 `0`。

  **迁移指南**：之前 `TINYINT` 列静默跳过舍入的作业，现在会得到真正舍入后的值；如果下游系统已按旧的未舍入结果对账，
  升级后需要重新校准。如果舍入后的 `TINYINT` 超出自身类型范围，可以把参数转换为更宽的类型——例如
  `ROUND(CAST(tiny_col AS INT), -1)`——或者在上游过滤掉这些行。此前为绕开 `ABS` / `SIGN` 拒绝而使用的强制转换
  （`ABS(CAST(tiny_col AS INT))`）仍然可以正常工作，可以在方便时再简化。

### 引擎行为变更

### 依赖升级
