import ChangeLog from '../changelog/connector-bigquery.md';

# BigQuery

> BigQuery 源连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流式](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列裁剪](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持自定义分片](../../introduction/concepts/connector-v2-features.md)

## 描述

用于 Google Cloud BigQuery 的源连接器。通过执行 SQL 查询读取数据，并将结果转换为 SeaTunnel 行。该连接器是有界源：读取查询结果后即结束。

## 支持的数据源信息

| 数据源     | 支持的版本 | Maven                                                                                  |
|------------|-----------|----------------------------------------------------------------------------------------|
| BigQuery   | 2.56.0    | [下载](https://mvnrepository.com/artifact/com.google.cloud/google-cloud-bigquery)      |

## 配置选项

| 名称                      | 类型    | 是否必须 | 默认值 | 描述                                                                                                                                                                                                |
|---------------------------|---------|---------|--------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| project_id                | string  | 是      | -      | GCP 项目 ID                                                                                                                                                                                         |
| query                     | string  | 否      | -      | 要执行的 SQL 查询语句。需要提供 `query` 或 `dataset_id` + `table_id` 之一。                                                                                                                          |
| dataset_id                | string  | 否      | -      | BigQuery 数据集 ID。与 `table_id` 一起使用（未提供 `query` 时），或用于设置查询作业的 `defaultDataset`。                                                                                                |
| table_id                  | string  | 否      | -      | BigQuery 表 ID。与 `dataset_id` 一起使用（未提供 `query` 时）。                                                                                                                                       |
| service_account_key_path  | string  | 否      | -      | GCP 服务账号 JSON 密钥文件路径                                                                                                                                                                       |
| service_account_key_json  | string  | 否      | -      | 内联 GCP 服务账号 JSON 密钥内容                                                                                                                                                                     |
| max_results               | long    | 否      | 100    | BigQuery API 分页大小。SDK 的 `TableResult` 在内部自动处理分页。                                                                                                                                     |
| fetch_size                | int     | 否      | 0      | SeaTunnel 内部批量大小——每次 poll 周期向下游客发送的最大行数。零表示无限制。与 `max_results` 独立。较小的值允许在批次之间更频繁地做 checkpoint。                                                         |
| location                  | string  | 否      | -      | BigQuery 作业位置，设置在 `JobId.setLocation()` 上。对于非 US/EU 区域的作业是必需的。                                                                                                                  |
| use_query_cache           | boolean | 否      | true   | 是否使用 BigQuery 查询结果缓存，映射到 `QueryJobConfiguration.useQueryCache()`。                                                                                                                     |
| dry_run                   | boolean | 否      | false  | 为 true 时执行试运行，返回预估扫描字节数和费用而不实际执行查询，映射到 `QueryJobConfiguration.dryRun()`。                                                                                              |
| maximum_bytes_billed      | long    | 否      | -      | 费用上限；如果预估扫描字节数超过此限制，查询将失败，防止意外的全表扫描，映射到 `QueryJobConfiguration.maximumBytesBilled()`。                                                                        |
| partition_filter          | string  | 否      | -      | 分区过滤谓词，仅用于表读取模式（`dataset_id` + `table_id`）。与 `query` 模式同时使用时会被拒绝并报错。当目标表启用了 `requirePartitionFilter` 时为必填。                                                 |
| emulator_host             | string  | 否      | -      | BigQuery emulator REST 地址，例如 `localhost:9050`。该参数仅用于测试。                                                                                                                                |
| universe_domain           | string  | 否      | -      | Google Cloud 宇宙域/环境域名，例如主权云 S3NS 环境配置为 `s3nsapis.fr`。                                                                                                                              |
| common-options            |         | 否      | -      | 源连接器通用参数，详见 [Source Common Options](../common-options/source-common-options.md)。                                                                                                          |

### 认证参数

对于生产环境的 BigQuery 作业，请提供以下认证方式之一。仅在配置了 `emulator_host` 用于测试时跳过认证。

1. **service_account_key_path**：服务账号 JSON 文件路径
2. **service_account_key_json**：内联 JSON 密钥内容
3. **默认凭据**：如果未指定上述任一项，则使用应用默认凭据（ADC）

### 读取模式

源连接器支持两种读取数据的方式：

1. **自定义查询**：设置 `query` 为 SQL `SELECT` 语句。连接器执行该语句并转换结果。
2. **表读取**：设置 `dataset_id` 和 `table_id`（不设置 `query`）。连接器自动生成 `SELECT * FROM \`project.dataset.table\``。可选设置 `partition_filter` 添加 `WHERE` 子句进行分区裁剪。

Schema 从查询结果或表元数据中推断。当提供 `dataset_id` 和 `table_id` 时，连接器通过 `BigQueryCatalog.getTable()` 读取表元数据。当仅提供 `query` 时，连接器以 `max_results = 1` 执行查询，从 `TableResult.getSchema()` 推断 Schema。

### max_results

`max_results` 控制 BigQuery API 分页大小（每次页请求的行数）。SDK 的 `TableResult` 通过 `getNextPage()` / `getNextPageToken()` 在内部自动处理分页，因此无论此设置如何，都会返回所有行。较大的值可减少大结果集的 API 往返次数；较小的值可降低每页的内存压力。

### fetch_size

`fetch_size` 控制 SeaTunnel 的内部批处理：即每次 `pollNext` 周期向下游客发送的最大行数。值为 `0`（默认）表示无限制——所有可用行在一个周期内发送。非零值将输出拆分为指定大小的批次，允许在批次之间更频繁地做 checkpoint。

此参数与 `max_results` 独立：`max_results` 控制 BigQuery HTTP API 的页大小（SDK 每次请求获取多少行），而 `fetch_size` 控制 SeaTunnel 在将控制权交还引擎之前发送多少行。常见做法是将 `max_results` 设为较大值（减少 API 往返），将 `fetch_size` 设为较小值（允许批次间 checkpoint）。

### location

`location` 设置在 `JobId.setLocation()` 上，告诉 BigQuery 在哪个区域运行作业。对于目标数据集在非 US/EU 区域（如 `asia-northeast1`、`us-central1`）的作业是必需的。

### dry_run

当 `dry_run = true` 时，源连接器通过 `BigQuery.create(JobInfo.of(queryConfig))` 并设置 `dryRun = true` 执行试运行。这会验证查询并返回预估扫描字节数，而不实际执行查询。**不输出任何行**——试运行完成后源连接器立即发送 `noMoreElement` 信号。预估扫描字节数通过 `JobStatistics.QueryStatistics.getEstimatedBytesProcessed()` 以 INFO 级别日志输出。如果同时设置了 `maximum_bytes_billed` 且预估字节数超过该限制，将输出 WARN 日志。

### maximum_bytes_billed

`maximum_bytes_billed` 设置查询的费用上限。如果预估计费字节数超过此限制，查询将失败且不执行。防止意外全表扫描产生意外费用。

### partition_filter

`partition_filter` 是一个可选的过滤谓词，仅用于表读取模式（`dataset_id` + `table_id`）。设置后，连接器会将其作为 `WHERE` 子句追加到生成的查询中：

```sql
SELECT * FROM `project.dataset.table` WHERE <partition_filter>
```

这启用了分区裁剪，可以显著减少大型分区表的扫描费用（按需计费 $6.25/TB）。示例：`partition_filter = "_PARTITIONDATE >= '2026-01-01'"`。

`partition_filter` 是直接追加到生成查询中的原始 SQL 谓词，与 `query` 选项处于相同的信任级别。两者均由管理员通过配置提供。

**校验规则：**

- 与 `query` 模式同时使用时会被拒绝并报错——应在 SQL 中直接包含过滤条件。
- 如果目标表在 BigQuery 元数据中启用了 `requirePartitionFilter = true`，工厂会在提交作业前的校验阶段直接报错，快速失败。

## 任务示例

### 简单表读取

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  BigQuery {
    project_id = "my-gcp-project"
    dataset_id = "public_data"
    table_id = "orders"
    service_account_key_path = "/path/to/key.json"
  }
}

sink {
  Console {}
}
```

### 自定义查询

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  BigQuery {
    project_id = "my-gcp-project"
    query = "SELECT order_id, customer_name, amount FROM `my-gcp-project.public_data.orders` WHERE amount > 100 ORDER BY order_id"
    service_account_key_path = "/path/to/key.json"
    max_results = 500
    use_query_cache = true
  }
}

sink {
  Console {}
}
```

### 试运行用于费用估算

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  BigQuery {
    project_id = "my-gcp-project"
    query = "SELECT * FROM `my-gcp-project.public_data.large_table`"
    service_account_key_path = "/path/to/key.json"
    dry_run = true
    maximum_bytes_billed = 1000000000
  }
}

sink {
  Console {}
}
```

### 带区域和内联密钥

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  BigQuery {
    project_id = "my-gcp-project"
    dataset_id = "analytics"
    table_id = "events"
    service_account_key_json = "${GCP_SA_KEY_JSON}"
    location = "asia-northeast1"
  }
}

sink {
  Console {}
}
```

## 生命周期和交付语义

BigQuery 源连接器是**有界**源（`Boundedness.BOUNDED`）：

1. **分片枚举**：`BigQuerySourceSplitEnumerator` 创建包含已解析查询的单个分片，并将其分配给第一个 reader。没有并行读取——BigQuery SDK 在内部处理分页。
2. **查询执行**：在第一次 `pollNext` 时，reader 调用 `BigQuery.query(QueryJobConfiguration)` 执行查询。SDK 通过 `TableResult.iterateAll()` 自动分页。
3. **批处理**：如果 `fetch_size > 0`，每次 `pollNext` 周期按 `fetch_size` 大小的批次发送行；否则所有行在一个周期内发送。批次之间，引擎可以执行 checkpoint。
4. **作业清理**：BigQuery 客户端生命周期由 `BigQueryClientFactory`（单例）外部管理。`BigQuery.query()` 创建的查询作业在完成后由 BigQuery 自动清理。reader 的 `close()` 是空操作。
5. **交付语义**：至少一次（at-least-once）。如果任务在读取过程中失败，查询将在恢复时重新执行。没有 offset 跟踪——整个查询结果会被重新读取。
6. **快照状态**：`snapshotState()` 返回待处理分片列表（尚未处理的）。由于只有一个分片，快照要么为空（分片已分配），要么包含该单个分片。

### 范围：仅支持有界批处理（不支持 CDC）

此连接器严格限定为有界批处理。BigQuery 不提供用于流式变更数据捕获的事务日志或 WAL 接口，原生实现 CDC 会导致高槽位争用和配额耗尽。对于需要从 BigQuery 进行流式变更捕获的用户，Google 推荐的架构是 **BigQuery Continuous Queries → Cloud Pub/Sub → SeaTunnel 现有的 `GooglePubSub` Source**。

### 单分片与未来并行化

当前实现使用基于 REST 查询分页（`max_results`）的单分片读取。这对于标准查询和表导出工作负载已经足够。未来可以通过 BigQuery Storage Read API（`ReadSession`）引入并行多分片读取，但这超出了当前版本的范围，并且需要引入额外的第三方依赖。

## 更新日志

<ChangeLog />
