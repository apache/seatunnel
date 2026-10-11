import ChangeLog from '../changelog/connector-bigquery.md';

# BigQuery

> BigQuery source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Description

Source connector for Google Cloud BigQuery. Reads data by executing a SQL query and converting the results into SeaTunnel rows. The connector is a bounded source: it reads the query results and then finishes.

## Supported DataSource Info

| Datasource | Supported Versions | Maven                                                                                  |
|------------|--------------------|----------------------------------------------------------------------------------------|
| BigQuery   | 2.56.0             | [Download](https://mvnrepository.com/artifact/com.google.cloud/google-cloud-bigquery) |

## Options

| Name                     | Type    | Required | Default | Description                                                                                                                                                                                          |
|--------------------------|---------|----------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| project_id               | string  | Yes      | -       | GCP project ID                                                                                                                                                                                       |
| query                    | string  | No       | -       | SQL query to execute against BigQuery. Either `query` or the `dataset_id` + `table_id` pair is required.                                                                                            |
| dataset_id               | string  | No       | -       | BigQuery dataset ID. Used with `table_id` when `query` is not provided, or to set `defaultDataset` on the query job.                                                                                |
| table_id                 | string  | No       | -       | BigQuery table ID. Used with `dataset_id` when `query` is not provided.                                                                                                                             |
| service_account_key_path | string  | No       | -       | Path to GCP service account JSON key file                                                                                                                                                           |
| service_account_key_json | string  | No       | -       | Inline GCP service account JSON key content                                                                                                                                                         |
| max_results              | long    | No       | 100     | BigQuery API page size. The SDK's `TableResult` handles pagination internally.                                                                                                                      |
| fetch_size               | int     | No       | 0       | SeaTunnel internal batch size — max rows emitted per poll cycle. Zero means no limit. Independent of `max_results`. A smaller value allows more frequent checkpointing between batches.            |
| location                 | string  | No       | -       | BigQuery job location, set on `JobId.setLocation()`. Required for non-US/EU jobs.                                                                                                                  |
| use_query_cache          | boolean | No       | true    | Whether to use BigQuery query result caching, mapped to `QueryJobConfiguration.useQueryCache()`.                                                                                                   |
| dry_run                  | boolean | No       | false   | When true, executes a dry run that returns estimated bytes scanned and cost without actually running the query, mapped to `QueryJobConfiguration.dryRun()`.                                       |
| maximum_bytes_billed     | long    | No       | -       | Cost cap; the query fails if estimated bytes exceed this limit, preventing accidental full-table scans, mapped to `QueryJobConfiguration.maximumBytesBilled()`.                                   |
| partition_filter         | string  | No       | -       | Partition filter predicate for direct table reads (`dataset_id` + `table_id` mode only). Rejected with an error if combined with `query`. Required when the table has `requirePartitionFilter` enabled. |
| emulator_host            | string  | No       | -       | BigQuery emulator REST host, such as `localhost:9050`. This option is intended for tests only.                                                                                                      |
| universe_domain          | string  | No       | -       | The Google Cloud Universe Domain, such as `s3nsapis.fr` for S3NS sovereign cloud.                                                                                                                  |
| common-options          |         | No       | -       | Source common options. See [Source Common Options](../common-options/source-common-options.md).                                                                                                    |

### Authentication Options

For production BigQuery jobs, provide **one** of the following authentication methods. Authentication is skipped only when `emulator_host` is configured for tests.

1. **service_account_key_path**: Path to service account JSON file
2. **service_account_key_json**: Inline JSON key content
3. **Default credentials**: Uses application default credentials (ADC) if neither is specified

### Query Modes

The source supports two ways to read data:

1. **Custom query**: Set `query` to a SQL `SELECT` statement. The connector executes it and converts the results.
2. **Table read**: Set `dataset_id` and `table_id` (without `query`). The connector generates `SELECT * FROM \`project.dataset.table\`` automatically. Optionally set `partition_filter` to add a `WHERE` clause for partition pruning.

Schema is inferred from the query results or table metadata. When `dataset_id` and `table_id` are provided, the connector reads table metadata via `BigQueryCatalog.getTable()`. When only `query` is provided, the connector executes the query with `max_results = 1` to infer the schema from `TableResult.getSchema()`.

### max_results

`max_results` controls the BigQuery API page size (number of rows per page request). The SDK's `TableResult` handles pagination internally via `getNextPage()` / `getNextPageToken()`, so all rows are returned regardless of this setting. A larger value reduces the number of API round-trips for large results; a smaller value reduces memory pressure per page.

### fetch_size

`fetch_size` controls SeaTunnel's internal batching: it is the maximum number of rows emitted to the downstream per `pollNext` cycle. A value of `0` (default) means no limit — all available rows are emitted in a single cycle. A non-zero value splits the output into batches of the given size, which allows more frequent checkpointing between batches.

This is independent of `max_results`: `max_results` controls the BigQuery HTTP API page size (how many rows the SDK fetches per request), while `fetch_size` controls how many rows SeaTunnel emits before yielding control back to the engine. A common pattern is to set `max_results` to a large value (to reduce API round-trips) and `fetch_size` to a smaller value (to allow checkpointing between batches).

### location

`location` is set on `JobId.setLocation()` and tells BigQuery which region to run the job in. This is required for jobs that target datasets in non-US/EU regions (e.g., `asia-northeast1`, `us-central1`).

### dry_run

When `dry_run = true`, the source executes a dry run via `BigQuery.create(JobInfo.of(queryConfig))` with `dryRun = true`. This validates the query and returns estimated bytes scanned without actually running it. **No rows are emitted** — the source signals `noMoreElement` immediately after the dry run completes. The estimated bytes processed are logged at INFO level via `JobStatistics.QueryStatistics.getEstimatedBytesProcessed()`. If `maximum_bytes_billed` is also set and the estimate exceeds it, a WARN is logged.

### maximum_bytes_billed

`maximum_bytes_billed` sets a cost cap on the query. If the estimated bytes billed would exceed this limit, the query fails without executing. This prevents accidental full-table scans from incurring unexpected costs.

### partition_filter

`partition_filter` is an optional predicate applied only in table-read mode (`dataset_id` + `table_id`). When provided, the connector appends it as a `WHERE` clause to the generated query:

```sql
SELECT * FROM `project.dataset.table` WHERE <partition_filter>
```

This enables partition pruning, which significantly reduces scan cost ($6.25/TB on-demand) for large partitioned tables. Example: `partition_filter = "_PARTITIONDATE >= '2026-01-01'"`.

`partition_filter` is a raw SQL predicate appended directly to the generated query, at the same trust level as the `query` option. Both are administrator-provided configuration values.

**Validation rules:**

- Rejected with an error if combined with `query` mode — include the filter directly in the SQL instead.
- If the target table has `requirePartitionFilter = true` in BigQuery metadata, the factory fails fast at validation time with a clear error before any job is submitted.

## Task Example

### Simple Table Read

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

### Custom Query

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

### Dry Run for Cost Estimation

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

### With Location and Inline Key

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

## Lifecycle and Delivery Semantics

The BigQuery source is a **bounded** source (`Boundedness.BOUNDED`):

1. **Split enumeration**: The `BigQuerySourceSplitEnumerator` creates a single split containing the resolved query and assigns it to the first reader. There is no parallel read — the BigQuery SDK handles pagination internally.
2. **Query execution**: On the first `pollNext`, the reader calls `BigQuery.query(QueryJobConfiguration)` to execute the query. The SDK paginates results via `TableResult.iterateAll()`.
3. **Batching**: If `fetch_size > 0`, rows are emitted in batches of `fetch_size` per `pollNext` cycle; otherwise all rows are emitted in a single cycle. Between batches, the engine can perform checkpointing.
4. **Job cleanup**: The BigQuery client lifecycle is managed externally by `BigQueryClientFactory` (singleton). Query jobs created by `BigQuery.query()` are automatically cleaned up by BigQuery after completion. The reader's `close()` is a no-op.
5. **Delivery semantics**: At-least-once. If a task fails mid-read, the query is re-executed on recovery. There is no offset tracking — the entire query result is re-read.
6. **Snapshot state**: `snapshotState()` returns the list of pending splits (not yet processed). Since there is only one split, the snapshot is either empty (split already assigned) or contains the single split.

### Scope: Bounded-Batch Only (No CDC)

This connector is strictly bounded-batch. BigQuery does not expose a transaction log or WAL socket for streaming change data capture, and attempting CDC natively would cause high slot contention and quota exhaustion. For users who need streaming change capture out of BigQuery, Google's recommended architecture is **BigQuery Continuous Queries → Cloud Pub/Sub → SeaTunnel's existing `GooglePubSub` Source**.

### Single Split and Future Parallelism

The current implementation uses a single split built on REST query pagination (`max_results`). This is sufficient for standard query and table export workloads. A future enhancement could introduce parallel multi-split reads via the BigQuery Storage Read API (`ReadSession`), but that is out of scope for this version and would require an additional third-party dependency.

## Changelog

<ChangeLog />
