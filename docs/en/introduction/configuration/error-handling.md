---
title: Error Handling
---

# Error Handling (Experimental)

In SeaTunnel, the default behavior is: if any Connector or Transform throws an exception, **the entire job fails**.

Starting with this experimental capability, users can change this behavior, allowing the engine to **capture error records, route them to an error sink, and continue advancing the job when conditions permit**.

> **Status: Experimental**
>
> Currently this capability is wired only in the Zeta engine, and JDBC Sink is the validated Sink implementation. Flink/Spark translation paths do not use this mechanism yet. Error handling and row-level error routing are disabled by default, and the configuration and semantics may be adjusted in future versions.

## Use Cases

Typical scenarios where enabling error handling is recommended include but are not limited to:

- A small amount of dirty data exists in large batch offline tasks (such as invalid dates, overly long strings, etc.);
- Occasional primary key or unique constraint conflicts in sink tables;
- Need to maintain overall job availability in the presence of individual exception records, and record error data separately for subsequent troubleshooting and data backfilling.

Scenarios where error handling is not recommended or should be used with caution include:

- Strong at-least-once or exactly-once semantic requirements for "all valid data must be strictly written";
- Scenarios using complex multi-table sinks and wishing to maintain strict consistency semantics across multiple tables.

## Quick Start

For most users, the simplest way to try this feature is to enable it only for a JDBC Sink and route failed rows to a separate JDBC error table.

1. Prepare the normal business table and a separate error table. The error table should use the fields listed in [Error Table Structure](#error-table-structure).
2. Keep the normal Sink configuration unchanged.
3. Add `env.sink_error_handler` and set `mode = "ROUTE"`.
4. Configure the error Sink under `env.sink_error_handler.sink`.
5. Submit the job with Zeta Engine and check the error table after the job runs.

Minimal example:

```hocon
env {
  sink_error_handler {
    mode = "ROUTE"
    max_error_records = 10

    sink {
      plugin_name = "Jdbc"
      url = "jdbc:mysql://localhost:3306/test"
      driver = "com.mysql.cj.jdbc.Driver"
      user = "root"
      password = "******"
      error_table = "orders_error"
    }
  }
}
```

What this means in practice:

- Valid rows continue to flow to the normal Sink.
- Rows classified as row-level errors are written to `orders_error`.
- If more than 10 row-level errors are handled by the same Sink stage in this job, the job fails.
- System-level errors, such as connection failures, still fail the job.

If you only want to observe row-level errors first, use `mode = "LOG"` instead of `mode = "ROUTE"`. In `LOG` mode, SeaTunnel records the error information in logs but does not write an error table.

## Overall Approach

After enabling error handling, the Zeta engine's processing logic for each record can be summarized as:

1. First, process the record normally through Transform / Sink according to the original logic;
2. If an exception occurs during processing, the engine will attempt to distinguish:
   - **Row-level error**: An exception caused by the data itself (such as data format error, constraint conflict, etc.);
   - **System-level error**: Infrastructure issues such as connection interruption, resource shortage (OOM), etc.;
3. For system-level errors, the behavior is consistent with the default: fail the job directly;
4. For situations determined to be row-level errors, the engine will hand the record and exception information to the **ErrorHandler**:
   - `mode = LOG`: Only log;
   - `mode = ROUTE`: In addition to logging, write the error record to a separately configured **error sink** (such as a JDBC error table).

Other normal records will still be passed downstream along the original pipeline.

Error handling behavior is controlled through **env configuration**:

- **Stage-level (env)**: Configure the default behavior for this stage uniformly in `env.transform_error_handler` / `env.sink_error_handler`;
- **Global (env)**: Provide default values for all stages in `env.error_handler`.

Some Transforms (such as JsonPath, DataValidator) still retain their own early row error control options such as `row_error_handle_way`. These options can coexist with the engine-level error handling mechanism introduced in this document, but have not yet been automatically merged with `env.*_error_handler`.

## Core Concepts

### Mode

The most common field in configuration is `mode`:

- `DISABLE`: Disable error handling for this stage (default behavior);
- `LOG`: Only log row-level error logs, do not route to error sink;
- `ROUTE`: Log and route row-level errors to the error sink.

If the above options are not configured at all, SeaTunnel's behavior remains consistent with historical versions: any exception will cause the job to fail.

### Error Sink

The **error sink** is a dedicated sink for receiving error data, which needs to be configured under `..._error_handler.sink`, for example:

```hocon
env {
  sink_error_handler {
    mode = "ROUTE"

    sink {
      plugin_name = "Jdbc"
      error_table = "orders_sink_error_basic"
      # Configure Jdbc Sink options for the error table here
    }
  }
}
```

A common usage pattern is:

- Main sink writes to business table (e.g., `orders_from_sink`);
- Error sink writes to error table (e.g., `orders_sink_error_*`) for subsequent troubleshooting and data backfilling.

### Row-Level Error vs System-Level Error

In most cases, users do not need to manually write logic to determine "whether it is a row-level error".

The engine will attempt to distinguish:

- **Row-level error**: Usually caused by a single piece of data itself, the engine can bypass this data when configuration allows and continue the job;
- **System-level error**: Usually infrastructure issues such as connection interruption, resource shortage (OOM), which will directly cause the job to fail.

Current version's default classification strategy (important):

- **Sink stage**: If the Sink Connector does not implement `SupportRowLevelErrorClassifier`, its exceptions will be treated as system-level errors (even if `sink_error_handler` is configured, the job will still fail).
- **Transform stage**: If the Transform does not implement `SupportRowLevelErrorClassifier`, its exceptions will be treated as system-level errors (even if `transform_error_handler` is configured, the job will still fail).

For some Connectors (such as JDBC), the Connector itself will explicitly declare "which exceptions are row-level errors" through the interface. The engine will prioritize such explicit declarations.

Only Connectors/Transforms that implement `SupportRowLevelErrorClassifier` can trigger row-level errors; otherwise, all exceptions will be treated as system-level errors and cause the job to fail.

> Note
>
> This document describes the current Zeta engine process. In the future, more built-in Transforms and engine integrations will be gradually promoted to implement `SupportRowLevelErrorClassifier` to more accurately distinguish between "row-level errors" and "system-level errors".

### Reliability Scope of ROUTE Mode

In Zeta, `ROUTE` mode drains pending error rows and flushes the configured error sink writer before checkpoint acknowledgement. If writing or closing the error sink fails, the task fails instead of silently reporting a clean shutdown.

The current experimental implementation supports only error sinks that do not require writer state, committer, aggregated committer, or commit-info serializers. If the configured error sink enables such lifecycle capabilities, the job fails fast during initialization instead of running with incomplete checkpoint/commit semantics. For example, a JDBC error sink should not enable exactly-once/XA options in `ROUTE` mode.

This is still an experimental capability. The final delivery semantics of routed error records depend on the configured error sink connector and its own transaction/commit behavior, so this should not be treated as a general exactly-once DLQ guarantee.

### What Happens When Row-Level Error Occurs in Transform Stage (Important)

When a Transform is determined to be a row-level error, **the record will be dropped from the main pipeline** and will not enter subsequent Transforms, nor will it enter downstream Sinks:

- For `map(...)`: Returns `null`, equivalent to "filtering out this record";
- For `flatMap(...)`: Returns an empty list, equivalent to "dropping this record".

If both `mode = ROUTE` is enabled and an error sink is configured, this original record and exception information can still be written to the error table for troubleshooting and data backfilling.

## Configuration and Parameter Description

### Where to Configure?

Error handling currently takes effect mainly through **env configuration**:

- **Stage-level (env)**: Configure the default behavior for this stage uniformly in `env.transform_error_handler` / `env.sink_error_handler`, for example:

  ```hocon
  env {
    transform_error_handler {
      mode = "ROUTE"

      sink {
        plugin_name = "Jdbc"
        error_table = "orders_transform_error_from_env"
      }
    }

    sink_error_handler {
      mode = "ROUTE"
      queue_capacity = 10000
      queue_overflow_policy = "FAIL"

      sink {
        plugin_name = "Jdbc"
        error_table = "orders_sink_error_from_env"
      }
    }
  }
  ```

- **Global (env)**: Provide default values for all stages in `env.error_handler`, for example:

  ```hocon
  env {
    error_handler {
      mode = "LOG"
      include_original_data = true
      include_stacktrace = false
    }
  }
  ```

Override order for parameters with the same name (from high to low):

1. Stage-level `env.transform_error_handler` / `env.sink_error_handler`;
2. Global `env.error_handler` (defaults to `DISABLE`).

The existing error handling options of each Transform / Sink plugin (such as `row_error_handle_way` in JsonPath / DataValidator) are currently **independent** from the above env configuration: internal plugin options only affect the internal behavior of the plugin, while `env.*_error_handler` controls the engine-level row-level error bypass capability.

### General Parameters Overview

| Parameter                     | Type    | Default  | Description / Values                                                                                                                                                                                                                 |
|-------------------------------|---------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `mode`                        | String  | `DISABLE` | Row-level error handling mode: `DISABLE` (off), `LOG` (log only), `ROUTE` (log and route to error sink). Unsupported values fail fast during config parsing.                                                                      |
| `max_error_ratio`             | Double  | `0.0`    | Allowed error ratio, 0.0–1.0; for example, `0.01` means fail the job when error records exceed 1%; `0.0` means no ratio-based failure trigger. Values outside 0.0–1.0 fail fast during config parsing.                            |
| `max_error_ratio_min_records` | Integer | `10000`  | Warm-up threshold for `max_error_ratio`: when total processed records is less than this value, ratio checks are not performed to avoid premature failure before enough records have been processed.                                  |
| `max_error_records`           | Long    | `0`      | Maximum total number of error records allowed; `0` means no count-based failure trigger. Negative values fail fast during config parsing.                                                                                          |
| `queue_capacity`              | Integer | `10000`  | Internal error queue (buffer) capacity limit, maximum number of error records that can be buffered simultaneously in the queue.                                                                                                     |
| `queue_overflow_policy`       | String  | `FAIL`   | Strategy when error queue is full: `FAIL` (fail job), `DROP` (drop new error records), `BLOCK` (block error-producing thread, may affect throughput). Unsupported values fail fast during config parsing.                         |
| `include_original_data`       | Boolean | `false`  | Whether to include original data content in error records.                                                                                                                                                                          |
| `include_stacktrace`          | Boolean | `false`  | Whether to include complete Java exception stack in error records; enabling will increase the size of individual error records.                                                                                                     |
| `original_data_format`        | String  | `TEXT`   | **Reserved parameter**. Current version only supports `TEXT`, internally unified as string form written to error table (`original_data` is the string representation of the record, i.e., `String.valueOf(row)`). Unsupported values fail fast during config parsing. |
| `original_data_max_length`    | Integer | `8192`   | Maximum length of serialized original data, excess will be truncated, used to control the size of individual error records.                                                                                                         |

Threshold statistics scope: Zeta stores threshold counters in engine state with a versioned key scoped by job ID, pipeline ID, action ID, and stage (`TRANSFORM` or `SINK`). Row processing updates those shared engine-state counters immediately, so parallel subtasks of the same action and stage see the same current total/error counters. `max_error_records` and `max_error_ratio` are therefore enforced as stage-level totals for that job and pipeline instead of per-subtask budgets. Sink counts 1 per `write(...)`; Transform chain counts 1 per `map(...)`/`flatMap(...)` call; multiple operators on the same Transform chain share the same stage counter. Different actions and different stages use separate counters. When a task attempt is recreated during recovery, the new handler reuses the same engine-state counters instead of resetting them to zero, so restart or rescale does not multiply the accepted error allowance.

### Error Sink Related Parameters Overview

Configure where error records should be written under `..._error_handler.sink`:

| Parameter      | Type   | Description                                                                                |
|----------------|--------|--------------------------------------------------------------------------------------------|
| `plugin_name`  | String | Connector name used by the error sink, such as `Jdbc`.                                    |
| `error_table`  | String | (JDBC-specific) Target table name for error records, such as `orders_sink_error_basic`.  |

In addition, the error sink also needs to configure the regular parameters of each Connector, such as JDBC's `url`, `username`, `password`, `driver`, etc., written exactly the same as a normal Sink.

If `mode = ROUTE`, `sink.plugin_name` must be configured. When `sink { ... }` is missing or `plugin_name` is empty, the job fails fast during configuration parsing because there is no error sink that can receive routed records.

### Error Table Structure

Currently, the engine constructs a unified error table schema for the error sink (taking JDBC as an example):

- `error_stage`: String, the stage where the error occurred (such as `TRANSFORM` / `SINK`);
- `plugin_type`: String, plugin type (such as `TRANSFORM` / `SINK`);
- `plugin_name`: String, plugin name (such as `Jdbc`, etc.);
- `source_table_path`: String, source table path or identifier;
- `job_id`: Long, SeaTunnel job ID used to distinguish error data from different jobs when they share the same error table;
- `error_message`: String, brief error message of the exception (truncated according to internal upper limit);
- `exception_class`: String, exception class name;
- `stacktrace`: String, complete stack information (only filled when `include_stacktrace = true`);
- `original_data`: String, original data content (only filled when `include_original_data = true`, length controlled by `original_data_max_length`);
- `occur_time`: Timestamp, error occurrence time (UTC).

The above field names remain consistent across different error tables for unified query and analysis.

## How JDBC Error Handling Works (Key)

JDBC is currently the main Connector using row-level error handling capability.

### What Counts as "Row-Level Error" in JDBC?

`JdbcSinkWriter` checks the `SQLException` chain, and if it finds:

- `SQLState` starting with `22` — data exception (such as data too long, type mismatch);
- `SQLState` starting with `23` — integrity constraint exception (such as primary key/unique key conflict);

It will treat it as a **row-level error** when Sink row-error handling is enabled. In that mode, SQLState `22`/`23` failures leave the JDBC retry loop immediately and are handed to the error handler with the current batch. When Sink row-error handling is not enabled, JDBC keeps the normal `max_retries` behavior and the write failure remains visible to the job. Other SQL failures are treated as **system-level errors** and directly fail the job.

For other Sinks, if `SupportRowLevelErrorClassifier` interface is not implemented, the engine will more conservatively treat exceptions as system-level errors: even if `sink_error_handler` is configured, such exceptions will not be bypassed as row-level errors, but will directly fail the job.

### What Happens to Batch Processing When Row-Level Error Occurs?

JDBC Sink typically puts multiple records in a JDBC batch and sends them to the database at once.

When a **row-level error** occurs while writing a record:

- The Connector will catch this exception;
- If it determines this is a "row-level data error", it will call a helper method to **clear the current JDBC batch in memory**.

This means:

- All records in the current batch that "have not yet been actually sent to the database but have been added to the batch" will be cleared together;
- This bad record will be handed to the error handler (write log / write error table);
- Other "good records" in the same batch will **not be automatically retried**.

From the user's perspective, it can be understood as:

> **Once a row-level error appears in this batch, the entire batch is treated as an "error batch".**

Therefore, in the combination of "**batch enabled and error handling enabled**":

- There may be a very small number of originally valid records that were not written to the target database due to being in the same batch as error data;
- Strict at-least-once semantics for "all valid records" no longer have formal guarantees under this configuration combination.

The above behavior is a current implementation detail at the Connector level, and implementations for different Sinks will be gradually optimized in the future to reduce the probability of mistakenly affecting valid records and improve traceability.

### JDBC Usage Recommendations

- If you care more about job stability and can accept a small number of valid records being dropped in error batches:
  - You can enable error handling and retain batch writing;
  - Error tables and logs can be used for post-hoc analysis and data backfilling of error data.

- If you have strict requirements for "no valid record shall be lost":
  - Consider disabling JDBC row-level error handling, or
  - When enabling error handling, reduce `batch_size` (even set to `1`) so that each batch contains at most one record;
  - It is strongly recommended to thoroughly validate with your actual database and JDBC driver in a test environment before enabling this capability in production.

## Current Status of Multi-Table Sink

> **Experimental capability, not yet fully supported.**

## Basic Configuration Example (Single-Table JDBC Sink)

Below is a minimal example demonstrating how to route row-level errors to a JDBC error table in the Sink stage:

```hocon
env {
  sink_error_handler {
    mode = "ROUTE"              # or LOG / DISABLE
    max_error_ratio = 0.01       # Fail job when error ratio > 1%
    max_error_records = 1000     # Or when total errors > 1000
    queue_capacity = 10000
    queue_overflow_policy = "FAIL"  # FAIL / DROP / BLOCK

    include_original_data = true
    include_stacktrace = false
    original_data_format = "TEXT"
    original_data_max_length = 8192

    sink {
      plugin_name = "Jdbc"
      error_table = "orders_sink_error_basic"
      # Configure Jdbc Sink options for the error table here
    }
  }
}
```

### MySQL Error Table Structure

When the JDBC error sink uses the default save-mode settings, SeaTunnel creates the error table automatically from the built-in error schema. If you disable automatic schema creation or need to pre-create the table, use the following structure:

```sql
CREATE TABLE sink_error_basic (
    error_stage VARCHAR(50),
    plugin_type VARCHAR(50),
    plugin_name VARCHAR(100),
    source_table_path VARCHAR(255),
    job_id BIGINT,
    error_message TEXT,
    exception_class VARCHAR(255),
    stacktrace TEXT,
    original_data TEXT,
    occur_time TIMESTAMP
);
```

For the Transform stage, similar configuration can be made through `transform_error_handler`.
