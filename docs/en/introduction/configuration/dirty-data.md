<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Dirty record collection and validation

Dirty handling is optional on **Flink** and **Spark** engines (for SeaTunnel Zeta engine, use its built-in `sink_error_handler` / dead-letter queue configuration). With neither `dirty.collector` nor `dirty.validator`, existing sink behavior is unchanged.

An optional validator can reject a `SeaTunnelRow` before it reaches the sink and send that exact row (along with any diagnostic message or exception) to its collector. Every delegate write exception still fails the write, including classified row errors: a buffered batch failure may concern an earlier row. Collector-only mode does not intercept sink exceptions; sinks with their own exact row attribution may explicitly call `context.getDirtyRecordCollector()`.

## Configuration and precedence

Both blocks can be placed in `env` or a sink. A sink-level `dirty.collector` replaces the complete `env` collector block; its fields are not merged individually. The validator block inherits independently from `env` when the sink omits it. An explicit sink-level `null` block disables that inherited collector or validator.

Unknown collector/validator types or initialization errors fail explicitly during job planning on both Flink and Spark. An absent collector uses `noop`; a present collector block must contain a non-blank `type`. A configured validator must include `type` and requires an active collector. Validator errors fail the job rather than silently accepting the record.

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
  dirty.collector = {
    type = "log"
    threshold = 1
    fail_on_threshold = true
    include_stack_trace = false
    log_payload = "false"
  }
}
source {
  FakeSource {
    row.num = 5
    schema.fields { id = int, name = string }
  }
}
sink {
  Kafka {
    topic = "clean_events"
    bootstrap.servers = "kafka:9092"
  }
}
```

The example fails if its collector receives a dirty record. It does not mark all rows as dirty: the source rows are valid and no validator is configured.

## Built-in collectors

`type = "noop"` (alias `none`) performs no collection. `type = "log"` logs dirty records and maintains a local per-collector-instance `AtomicLong` counter. `threshold` is evaluated per collector/task instance, not as a globally enforced or checkpoint-persisted total across Flink or Spark tasks. Restarts or retries can reset or replay counts and collection side effects; custom collectors must provide their own durable recovery and deduplication when required.

| log collector option | Type | Default | Meaning |
| --- | --- | --- | --- |
| `log_level` | string | `ERROR` | `ERROR`, `WARN`, `INFO` or `DEBUG`; other values use `ERROR`. |
| `log_first_n_records` | long | `10` | Log the first N dirty records per collector; must be non-negative. |
| `log_every_n_records` | long | `1000` | Also log every Nth record; must be positive. Set `1` to explicitly log every record. |
| `include_stack_trace` | boolean | `true` | Include exception details in the message. |
| `threshold` | long | `-1` | Count at which the threshold action runs; non-positive disables it. |
| `fail_on_threshold` | boolean | `false` | Throw when count reaches or exceeds a positive threshold; otherwise warn once per collector instance. |
| `log_payload` | string | `false` | `false` logs arity, `fields` logs field names, `full` logs values. |
| `mask_fields` | list of strings | `[]` | Case-insensitive fields masked when logging full values. |

The log collector samples diagnostics by default: the first 10 records and every 1000th record thereafter. Sampling is local to each collector instance. Every record still increments the counter and evaluates the threshold, including unsampled records and records whose configured log level is disabled. Payload and exception formatting runs only for enabled samples. On close, the collector emits at most one summary of observed, logged and suppressed records at the configured level. Use `log_every_n_records = 1` only when per-record diagnostics are required.

Keep payload logging disabled for data that must not appear in logs. If full payload logging is deliberately enabled, configure `mask_fields` for sensitive fields. When `mask_fields` is non-empty and authoritative field-name mapping is unavailable (for example due to missing schema metadata or a row-arity mismatch), the log collector fails closed and masks every field value instead of logging raw values under positional names. Validation requires an active collector; `noop` cannot silently discard rejected records.

## Custom SPI implementations

A collector implements `DirtyRecordCollectorProvider` (`getType`, `createCollector`) and returns a serializable `DirtyRecordCollector`. A validator implements `DirtyDataValidator` and returns `ValidationResult.clean()` or a dirty result from `validate(row, catalogTable)`. Register provider and validator class names in:

```text
META-INF/services/org.apache.seatunnel.api.table.factory.Factory
```

Initialization runs during job planning and the policy is serialized to workers. There is no worker-side init callback after deserialization. Keep policy state serializable, and lazily create worker resources when needed; do not depend on live transient resources opened by the planning instance. Implement `close()` for resources owned by each instance.

Package the implementation and service entry in a JAR available to both job planning and the engine's worker/translation classloaders (for example via `env.jars`). Select a collector by its provider type. Select a validator by its factory identifier or class name:

```hocon
env {
  jars = "file:///path/to/my-dirty-extensions.jar"
}
sink {
  Kafka {
    topic = "clean_events"
    bootstrap.servers = "kafka:9092"
    dirty.collector { type = "log", threshold = 1, fail_on_threshold = true }
    dirty.validator { type = "com.example.RequiredFieldsValidator" }
  }
}
```

## Runtime boundaries

The configured wrapper runs around real sink writers on Flink and Spark. Only synchronous single-table inputs and writers are supported. Active dirty handling rejects multiple input catalog tables during planning, including per-table sink-template fallback; policies are resolved before sink preparation and save-mode side effects. Multi-table-capable sinks (even with one input table) and already aggregated multi-table sinks are rejected during planning, before invoking their save-mode handlers. Async writers, row-error routing and deferred terminal outcomes are rejected at writer startup when dirty handling is active. Without dirty handling, existing paths keep their original behavior.

Validation runs before `write`. All delegate write exceptions propagate unchanged, so batch or latched flush failures cannot collect a healthy current row by mistake. Schema-capable delegates retain their schema callbacks and physical sink-table identity, and the wrapper updates its cached table metadata after a delegate successfully applies a schema change event (clearing it to fail closed if an unrecognized event omits post-change metadata). Errors during commit, checkpoint, schema changes and close still propagate through their existing lifecycle. The wrapper delegates state/commit calls and closes its collector with the writer; it does not add durable error storage or replay. Use a custom collector when durable rejected-record storage is required.
