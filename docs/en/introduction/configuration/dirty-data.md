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

Dirty handling is optional. With neither `dirty.collector` nor `dirty.validator`,
existing sink behavior is unchanged. An optional validator can reject a
`SeaTunnelRow` before it reaches the sink and send that exact row to its collector.
Every delegate write exception still fails the write, including classified row
errors: a buffered batch failure may concern an earlier row. Collector-only mode
does not intercept sink exceptions. Sinks with their own exact row attribution
may explicitly invoke the collector API.

## Configuration and precedence

Both blocks can be placed in `env` or a sink. A sink-level `dirty.collector`
replaces the complete env collector block; its fields are not merged individually.
The validator block inherits independently from env when the sink omits it.
An explicit sink-level null block disables that inherited collector or validator.
Unknown collector/validator types or initialization errors fail explicitly, including
on the Flink starter path. An absent collector uses `noop`; a present collector
block must contain a non-blank `type`. A configured validator must include `type`
and requires an active collector. Validator errors fail the job rather than
silently accepting the record.

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
  dirty.collector {
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
  Console {}
}
```

The example fails if its collector receives a dirty record. It does not mark all
rows as dirty: the source rows are valid and no validator is configured.

## Built-in collectors

`type = "noop"` (alias `none`) performs no collection. `type = "log"` logs dirty
records and maintains a collector-instance counter. The default counter is local
to that instance; `threshold` is evaluated per collector/task, not as a globally
enforced or checkpoint-persisted total on Zeta, Flink or Spark. Restarts/retries
can reset or replay counts and collection side effects; collectors must provide
their own durable recovery and deduplication when required.

| log collector option | Type | Default | Meaning |
| --- | --- | --- | --- |
| `log_level` | string | `ERROR` | `ERROR`, `WARN`, `INFO` or `DEBUG`; other values use ERROR. |
| `include_stack_trace` | boolean | `true` | Include exception details in the message. |
| `threshold` | long | `-1` | Count at which the threshold action runs; non-positive disables it. |
| `fail_on_threshold` | boolean | `false` | Throw when count reaches or exceeds a positive threshold; otherwise warn once per collector instance. |
| `log_payload` | string | `false` | `false` logs arity, `fields` logs field names, `full` logs values. |
| `mask_fields` | list of strings | `[]` | Case-insensitive fields masked when logging full values. |

Keep payload logging disabled for data that must not appear in logs. If full
payload logging is deliberately enabled, configure the required field masking.
Validation requires an active collector; `noop` cannot silently discard rejected
records. Configure a collector with the required disposition and delivery guarantees.

## Custom SPI implementations

A collector implements `DirtyRecordCollectorProvider` (`getType`, `createCollector`)
and returns a serializable `DirtyRecordCollector`. A validator implements
`DirtyDataValidator` and returns `ValidationResult.clean()` or a dirty result from
`validate(row, catalogTable)`. Register provider and validator class names in:

```text
META-INF/services/org.apache.seatunnel.api.table.factory.Factory
```

Initialization runs during job planning and the policy is serialized to workers.
There is no worker-side init callback after deserialization. Keep policy state
serializable, and lazily create worker resources when needed; do not depend on
live transient resources opened by the planning instance. Implement close for
resources owned by each instance.

Package the implementation and service entry in a JAR available to both job
planning and the engine's worker/translation classloaders. Select a collector by
its provider type. Select a validator by its factory identifier or class name:

```hocon
sink {
  Console {
    dirty.collector { type = "log", threshold = 1, fail_on_threshold = true }
    dirty.validator { type = "com.example.RequiredFieldsValidator" }
  }
}
```

The custom validator JAR must exist before using that example. `AlwaysDirtyDataValidator`
and the counting collector shipped with the test scenarios are demonstration tools;
the former rejects every row and is not a production validation rule. Neither is
selected by default. The counting collector never formats payloads or error text;
it emits one count summary on close and at most one nonfatal threshold notice.
A fatal threshold continues to throw on every call at or above the threshold.

## Runtime boundaries

The configured wrapper runs around real sink writers on Zeta, Flink and Spark.
Only synchronous single-table inputs and writers are supported. Active dirty
handling rejects multiple input catalog tables during planning, including per-table
sink-template fallback; policies are resolved before sink preparation/save-mode side
effects. Multi-table/async writers,
deferred terminal outcomes and enabled Zeta sink error-handler policies are rejected
at startup when dirty handling is active; they are not silently downgraded.
Without dirty handling, those existing paths keep their original behavior. Zeta
can aggregate even a single JDBC input into a multi-table writer; active dirty
handling rejects that aggregated path as well. Pre-write validation does not
bypass the writer capability restriction.

Validation runs before `write`. Zeta reports this wrapper's explicit validation
rejections as DROPPED. A normal delegate return retains its existing accepted-write
accounting; later flush, checkpoint or commit can still fail. Explicit collector
callbacks by custom sink code do not establish the delegate's row disposition or
automatically produce DROPPED metrics. All delegate write exceptions propagate
unchanged, so JDBC batch or latched flush failures cannot collect a healthy current
row by mistake. Schema-capable delegates
retain their schema callbacks and physical sink-table identity. Errors during commit, checkpoint, schema changes
and close still propagate through their existing lifecycle. The wrapper delegates
state/commit calls and closes its collector with the writer; it does not add durable
error storage or replay. Use a custom collector when durable rejected-record storage
is required, and validate that collector's delivery and recovery guarantees.
