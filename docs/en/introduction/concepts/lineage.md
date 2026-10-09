---
title: OpenLineage Lineage Reporting
---

<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# OpenLineage Lineage Reporting

SeaTunnel can emit table-level OpenLineage `RunEvent` records to any OpenLineage-compatible
receiver. The feature is disabled by default. When it is disabled, SeaTunnel does not create a
lineage network call or a lineage-specific background thread, and existing job behavior is
unchanged.

## Configuration

The following options are available in a job `env` block unless noted otherwise.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `openlineage_enabled` | Boolean | `false` | Enables OpenLineage reporting. |
| `openlineage_transport` | String | `http` | Transport name selected from the `LineageBackend` SPI. |
| `openlineage_url` | String | None | Base URL of the OpenLineage receiver. Required when reporting is enabled. |
| `openlineage_namespace` | String | `seatunnel` | OpenLineage job namespace. |
| `openlineage_auth_token` | String | None | Receiver bearer token. It may come only from an environment variable or cluster configuration. |
| `openlineage_timeout_ms` | Int | `10000` | Timeout for one send attempt, in milliseconds. |
| `openlineage_retry_times` | Int | `3` | Number of retries after a failed send. |
| `openlineage_run_facet` | String | `seatunnel_properties` | Name of the custom run facet. |
| `openlineage_run_properties` | Map | None | Custom properties copied to the run facet. |
| `openlineage_heartbeat_min_interval_ms` | Long | `3600000` | Minimum interval between streaming-job heartbeat events, in milliseconds. `0` disables heartbeat reporting. |
| `openlineage_producer` | String | `https://seatunnel.apache.org/<version>` | OpenLineage producer identifier. The default is derived from the running SeaTunnel version at runtime. |

Values are resolved independently with this precedence:

```text
job env {} > process environment > cluster configuration > default
```

The token is the exception to this rule. `openlineage_auth_token` is resolved as
`OPENLINEAGE_AUTH_TOKEN` > cluster configuration. A token in a job `env {}` block is rejected at
job startup. This prevents the token from being persisted in job configuration or serialized into
the Flink JobGraph.

Lineage delivery is unconditionally best effort, and there is deliberately no option to change that.
A receiver or network failure is contained at the engine integration boundary and reported as a
warning, so it can never change the outcome of the data-processing job.

### Environment variable names

The environment variable name is the upper-case option name:

| Option | Environment variable |
| --- | --- |
| `openlineage_enabled` | `OPENLINEAGE_ENABLED` |
| `openlineage_transport` | `OPENLINEAGE_TRANSPORT` |
| `openlineage_url` | `OPENLINEAGE_URL` |
| `openlineage_namespace` | `OPENLINEAGE_NAMESPACE` |
| `openlineage_auth_token` | `OPENLINEAGE_AUTH_TOKEN` |
| `openlineage_timeout_ms` | `OPENLINEAGE_TIMEOUT_MS` |
| `openlineage_retry_times` | `OPENLINEAGE_RETRY_TIMES` |
| `openlineage_run_facet` | `OPENLINEAGE_RUN_FACET` |
| `openlineage_run_properties` | `OPENLINEAGE_RUN_PROPERTIES` |
| `openlineage_heartbeat_min_interval_ms` | `OPENLINEAGE_HEARTBEAT_MIN_INTERVAL_MS` |
| `openlineage_producer` | `OPENLINEAGE_PRODUCER` |

For example, set `OPENLINEAGE_URL` and `OPENLINEAGE_AUTH_TOKEN` through the process or service
manager environment. Do not put a real token in a job file, repository file, command history, or
logs.

## Dataset names

Supported connector dataset identities use these canonical forms. For multi-table sources and sinks,
each configured table is represented separately.

| Connector | Namespace | Name |
| --- | --- | --- |
| Paimon | `paimon://<catalog_name>/<database>` | `<table>` |
| Doris | `mysql://<fe_host>:<query_port>` | `<database>.<table>` |
| JDBC | `<scheme>://<host>:<port>` | `<database>.<table>` |

For Paimon, `<catalog_name>` comes from the connector's `catalog_name` option and the dataset name is
the table name only, not `database.table`. Set `catalog_name` explicitly to keep the namespace stable
across jobs; when it is omitted, no Paimon dataset is emitted. For Doris, `fenodes` normally contains the HTTP Stream Load port; the
lineage namespace uses the Doris query port instead. The generic `default.default.default` table path
is not emitted.

A sink that routes rows with a template, such as `table = "${table_name}"`, produces no dataset
either. The template is substituted per row at write time, so it never names a real table, and
emitting it verbatim would collapse every template-routed job onto one shared node in the graph. A
name that merely contains a dollar sign, such as `orders$archive`, is a normal identifier and is
still emitted.

## Run facet properties

Besides the properties configured through `openlineage_run_properties`, the run facet carries an
`engine` property naming the execution engine that reported the run. Each engine adds the
identifiers that make its run findable in that engine's own UI; those are described in the engine
sections below.

## Zeta

### Zeta cluster configuration

Zeta reads cluster-level values from `seatunnel.yaml`:

```yaml
seatunnel:
  engine:
    openlineage:
      enabled: true
      transport: http
      url: http://lineage.example/api/lineage
      namespace: seatunnel
      timeout_ms: 10000
      retry_times: 3
      run_facet: seatunnel_properties
      heartbeat_min_interval_ms: 3600000
      producer: https://seatunnel.apache.org/<version>
```

The cluster-level `auth_token` value is supported, but should be supplied through the cluster's
secret-management mechanism. It must not be copied into a job `env` block.

### Events reported by Zeta

Zeta emits `START` when a job enters `RUNNING`, then emits `COMPLETE`, `ABORT`, or `FAIL` for the
corresponding terminal state. `SAVEPOINT_DONE` is represented as `COMPLETE`, while `UNKNOWABLE` is
represented as `FAIL`. The run facet carries a `sink_action` property in addition to `engine`.

Zeta reads terminal output metrics from historical job metrics, preferring committed counters and
falling back to attempted write counters when no positive committed counter is available. The event
records which of the two was used, so a consumer is never told that attempted rows are committed
ones.

A job that has not finished reports periodic heartbeat events, so a receiver does not mistake a
still-running job for one whose producer died. Zeta emits them from its checkpoint completion
callback, so a Zeta job with checkpointing disabled has no heartbeat. They are throttled by
`openlineage_heartbeat_min_interval_ms`, and setting it to `0` stops them. A run that does stop
reporting is subject to the receiver's abandoned-run timeout, and the state inferred that way is
replaced by a terminal event arriving later.
