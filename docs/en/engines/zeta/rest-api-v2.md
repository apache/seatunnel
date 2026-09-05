# RESTful API V2

SeaTunnel has a monitoring API that can be used to query status and statistics of running jobs, as well as recent
completed jobs. The monitoring API is a RESTful API that accepts HTTP requests and responds with JSON data.

:::tip
This API is provided by the SeaTunnel Engine (Zeta) server, so it is only available for jobs running on the Zeta
engine. It is not available when a job runs on the Flink or Spark engine; use that engine's own tooling to submit
and monitor jobs in that case.
:::

## Overview

The v2 API and the Web UI are both served by the embedded Jetty server. Jetty starts only when
`seatunnel.engine.http.enable-http = true` or `enable-https = true`.

There are two different "default" sources that are easy to mix up:

- Code defaults: `enable-http = false`, `enable-https = false`, `port = 8080`, `context-path = ""`, `enable-dynamic-port = false`, `port-range = 100`
- The packaged `seatunnel.yaml` example: it already sets `enable-http: true` and `port: 8080`

As a result, if you start SeaTunnel with the packaged configuration, the Web UI and REST API usually
listen on `http://<host>:8080/`. If you build a minimal config yourself, rely on code defaults, or
remove `enable-http`, Jetty will not start by default.

Use the following configuration for a fixed port:

```yaml

seatunnel:
  engine:
    http:
      enable-http: true
      port: 8080
```

If you want Jetty to choose the first free port between `port` and `port + port-range`, enable
dynamic ports explicitly:

```yaml

seatunnel:
  engine:
    http:
      enable-http: true
      port: 8080
      enable-dynamic-port: true
      port-range: 100
```

`context-path` can also be configured as follows:

```yaml

seatunnel:
  engine:
    http:
      enable-http: true
      port: 8080
      context-path: /seatunnel
```

## Web UI and Port 8080 Troubleshooting

- If `http://<host>:8080/` is unreachable, first check whether `seatunnel.engine.http.enable-http` or `enable-https` is actually enabled. The `network.rest-api.enabled` setting in `hazelcast.yaml` does not replace the Jetty switch.
- If `enable-dynamic-port = true`, the actual listening port may not be 8080. Jetty will choose the first available port between `port` and `port + port-range`. Use the startup log `SeaTunnel REST service will start on port xxx` as the source of truth.
- If `context-path = /seatunnel`, both the Web UI and REST endpoints move under that prefix. For example, the overview endpoint becomes `/seatunnel/overview`.
- The Web UI static resources and REST endpoints share the same Jetty service. If Jetty does not start, both are unavailable together.

## Enable HTTPS

Please refer [security](security.md)

## API reference

### Get Connector Option Rules

<details>
 <summary><code>GET</code> <code><b>/option-rules?type=source&plugin=FakeSource</b></code> <code>(Returns the full runtime OptionRule metadata of a connector.)</code></summary>

#### Parameters

> |  name  |   type   | data type |                            description                             |
> |--------|----------|-----------|--------------------------------------------------------------------|
> | type   | required | string    | plugin type, supports `source`, `sink` and `transform`             |
> | plugin | required | string    | connector factory identifier, for example `FakeSource` or `Console` |

#### Responses

```json
{
  "engineType": "seatunnel",
  "pluginType": "source",
  "pluginName": "FakeSource",
  "optionRule": {
    "optionalOptions": [
      {
        "key": "row.num",
        "type": "java.lang.Integer",
        "defaultValue": 5,
        "description": "The total number of data generated per degree of parallelism",
        "fallbackKeys": [],
        "optionValues": null
      }
    ],
    "requiredOptions": [
      {
        "ruleType": "EXCLUSIVE",
        "options": [
          {
            "key": "schema",
            "type": "org.apache.seatunnel.api.table.catalog.TableSchema",
            "defaultValue": null,
            "description": "The schema of the upstream table",
            "fallbackKeys": [],
            "optionValues": null
          }
        ]
      },
      {
        "ruleType": "CONDITIONAL",
        "options": [
          {
            "key": "string.template",
            "type": "java.util.List<java.lang.String>",
            "defaultValue": null,
            "description": "The template list of string type that connector generated, if user configured it, connector will randomly select an item from the template list",
            "fallbackKeys": [],
            "optionValues": null
          }
        ],
        "expression": "'string.fake.mode' == TEMPLATE",
        "expressionTree": {
          "condition": {
            "option": {
              "key": "string.fake.mode",
              "type": "org.apache.seatunnel.connectors.seatunnel.fake.config.FakeSourceOptions$FakeMode",
              "defaultValue": "RANDOM",
              "description": "The fake mode of generating string data",
              "fallbackKeys": [],
              "optionValues": [
                "RANDOM",
                "TEMPLATE"
              ]
            },
            "expectValue": "TEMPLATE",
            "compareOperator": null,
            "compareOption": null,
            "conditionOperator": "EQUAL",
            "conditionOperatorCategory": "EQUALITY",
            "operator": null,
            "next": null
          },
          "operator": null,
          "next": null
        }
      }
    ],
    "conditionRules": [],
    "valueConstraints": [
      {
        "expression": "'row.num' >= 1",
        "conditionTree": {
          "option": {
            "key": "row.num",
            "type": "java.lang.Integer",
            "defaultValue": 5,
            "description": "The total number of data generated per degree of parallelism",
            "fallbackKeys": [],
            "optionValues": null
          },
          "expectValue": 1,
          "compareOperator": ">=",
          "compareOption": null,
          "conditionOperator": "GREATER_OR_EQUAL",
          "conditionOperatorCategory": "NUMERIC",
          "operator": null,
          "next": null
        }
      },
      {
        "expression": "'port' must be between 1 and 65535",
        "conditionTree": {
          "option": {
            "key": "port",
            "type": "java.lang.Integer",
            "defaultValue": null,
            "description": "Server port",
            "fallbackKeys": [],
            "optionValues": null
          },
          "expectValue": "must be between 1 and 65535",
          "compareOperator": "extension",
          "compareOption": null,
          "conditionOperator": "EXTENSION",
          "conditionOperatorCategory": "EXTENSION",
          "operator": null,
          "next": null
        }
      }
    ]
  }
}
```

**Notes:**
- The response is resolved from runtime plugin discovery, so it follows the connector version installed on the server.
- `requiredOptions[].ruleType` can be `ABSOLUTELY_REQUIRED`, `EXCLUSIVE`, `BUNDLED`, or `CONDITIONAL`.
- `optionRule.conditionRules` recursively exposes nested conditional option rules and is an empty array when the connector does not define nested rules.
- For conditional rules, both `expression` and `expressionTree` are returned for dynamic form rendering.
- `optionRule.valueConstraints` describes value-level validation rules such as numeric ranges, string patterns, and cross-field comparisons. Each entry provides a human-readable `expression` string alongside a structured `conditionTree` for programmatic use. This array is empty when the connector does not define any value constraints.
- Within `conditionTree`, the `compareOperator` field is `null` for `EQUAL` and otherwise uses the operator symbol exposed by the runtime rule (for example `>=`, `is not blank`, or `extension`). The `compareOption` field is populated only for cross-field comparisons.
- `conditionOperator` is a stable operator identifier. Possible values include `EQUAL`, `GREATER_OR_EQUAL`, `NOT_BLANK`, `FIELD_LESS_THAN`, `EXTENSION`, etc. `conditionOperatorCategory` indicates the operator category, such as `NUMERIC`, `STRING`, `COLLECTION`, `EQUALITY`, `EXTENSION`, etc.
- For `EXTENSION` conditions, `expectValue` carries the rule description text returned by `ConditionExtension.description()`.

</details>

------------------------------------------------------------------------------------------

### Returns an overview over the Zeta engine cluster.

<details>
 <summary><code>GET</code> <code><b>/overview?tag1=value1&tag2=value2</b></code> <code>(Returns an overview over the Zeta engine cluster.)</code></summary>

#### Parameters

> |   name   |   type   | data type |                                             description                                              |
> |----------|----------|-----------|------------------------------------------------------------------------------------------------------|
> | tag_name | optional | string    | the tags filter, you can add tag filter to get those matched worker count, and slot on those workers |

#### Responses

```json
{
    "projectVersion":"2.3.10-SNAPSHOT",
    "gitCommitAbbrev":"DeadD0d0",
    "totalSlot":"0",
    "unassignedSlot":"0",
    "works":"1",
    "runningJobs":"0",
    "pendingJobs":"0",
    "finishedJobs":"0",
    "failedJobs":"0",
    "cancelledJobs":"0"
}
```

**Notes:**
- If you use `dynamic-slot`, the `totalSlot` and `unassignedSlot` always be `0`. when you set it to fix slot number, it will return the correct total and unassigned slot number
- If the url has tag filter, the `works`, `totalSlot` and `unassignedSlot` will return the result on the matched worker. but the job related metric will always return the cluster level information.

</details>

------------------------------------------------------------------------------------------

### Query Worker Resources

<details>
 <summary><code>GET</code> <code><b>/resource/workers</b></code> <code>(Returns the current resource snapshot for registered workers.)</code></summary>

#### Parameters

None.

#### Responses

```json
{
  "available": true,
  "collectedAt": 1723017600000,
  "workers": [
    {
      "address": "10.0.0.8:5801",
      "tags": {"region": "us-west"},
      "totalSlots": 4,
      "freeSlots": 1,
      "usedSlots": 3,
      "dynamicSlot": false,
      "totalCpuCores": 8,
      "availableCpuCores": 2,
      "totalHeapMemoryBytes": 17179869184,
      "availableHeapMemoryBytes": 4294967296,
      "cpuUsage": 0.42,
      "memUsage": 0.58,
      "runningJobIds": [123456789]
    },
    {
      "address": "10.0.0.9:5801",
      "tags": {},
      "totalSlots": 2,
      "freeSlots": 0,
      "usedSlots": 2,
      "dynamicSlot": true,
      "totalCpuCores": 8,
      "availableCpuCores": 4,
      "totalHeapMemoryBytes": 17179869184,
      "availableHeapMemoryBytes": 8589934592,
      "cpuUsage": 0.35,
      "memUsage": 0.41,
      "runningJobIds": [123456789]
    }
  ]
}
```

**Notes:**

- Fixed-slot workers return `totalSlots`, `usedSlots`, and `freeSlots`.
- Dynamic-slot workers do not have a fixed slot capacity. For them, `totalSlots` is the number of currently tracked assigned and unassigned slots, while `freeSlots` is the currently unassigned count. Use `dynamicSlot` together with the CPU and heap fields when interpreting capacity.
- `available` is `false` when the master resource snapshot cannot be read, including the master-election window. In that case, `workers` is empty and clients should retry instead of interpreting the response as an empty cluster.
- `collectedAt` is the timestamp in milliseconds when the master built this response. Worker values come from the latest resource-manager heartbeat and are not an atomic sample with `/system-monitoring-information`.
- Resource and usage fields are omitted until the worker heartbeat contains those values.

</details>

------------------------------------------------------------------------------------------

### Query An Overview And State Of Running Jobs

<details>
 <summary><code>GET</code> <code><b>/running-jobs?page=1&rows=10</b></code> <code>(Query an overview over running jobs and their current state.)</code></summary>

#### Parameters

> | name  |   type   | data type | description                                                                       |
> |-------|----------|-----------|-----------------------------------------------------------------------------------|
> | page  | optional | int       | page number.                                                                      |
> | rows  | optional | int       | page size.                                                                        |

#### Responses

```json
[
  {
    "jobId": "",
    "jobName": "",
    "jobStatus": "",
    "envOptions": {
    },
    "createTime": "",
    "jobDag": {
      "jobId": "",
      "envOptions": [],
      "vertexInfoMap": [
        {
          "vertexId": 1,
          "type": "",
          "vertexName": "",
          "tablePaths": [
            ""
          ]
        }
      ],
      "pipelineEdges": {}
    },
    "pluginJarsUrls": [
    ],
    "isStartWithSavePoint": false,
    "metrics": {
      "sourceReceivedCount": "",
      "sinkWriteCount": ""
    }
  }
]
```

</details>

------------------------------------------------------------------------------------------

### Returns Diagnostic Information For Pending Jobs

<details>
 <summary><code>GET</code> <code><b>/pending-jobs?jobId=123&limit=10</b></code> <code>(Inspect the pending queue, slot usage and blocking reasons.)</code></summary>

#### Parameters

> |   name   |   type   | data type | description                                                                 |
> |----------|----------|-----------|-----------------------------------------------------------------------------|
> | jobId    | optional | long      | If set, only returns the diagnostics for the specified job. When both `jobId` and `limit` are provided, `jobId` takes precedence and `limit` is ignored. |
> | limit    | optional | integer   | Limits the number of jobs returned. This parameter is ignored when `jobId` is provided. |
> | pretty   | optional | boolean   | When `true`, pretty-print JSON and format timestamp fields.                 |

#### Responses

```json
{
  "queueSummary": {
    "size": 2,
    "scheduleStrategy": "WAIT",
    "oldestEnqueueTimestamp": 1717500000000,
    "newestEnqueueTimestamp": 1717500005000,
    "lackingTaskGroups": 6
  },
  "clusterSnapshot": {
    "totalSlots": 8,
    "freeSlots": 1,
    "assignedSlots": 7,
    "workerCount": 2,
    "workers": [
      {
        "address": "10.0.0.8:5801",
        "tags": {
          "zone": "az1"
        },
        "totalSlots": 4,
        "freeSlots": 0,
        "usedSlots": 4,
        "dynamicSlot": false,
        "totalCpuCores": 8,
        "availableCpuCores": 2,
        "totalHeapMemoryBytes": 17179869184,
        "availableHeapMemoryBytes": 4294967296,
        "cpuUsage": 0.83,
        "memUsage": 0.64,
        "runningJobIds": [
          1001,
          1002
        ]
      }
    ]
  },
  "pendingJobs": [
    {
      "jobId": 1003,
      "jobName": "cdc_mysql_to_es",
      "pendingSourceState": "SUBMIT",
      "jobStatus": "PENDING",
      "enqueueTimestamp": 1717500000000,
      "checkTime": 1717500005000,
      "waitDurationMs": 5000,
      "checkCount": 3,
      "totalTaskGroups": 16,
      "allocatedTaskGroups": 10,
      "lackingTaskGroups": 6,
      "failureReason": "REQUEST_FAILED",
      "failureMessage": "NoEnoughResourceException: can't apply resource request",
      "tagFilter": {},
      "blockingJobIds": [
        1001
      ],
      "pipelines": [
        {
          "pipelineId": 1,
          "pipelineName": "Job job-name, Pipeline: [(1/2)]",
          "totalTaskGroups": 8,
          "allocatedTaskGroups": 5,
          "lackingTaskGroups": 3,
          "taskGroupDiagnostics": [
            {
              "taskGroupLocation": {
                "jobId": 1003,
                "pipelineId": 1,
                "taskGroupId": 1
              },
              "taskFullName": "Source[0]",
              "allocated": false,
              "failureReason": "REQUEST_FAILED",
              "failureMessage": "NoEnoughResourceException: slot not enough"
            }
          ]
        }
      ],
      "lackingTaskGroupDiagnostics": [
        {
          "taskGroupLocation": {
            "jobId": 1003,
            "pipelineId": 1,
            "taskGroupId": 1
          },
          "taskFullName": "Source[0]",
          "allocated": false,
          "failureReason": "REQUEST_FAILED",
          "failureMessage": "NoEnoughResourceException: slot not enough"
        }
      ]
    }
  ]
}
```

When `pretty=true`, the endpoint returns a pretty-printed JSON response and formats `oldestEnqueueTimestamp`, `newestEnqueueTimestamp`, `enqueueTimestamp`, and `checkTime` as `yyyy-MM-dd HH:mm:ss`.

This endpoint helps troubleshoot why jobs stay in `PENDING` by showing the pending queue order, aggregated resource view, and per task-group slot request failures (tag mismatch, worker busy, resource exhausted, etc.).

**Pending Jobs Response Fields**

- **queueSummary** – overview of the entire pending queue.
  - `size`: number of jobs currently pending.
  - `scheduleStrategy`: strategy in use (e.g. `WAIT`, `FAIL_FAST`) that dictates what happens when resources are insufficient.
  - `oldestEnqueueTimestamp` / `newestEnqueueTimestamp`: timestamps (ms) of the oldest/latest job in the queue.
  - `lackingTaskGroups`: total TaskGroup count still waiting for slots. **Note**: This value reflects only the jobs included in the current response (i.e., the subset limited by the `limit` parameter or filtered by `jobId`), not the entire pending queue. To view the complete statistics for all pending jobs, call this API without the `limit` parameter.
- **clusterSnapshot** – cluster resource snapshot (can be filtered by tags).
  - `totalSlots` / `assignedSlots` / `freeSlots`: total, allocated and remaining slots in the filtered view.
  - `workerCount`: number of workers that match the tag filters.
  - `workers[]`: per-worker details:
    - `address`: host:port of the worker.
    - `tags`: worker-level tags.
    - `totalSlots` / `freeSlots`: slot capacity and available slot count on that worker.
    - `dynamicSlot`: whether the worker uses dynamic slot allocation.
    - `cpuUsage` / `memUsage`: sampled system load (only present when `slot-allocate-strategy` is `SYSTEM_LOAD`).
    - `runningJobIds[]`: jobs currently occupying slots on that worker (helps identify blockers).
- **pendingJobs[]** – diagnostics for each pending job.
  - `jobId` / `jobName`: identifiers.
  - `pendingSourceState`: whether the job comes from a new submission (`SUBMIT`) or master switch restore (`RESTORE`).
  - `jobStatus`: status recorded in the physical plan (typically `PENDING`).
  - `enqueueTimestamp`: when the job entered the pending queue.
  - `checkTime`: timestamp of the latest diagnostic snapshot.
  - `waitDurationMs`: `checkTime - enqueueTimestamp`.
  - `checkCount`: how many times the scheduler has checked this job.
  - `totalTaskGroups` / `allocatedTaskGroups` / `lackingTaskGroups`: TaskGroup totals vs. assigned vs. lacking.
  - `failureReason` / `failureMessage`: classified cause (e.g. `RESOURCE_NOT_ENOUGH`, `REQUEST_FAILED`) plus raw message.
  - `tagFilter`: worker tag requirements declared by the job (if any).
  - `blockingJobIds[]`: other jobs that currently occupy the required slots.
  - `pipelines[]`: per-pipeline breakdown.
    - `pipelineId` / `pipelineName`.
    - `totalTaskGroups` / `allocatedTaskGroups` / `lackingTaskGroups`.
    - `taskGroupDiagnostics[]` (per TaskGroup slot request state):
      - `taskGroupLocation` (`jobId`, `pipelineId`, `taskGroupId`).
      - `taskFullName`: human-readable name (source/sink, etc.).
      - `allocated`: whether the slot request succeeded.
      - `failureReason` / `failureMessage`: task-level cause when allocation failed.
  - `lackingTaskGroupDiagnostics[]`: flattened list of `allocated=false` TaskGroups for quick review.

</details>

------------------------------------------------------------------------------------------

### Return Details Of A Job

<details>
 <summary><code>GET</code> <code><b>/job-info/:jobId</b></code> <code>(Return details of a job. )</code></summary>

#### Parameters

> | name  |   type   | data type | description |
> |-------|----------|-----------|-------------|
> | jobId | required | long      | job id      |

#### Responses

```json
{
  "jobId": "",
  "jobName": "",
  "jobStatus": "",
  "createTime": "",
  "jobDag": {
    "jobId": "",
    "envOptions": [],
    "vertexInfoMap": [
      {
        "vertexId": 1,
        "type": "",
        "vertexName": "",
        "tablePaths": [
          ""
        ]
      }
    ],
    "pipelineEdges": {}
  },
  "metrics": {
    "IntermediateQueueSize": "",
    "SourceReceivedCount": "",
    "SourceReceivedQPS": "",
    "SourceReceivedBytes": "",
    "SourceReceivedBytesPerSeconds": "",
    "SinkWriteCount": "",
    "SinkWriteQPS": "",
    "SinkWriteBytes": "",
    "SinkWriteBytesPerSeconds": "",
    "SinkCommittedCount": "",
    "SinkCommittedQPS": "",
    "SinkCommittedBytes": "",
    "SinkCommittedBytesPerSeconds": "",
    "TableSourceReceivedCount": {},
    "TableSourceReceivedBytes": {},
    "TableSourceReceivedBytesPerSeconds": {},
    "TableSourceReceivedQPS": {},
    "TableSinkWriteCount": {},
    "TableSinkWriteQPS": {},
    "TableSinkWriteBytes": {},
    "TableSinkWriteBytesPerSeconds": {},
    "TableSinkCommittedCount": {},
    "TableSinkCommittedQPS": {},
    "TableSinkCommittedBytes": {},
    "TableSinkCommittedBytesPerSeconds": {}
  },
  "finishedTime": "",
  "errorMsg": null,
  "envOptions": {
  },
  "pluginJarsUrls": [
  ],
  "isStartWithSavePoint": false,
  "diagnostics": {
    "jobId": "",
    "generatedAt": 1755000004000,
    "stateTimestamps": {
      "INITIALIZING": 1755000000000,
      "CREATED": 1755000000200,
      "SCHEDULED": 1755000001000,
      "RUNNING": 1755000003000
    },
    "pipelines": [
      {
        "pipelineId": 1,
        "pipelineStatus": "RUNNING",
        "restoreCount": 7,
        "maxRestoreCount": 100,
        "stateTimestamps": {
          "INITIALIZING": 1755000000000,
          "CREATED": 1755000000200,
          "SCHEDULED": 1755000001100,
          "DEPLOYING": 1755000002000,
          "RUNNING": 1755000003500
        }
      }
    ],
    "totalPipelineRestoreCount": 7
  }
}
```

`jobId`, `jobName`, `jobStatus`, `createTime`, `jobDag`, `metrics` always be returned.
`envOptions`, `pluginJarsUrls`, `isStartWithSavePoint` will return when job is running.
`finishedTime`, `errorMsg` will return when job is finished.
`diagnostics` will return when the job is running and its diagnostics can be read from the master
node. It is auxiliary information: if it can not be obtained, the field is omitted instead of
failing the request. Only this endpoint returns it; `/running-jobs` does not, because collecting it
for every running job would cost one more round trip to the master per job.

#### Metrics field description

| Field | Description |
| --- | --- |
| IntermediateQueueSize | Size of intermediate queue between operators |
| SourceReceivedCount | Total rows received from sources |
| SourceReceivedQPS | Source receive rate (rows/s) |
| SourceReceivedBytes | Total bytes received from sources |
| SourceReceivedBytesPerSeconds | Source receive rate (bytes/s) |
| SinkWriteCount | Sink write attempts (rows) |
| SinkWriteQPS | Sink write attempt rate (rows/s) |
| SinkWriteBytes | Sink write attempts (bytes) |
| SinkWriteBytesPerSeconds | Sink write attempt rate (bytes/s) |
| SinkCommittedCount | Sink committed rows after checkpoint succeeds |
| SinkCommittedQPS | Sink committed rate (rows/s) |
| SinkCommittedBytes | Sink committed bytes after checkpoint succeeds |
| SinkCommittedBytesPerSeconds | Sink committed rate (bytes/s) |
| TableSourceReceived* | Per-table source metrics, key format `TableSourceReceivedXXX#<table>` |
| TableSinkWrite* | Per-table sink write attempts, key format `TableSinkWriteXXX#<table>` |
| TableSinkCommitted* | Per-table sink committed metrics, key format `TableSinkCommittedXXX#<table>` |

#### Diagnostics field description

| Field | Description |
| --- | --- |
| generatedAt | Epoch millis when this diagnostics block was collected |
| stateTimestamps | Epoch millis when the job entered each state. States never entered are omitted. A pipeline restart does not change the job state, so this alone does not show restarts |
| pipelines[].pipelineId | Pipeline id inside the job |
| pipelines[].pipelineStatus | Current pipeline state |
| pipelines[].restoreCount | How many times this pipeline has been restored since the job was submitted. A value that keeps growing while `jobStatus` stays `RUNNING` is a crash loop |
| pipelines[].maxRestoreCount | Restore limit of this pipeline, from the `job.retry.times` env option |
| pipelines[].stateTimestamps | Epoch millis when the pipeline entered each state. After a restore, the timestamps of the new attempt overwrite the previous ones |
| totalPipelineRestoreCount | Sum of `restoreCount` over all pipelines of the job |

When we can't get the job info, the response will be:

```json
{
  "jobId" : ""
}
```

</details>

------------------------------------------------------------------------------------------

### Return Details Of A Job

This API has been deprecated, please use /job-info/:jobId instead

<details>
 <summary><code>GET</code> <code><b>/running-job/:jobId</b></code> <code>(Return details of a job. )</code></summary>

#### Parameters

> | name  |   type   | data type | description |
> |-------|----------|-----------|-------------|
> | jobId | required | long      | job id      |

#### Responses

```json
{
  "jobId": "",
  "jobName": "",
  "jobStatus": "",
  "createTime": "",
  "jobDag": {
    "jobId": "",
    "envOptions": [],
    "vertexInfoMap": [
      {
        "vertexId": 1,
        "type": "",
        "vertexName": "",
        "tablePaths": [
          ""
        ]
      }
    ],
    "pipelineEdges": {}
  },
  "metrics": {
    "IntermediateQueueSize": "",
    "SourceReceivedCount": "",
    "SourceReceivedQPS": "",
    "SourceReceivedBytes": "",
    "SourceReceivedBytesPerSeconds": "",
    "SinkWriteCount": "",
    "SinkWriteQPS": "",
    "SinkWriteBytes": "",
    "SinkWriteBytesPerSeconds": "",
    "TableSourceReceivedCount": {},
    "TableSourceReceivedBytes": {},
    "TableSourceReceivedBytesPerSeconds": {},
    "TableSourceReceivedQPS": {},
    "TableSinkWriteCount": {},
    "TableSinkWriteQPS": {},
    "TableSinkWriteBytes": {},
    "TableSinkWriteBytesPerSeconds": {}
  },
  "finishedTime": "",
  "errorMsg": null,
  "envOptions": {
  },
  "pluginJarsUrls": [
  ],
  "isStartWithSavePoint": false
}
```

`jobId`, `jobName`, `jobStatus`, `createTime`, `jobDag`, `metrics` always be returned.
`envOptions`, `pluginJarsUrls`, `isStartWithSavePoint` will return when job is running.
`finishedTime`, `errorMsg` will return when job is finished.

When we can't get the job info, the response will be:

```json
{
  "jobId" : ""
}
```

</details>

------------------------------------------------------------------------------------------

### Query Finished Jobs Info

<details>
 <summary><code>GET</code> <code><b>/finished-jobs/:state?page=1&rows=10</b></code> <code>(Query finished Jobs Info.)</code></summary>

#### Parameters

> | name  |   type   | data type | description                                                                       |
> |-------|----------|-----------|-----------------------------------------------------------------------------------|
> | state | optional | string    | finished job status. `FINISHED`,`CANCELED`,`FAILED`,`SAVEPOINT_DONE`,`UNKNOWABLE` |
> | page  | optional | int       | page number.                                                                      |
> | rows  | optional | int       | page size.                                                                        |

#### Responses

```json
[
  {
    "jobId": "",
    "jobName": "",
    "jobStatus": "",
    "errorMsg": null,
    "createTime": "",
    "finishTime": "",
    "jobDag": {
      "jobId": "",
      "envOptions": [],
      "vertexInfoMap": [
        {
          "vertexId": 1,
          "type": "",
          "vertexName": "",
          "tablePaths": [
            ""
          ]
        }
      ],
      "pipelineEdges": {}
    },
    "metrics": ""
  }
]
```

</details>

------------------------------------------------------------------------------------------

### Returns System Monitoring Information

<details>
 <summary><code>GET</code> <code><b>/system-monitoring-information</b></code> <code>(Returns system monitoring information.)</code></summary>

#### Parameters

#### Responses

```json
[
  {
    "processors":"8",
    "physical.memory.total":"16.0G",
    "physical.memory.free":"16.3M",
    "swap.space.total":"0",
    "swap.space.free":"0",
    "heap.memory.used":"135.7M",
    "heap.memory.free":"440.8M",
    "heap.memory.total":"576.5M",
    "heap.memory.max":"3.6G",
    "heap.memory.used/total":"23.54%",
    "heap.memory.used/max":"3.73%",
    "minor.gc.count":"6",
    "minor.gc.time":"110ms",
    "major.gc.count":"2",
    "major.gc.time":"73ms",
    "load.process":"24.78%",
    "load.system":"60.00%",
    "load.systemAverage":"2.07",
    "thread.count":"117",
    "thread.peakCount":"118",
    "cluster.timeDiff":"0",
    "event.q.size":"0",
    "executor.q.async.size":"0",
    "executor.q.client.size":"0",
    "executor.q.client.query.size":"0",
    "executor.q.client.blocking.size":"0",
    "executor.q.query.size":"0",
    "executor.q.scheduled.size":"0",
    "executor.q.io.size":"0",
    "executor.q.system.size":"0",
    "executor.q.operations.size":"0",
    "executor.q.priorityOperation.size":"0",
    "operations.completed.count":"10",
    "executor.q.mapLoad.size":"0",
    "executor.q.mapLoadAllKeys.size":"0",
    "executor.q.cluster.size":"0",
    "executor.q.response.size":"0",
    "operations.running.count":"0",
    "operations.pending.invocations.percentage":"0.00%",
    "operations.pending.invocations.count":"0",
    "proxy.count":"8",
    "clientEndpoint.count":"0",
    "connection.active.count":"2",
    "client.connection.count":"0",
    "connection.count":"0"
  }
]
```

</details>

------------------------------------------------------------------------------------------

### Submit A Job

<details>
<summary><code>POST</code> <code><b>/submit-job</b></code> <code>(Returns jobId and jobName if job submitted successfully.)</code></summary>

#### Parameters

> | name                 |   type   | data type | description                                              |
> |----------------------|----------|-----------|----------------------------------------------------------|
> | jobId                | optional | string    | job id                                                   |
> | jobName              | optional | string    | job name                                                 |
> | isStartWithSavePoint | optional | string    | if job is started with save point                        |
> | restoreMode          | optional | string    | Restore source for job recovery: `CHECKPOINT` or `SAVEPOINT`. Used together with `restoreSourceJobId`. See [Job Recovery and Restart](rest-api-job-lifecycle.md#6-job-recovery-and-restart). |
> | restoreSourceJobId   | optional | string    | The job id to restore from when `restoreMode` is set. When only `isStartWithSavePoint` is set (no `restoreMode`), this falls back to `jobId`. |
> | format               | optional | string    | config format, support json, hocon and sql, default json |

**Note:** The dry-run feature is intentionally not supported via the REST API. It is exclusively available through the SeaTunnel CLI.

#### Body

You can choose json, hocon or sql to pass request body.
The json format example:
``` json
{
    "env": {
        "job.mode": "batch"
    },
    "source": [
        {
            "plugin_name": "FakeSource",
            "plugin_output": "fake",
            "row.num": 100,
            "schema": {
                "fields": {
                    "name": "string",
                    "age": "int",
                    "card": "int"
                }
            }
        }
    ],
    "transform": [
    ],
    "sink": [
        {
            "plugin_name": "Console",
            "plugin_input": ["fake"]
        }
    ]
}
```
The hocon format example:
``` hocon
env {
  job.mode = "batch"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 100
    schema = {
      fields {
        name = "string"
        age = "int"
        card = "int"
      }
    }
  }
}

transform {
}

sink {
  Console {
    plugin_input = "fake"
  }
}

```

The SQL format example:
```sql
/* config
env {
  parallelism = 2
  job.mode = "BATCH"
}
*/

CREATE TABLE fake_source (
    id INT,
    name STRING,
    age INT
) WITH (
    'connector' = 'FakeSource',
    'rows' = '[
        { fields = [1, "Alice", 25], kind = INSERT },
        { fields = [2, "Bob", 30], kind = INSERT }
    ]',
    'schema' = '{
        fields {
            id = "int",
            name = "string",
            age = "int"
        }
    }',
    'type' = 'source'
);

CREATE TABLE console_sink (
    id INT,
    name STRING,
    age INT
) WITH (
    'connector' = 'Console',
    'type' = 'sink'
);

INSERT INTO console_sink SELECT * FROM fake_source;
```

#### Responses

```json
{
    "jobId": 733584788375666689,
    "jobName": "rest_api_test"
}
```

</details>

------------------------------------------------------------------------------------------

### Submit A Job By Upload Config File

<details>
<summary><code>POST</code> <code><b>/submit-job/upload</b></code> <code>(Returns jobId and jobName if job submitted successfully.)</code></summary>

#### Parameters

> | name                 |   type   | data type |            description            |
> |----------------------|----------|-----------|-----------------------------------|
> | jobId                | optional | string    | job id                            |
> | jobName              | optional | string    | job name                          |
> | isStartWithSavePoint | optional | string    | if job is started with save point |
> | restoreMode          | optional | string    | Restore source for job recovery: `CHECKPOINT` or `SAVEPOINT`. Used together with `restoreSourceJobId`. See [Job Recovery and Restart](rest-api-job-lifecycle.md#6-job-recovery-and-restart). |
> | restoreSourceJobId   | optional | string    | The job id to restore from when `restoreMode` is set. When only `isStartWithSavePoint` is set (no `restoreMode`), this falls back to `jobId`. |

#### Request Body
The name of the uploaded file key is config_file, and supports the following formats:
- `.json` files: parsed in JSON format
- `.conf` or `.config` files: parsed in HOCON format
- `.sql` files: parsed in SQL format, supports CREATE TABLE and INSERT INTO syntax

curl Example :
```bash
# Upload HOCON config file
curl --location 'http://127.0.0.1:8080/submit-job/upload' --form 'config_file=@"/temp/fake_to_console.conf"'

# Upload SQL config file
curl --location 'http://127.0.0.1:8080/submit-job/upload' --form 'config_file=@"/temp/job.sql"'

# Upload a config file and restore from the latest checkpoint of a previous job
curl --location 'http://127.0.0.1:8080/submit-job/upload?restoreMode=CHECKPOINT&restoreSourceJobId=733584788375666689' --form 'config_file=@"/temp/fake_to_console.conf"'
```
#### Responses

```json
{
    "jobId": 733584788375666689,
    "jobName": "SeaTunnel_Job"
}
```

</details>

------------------------------------------------------------------------------------------

### Batch Submit Jobs

<details>
<summary><code>POST</code> <code><b>/submit-jobs</b></code> <code>(Returns jobId and jobName if the job is successfully submitted.)</code></summary>

#### Parameters (add in the `params` field in the request body)

> |    Parameter Name     |   Required   |  Type   |              Description              |
> |----------------------|--------------|---------|---------------------------------------|
> | jobId                | optional     | string  | job id                                |
> | jobName              | optional     | string  | job name                              |
> | isStartWithSavePoint | optional     | string  | if the job is started with save point |

#### Request Body

```json
[
  {
    "params":{
      "jobId":"123456",
      "jobName":"SeaTunnel-01"
    },
    "env": {
      "job.mode": "batch"
    },
    "source": [
      {
        "plugin_name": "FakeSource",
        "plugin_output": "fake",
        "row.num": 1000,
        "schema": {
          "fields": {
            "name": "string",
            "age": "int",
            "card": "int"
          }
        }
      }
    ],
    "transform": [
    ],
    "sink": [
      {
        "plugin_name": "Console",
        "plugin_input": ["fake"]
      }
    ]
  },
  {
    "params":{
      "jobId":"1234567",
      "jobName":"SeaTunnel-02"
    },
    "env": {
      "job.mode": "batch"
    },
    "source": [
      {
        "plugin_name": "FakeSource",
        "plugin_output": "fake",
        "row.num": 1000,
        "schema": {
          "fields": {
            "name": "string",
            "age": "int",
            "card": "int"
          }
        }
      }
    ],
    "transform": [
    ],
    "sink": [
      {
        "plugin_name": "Console",
        "plugin_input": ["fake"]
      }
    ]
  }
]
```

#### Response

```json
[
  {
    "jobId": "123456",
    "jobName": "SeaTunnel-01"
  },{
    "jobId": "1234567",
    "jobName": "SeaTunnel-02"
  }
]
```

</details>

------------------------------------------------------------------------------------------

### Stop A Job

<details>
<summary><code>POST</code> <code><b>/stop-job</b></code> <code>(Returns jobId if job stopped successfully.)</code></summary>

#### Parameters

> | name                | required | data type | description                                                      |
> |---------------------|----------|-----------|------------------------------------------------------------------|
> | jobId               | yes      | long      | job id                                                           |
> | isStopWithSavePoint | no       | boolean   | If the job is stopped with a savepoint.                          |
> | force               | no       | boolean   | If true, the job is force-stopped (ignores isStopWithSavePoint). |


#### Body

```json
{
  "jobId": 733584788375666689,
  "isStopWithSavePoint": false,
  "force": false
}
```

#### Responses

```json
{
"jobId": 733584788375666689
}
```

**Notes:**
- If the job status is `DOING_SAVEPOINT` and the savepoint does not complete successfully, a forced stop (When the `force` option is enabled) will set the job status to `CANCELED`.
- A forced stop may leave checkpoint data incomplete or in an inconsistent state. It should be used only for exceptional or abnormal situations.

</details>

------------------------------------------------------------------------------------------
### Batch Stop Jobs

<details>
<summary><code>POST</code> <code><b>/stop-jobs</b></code> <code>(Returns jobId if the job is successfully stopped.)</code></summary>

#### Request Body

```json
[
  {
    "jobId": 881432421482889220,
    "isStopWithSavePoint": false,
    "force": false
  },
  {
    "jobId": 881432456517910529,
    "isStopWithSavePoint": false,
    "force": false
  }
]
```

#### Response

```json
[
  {
    "jobId": 881432421482889220
  },
  {
    "jobId": 881432456517910529
  }
]
```

</details>

------------------------------------------------------------------------------------------
### Encrypt Config

<details>
<summary><code>POST</code> <code><b>/encrypt-config</b></code> <code>(Returns the encrypted config if config is encrypted successfully.)</code></summary>
For more information about customize encryption, please refer to the documentation [config-encryption-decryption](../../introduction/configuration/config-encryption-decryption.md).

#### Body

```json
{
    "env": {
        "parallelism": 1,
        "shade.identifier":"base64"
    },
    "source": [
        {
            "plugin_name": "MySQL-CDC",
            "schema" : {
                "fields": {
                    "name": "string",
                    "age": "int"
                }
            },
            "plugin_output": "fake",
            "parallelism": 1,
            "hostname": "127.0.0.1",
            "username": "seatunnel",
            "password": "seatunnel_password",
            "table-name": "inventory_vwyw0n"
        }
    ],
    "transform": [
    ],
    "sink": [
        {
            "plugin_name": "Clickhouse",
            "host": "localhost:8123",
            "database": "default",
            "table": "fake_all",
            "username": "seatunnel",
            "password": "seatunnel_password"
        }
    ]
}
```

#### Responses

```json
{
    "env": {
        "parallelism": 1,
        "shade.identifier": "base64"
    },
    "source": [
        {
            "plugin_name": "MySQL-CDC",
            "schema": {
                "fields": {
                    "name": "string",
                    "age": "int"
                }
            },
            "plugin_output": "fake",
            "parallelism": 1,
            "hostname": "127.0.0.1",
            "username": "c2VhdHVubmVs",
            "password": "c2VhdHVubmVsX3Bhc3N3b3Jk",
            "table-name": "inventory_vwyw0n"
        }
    ],
    "transform": [],
    "sink": [
        {
            "plugin_name": "Clickhouse",
            "host": "localhost:8123",
            "database": "default",
            "table": "fake_all",
            "username": "c2VhdHVubmVs",
            "password": "c2VhdHVubmVsX3Bhc3N3b3Jk"
        }
    ]
}
```

</details>


------------------------------------------------------------------------------------------

### Update the tags of running node

<details><summary><code>POST</code><code><b>/update-tags</b></code><code>Updates the tags of the current REST node with the legacy flat-map request body</code><code>(If the update is successful, return a success message)</code></summary>


#### update node tags
##### Body
`/update-tags` keeps the legacy flat `Map` contract without reserving tag names or values:

```json
{
  "tag1": "dev_1",
  "tags": {
    "nested": "legacy-value"
  }
}
```

The Web UI uses `POST /update-local-member-tags` for the target-validated request format. It sends the request to its own REST origin, so open the UI from the target worker's REST address and use the member `uuid` from `/system-monitoring-information`; remote rows remain read-only and a mismatched UUID returns an error instead of updating a different node.

```json
{
  "uuid": "4f1c8c53-8d9f-4f5c-b9cc-278f3bbd2d2a",
  "tags": {
    "tag1": "dev_1",
    "tag2": "dev_2"
  }
}
```
##### Responses

```json
{
  "status": "success",
  "message": "update node tags done."
}
```
#### remove node tags
##### Body
Use an empty `tags` map with `POST /update-local-member-tags` to clear target node tags:

```json
{
  "uuid": "4f1c8c53-8d9f-4f5c-b9cc-278f3bbd2d2a",
  "tags": {}
}
```

An empty flat `Map` sent to `POST /update-tags` clears the current REST node:

```json
{}
```
##### Responses

```json
{
  "status": "success",
  "message": "update node tags done."
}
```

#### Request parameter exception
- If the parameter body is empty

##### Responses

```json
{
    "status": "fail",
    "message": "Request body is empty."
}
```
- If the parameter is not a `Map` object
##### Responses

```json
{
  "status": "fail",
  "message": "Invalid JSON format in request body."
}
```
</details>

------------------------------------------------------------------------------------------

### Get Logs from All Nodes

<details>
 <summary><code>GET</code> <code><b>/logs/:jobId</b></code> <code>(Returns a list of logs.)</code></summary>

#### Request Parameters

#### Parameters (to be added in the `params` field of the request body)

> |    Parameter Name     |   Required   |  Type   |            Description            |
> |-----------------------|--------------|---------|------------------------------------|
> | jobId                 |   optional   | string  | job id                            |

If `jobId` is empty, the request will return logs from all nodes. Otherwise, it will return the list of logs for the specified `jobId` from all nodes.

#### Response

Returns a list of logs from the requested nodes along with their content.

#### Return List of All Log Files

If you want to view the log list first, you can retrieve it via a `GET` request: `http://localhost:8080/logs?format=json`

```json
[
  {
    "node": "localhost:8080",
    "logLink": "http://localhost:8080/logs/job-899485770241277953.log",
    "logName": "job-899485770241277953.log"
  },
  {
    "node": "localhost:8080",
    "logLink": "http://localhost:8080/logs/job-899470314109468673.log",
    "logName": "job-899470314109468673.log"
  }
]
```

Supported formats are `json` and `html`, with `html` as the default.

#### Examples

Retrieve logs for `jobId` `733584788375666689` across all nodes: `http://localhost:8080/logs/733584788375666689`
Retrieve the list of logs from all nodes: `http://localhost:8080/logs`
Retrieve the list of logs in JSON format: `http://localhost:8080/logs?format=json`
Retrieve the content of a specific log file: `http://localhost:8080/logs/job-898380162133917698.log`

</details>

### Get Log Content from a Single Node

<details>
 <summary><code>GET</code> <code><b>/log</b></code> <code>(Returns a list of logs.)</code></summary>

#### Response

Returns a list of logs from the requested node.

#### Examples

To get a list of logs from the current node: `http://localhost:5801/log`
To get the content of a log file: `http://localhost:5801/log/job-898380162133917698.log`

</details>

------------------------------------------------------------------------------------------

### Read And Change Log Levels

A log level changed through these endpoints is a runtime override: it takes effect immediately, is
node local, and is lost when the node restarts. Levels that should survive a restart belong in
`config/log4j2.properties`, see [Logging](logging.md).

The root logger is addressed as `root`.

<details>
 <summary><code>GET</code> <code><b>/loggers</b></code> <code>(Returns the loggers of the running configuration.)</code></summary>

#### Query Parameters

> |  Parameter Name  |   Required   |  Type   |                                  Description                                   |
> |------------------|--------------|---------|--------------------------------------------------------------------------------|
> | scope            |   optional   | string  | `node` (default) answers for the node that serves the request, `cluster` asks every member |

#### Response

```json
{
  "node": "localhost:8080",
  "loggers": [
    {
      "name": "root",
      "level": "INFO",
      "origin": "file"
    },
    {
      "name": "org.apache.seatunnel.connectors.seatunnel.jdbc",
      "level": "DEBUG",
      "origin": "runtime-override",
      "fileLevel": "INFO"
    }
  ]
}
```

`origin` tells where the current level comes from: `file` for the level of the log4j2 configuration
file, `runtime-override` for a level that was set through one of the log level endpoints. `fileLevel`
is only present when an overridden logger is configured in the file as well, and reports the level a
`DELETE` puts back.

With `?scope=cluster` the answer is one entry per member:

```json
{
  "scope": "cluster",
  "status": "SUCCESS",
  "nodes": [
    {
      "node": "localhost:8080",
      "loggers": [
        {
          "name": "root",
          "level": "INFO",
          "origin": "file"
        }
      ]
    }
  ]
}
```

`status` is `SUCCESS` when every member answered, `PARTIAL_FAILURE` when some did not, and `FAILURE`
when none did; the member that failed carries its own `status` and `error`. A cluster request reaches
every member on the REST port of its configuration, so it does not reach members that took a
different port through `enable-dynamic-port`.

</details>

<details>
 <summary><code>GET</code> <code><b>/loggers/:name</b></code> <code>(Returns the effective level of one logger.)</code></summary>

#### Response

The level is resolved through the closest configured ancestor, so a logger that is not configured
itself can be asked about as well.

```json
{
  "name": "org.apache.seatunnel.connectors.seatunnel.jdbc",
  "level": "INFO",
  "origin": "file",
  "node": "localhost:8080"
}
```

</details>

<details>
 <summary><code>POST</code> <code><b>/loggers/:name</b></code> <code>(Overrides the level of one logger.)</code></summary>

#### Query Parameters

> |  Parameter Name  |   Required   |  Type   |                                  Description                                   |
> |------------------|--------------|---------|--------------------------------------------------------------------------------|
> | level            |   optional   | string  | `OFF`, `FATAL`, `ERROR`, `WARN`, `INFO`, `DEBUG`, `TRACE` or `ALL`, any letter case; may also be sent in the body |
> | scope            |   optional   | string  | `node` (default) changes the node that serves the request, `cluster` changes every member |

#### Body

```json
{
  "level": "DEBUG"
}
```

#### Response

```json
{
  "name": "org.apache.seatunnel.connectors.seatunnel.jdbc",
  "level": "DEBUG",
  "origin": "runtime-override",
  "node": "localhost:8080",
  "previousLevel": "INFO",
  "status": "SUCCESS"
}
```

An unknown level is rejected with `400` and the list of valid levels instead of being reported as
applied. Every change is written to the node log as a single `INFO` line with the logger, the old and
the new level, the scope and the address of the caller.

#### Examples

Raise the JDBC connector to `DEBUG` on one node:
`curl -X POST 'http://localhost:8080/loggers/org.apache.seatunnel.connectors.seatunnel.jdbc?level=DEBUG'`

Raise it on every member of the cluster:
`curl -X POST 'http://localhost:8080/loggers/org.apache.seatunnel.connectors.seatunnel.jdbc?level=DEBUG&scope=cluster'`

</details>

<details>
 <summary><code>DELETE</code> <code><b>/loggers/:name</b></code> <code>(Reverts a runtime override.)</code></summary>

#### Query Parameters

> |  Parameter Name  |   Required   |  Type   |                                  Description                                   |
> |------------------|--------------|---------|--------------------------------------------------------------------------------|
> | scope            |   optional   | string  | `node` (default) reverts the node that serves the request, `cluster` reverts every member |

#### Response

The logger goes back to the level it had before its first override, which is the level of the
configuration file, or the level inherited from its parent when the file does not configure it.

```json
{
  "name": "org.apache.seatunnel.connectors.seatunnel.jdbc",
  "level": "INFO",
  "origin": "file",
  "node": "localhost:8080",
  "previousLevel": "DEBUG",
  "status": "SUCCESS"
}
```

`status` is `NO_OVERRIDE` when the logger was never overridden through an endpoint; nothing is
changed in that case.

</details>

### Get Node Metrics

<details>
 <summary>
    <code>GET</code> <code><b>/metrics</b></code>  
    <code>GET</code> <code><b>/openmetrics</b></code>
</summary>

To get the metrics, you need to open `Telemetry` first, or you will get an empty response.  

More information about `Telemetry` can be found in the [Telemetry](telemetry.md) documentation.

</details>

### Get HTTP Service Status

<details>
 <summary><code>GET</code> <code><b>/http-service/status</b></code> <code>(Return HTTP service runtime status.)</code></summary>

#### Response

Returns the HTTP service switches, configured ports, effective connector ports, context path, and authentication mode for the current node.
Sensitive values such as passwords and keystore or truststore paths are not returned.

#### Response Example

```json
{
  "httpEnabled": true,
  "httpsEnabled": false,
  "contextPath": "/",
  "configuredHttpPort": 5801,
  "configuredHttpsPort": 58443,
  "httpPort": 5801,
  "httpsPort": 58443,
  "dynamicPortEnabled": false,
  "portRange": 100,
  "basicAuthEnabled": false,
  "mutualTlsEnabled": false
}
```

</details>

### Get Job Checkpoint Overview

<details>
 <summary><code>GET</code> <code><b>/jobs/checkpoints/:jobId</b></code> <code>(Return checkpoint overview of every pipeline).</code></summary>

#### Path Parameter

- `jobId`: required job identifier.

#### Response Example

```json
{
  "jobId": "1234567890",
  "updatedAt": 1720000000123,
  "pipelines": [
    {
      "pipelineId": 1,
      "counts": {
        "triggered": 10,
        "completed": 8,
        "failed": 1,
        "inProgress": 1,
        "restored": 2
      },
      "latestCompleted": {
        "checkpointId": 9,
        "checkpointType": "CHECKPOINT_TYPE",
        "status": "COMPLETED",
        "triggerTimestamp": 1720000000000,
        "completedTimestamp": 1720000000450,
        "durationMillis": 450,
        "stateSize": 128934
      },
      "latestFailed": {
        "checkpointId": 8,
        "checkpointType": "CHECKPOINT_TYPE",
        "status": "FAILED",
        "triggerTimestamp": 1719999995000,
        "failureReason": "CHECKPOINT_EXPIRED"
      },
      "latestSavepoint": null,
      "inProgress": [
        {
          "checkpointId": 10,
          "checkpointType": "CHECKPOINT_TYPE",
          "triggerTimestamp": 1720000005000,
          "acknowledged": 2,
          "total": 4
        }
      ],
      "history": [
        {
          "pipelineId": 1,
          "checkpoint": {
            "checkpointId": 9,
            "checkpointType": "CHECKPOINT_TYPE",
            "status": "COMPLETED",
            "triggerTimestamp": 1720000000000,
            "completedTimestamp": 1720000000450,
            "durationMillis": 450,
            "stateSize": 128934
          }
        }
      ]
    }
  ]
}
```
</details>

#### Field Description

| Field | Description |
| --- | --- |
| `jobId` | Job ID. |
| `updatedAt` | Latest snapshot timestamp (millisecond). |
| `pipelines` | List of pipeline statistics. |
| `pipelines[].pipelineId` | Pipeline ID. |
| `pipelines[].counts.triggered/completed/failed/inProgress/restored` | Checkpoint statistics:<br/>- `triggered`: total triggered checkpoints.<br/>- `completed`: total successful checkpoints.<br/>- `failed`: total failed checkpoints.<br/>- `inProgress`: checkpoints currently running.<br/>- `restored`: number of restore (including savepoint) attempts. |
| `pipelines[].latestCompleted/latestFailed/latestSavepoint` | Metadata of the latest completed/failed/savepoint checkpoints (see table below for field definitions). |
| `pipelines[].inProgress` | Ongoing checkpoints with details:<br/>- `checkpointId`: ID of the running checkpoint.<br/>- `checkpointType`: type (`CHECKPOINT_TYPE`, savepoint, etc.).<br/>- `triggerTimestamp`: when it was triggered (ms).<br/>- `acknowledged`: number of subtasks that have ACKed.<br/>- `total`: total subtasks requiring ACK. |
| `pipelines[].history` | Ring-buffer history (default 32 entries) ordered latest-first; each entry contains `pipelineId` plus checkpoint metadata. |

Checkpoint metadata fields:

| Field | Description |
| --- | --- |
| `checkpointId` | Checkpoint identifier. |
| `checkpointType` | Checkpoint type. |
| `status` | `COMPLETED`, `FAILED`, or `CANCELED`. |
| `triggerTimestamp` | Trigger time in milliseconds. |
| `completedTimestamp` | Completion time (only for success). |
| `durationMillis` | Duration in milliseconds. |
| `stateSize` | State size in bytes. |
| `failureReason` | Failure/cancel reason, optional. |

### Get Job Checkpoint History

<details>
 <summary><code>GET</code> <code><b>/jobs/checkpoints/history/:jobId</b></code> <code>(Return checkpoint history records.)</code></summary>

#### Query Parameters

| Name | Description |
| --- | --- |
| `jobId` | Required job ID (path). |
| `pipelineId` | Optional pipeline filter. |
| `limit` | Optional limit (default 20). |
| `status` | Optional status filter: `COMPLETED`, `FAILED`, `CANCELED`. |

#### Response Example

```json
[
  {
    "pipelineId": 1,
    "checkpoint": {
      "checkpointId": 9,
      "checkpointType": "CHECKPOINT_TYPE",
      "status": "COMPLETED",
      "triggerTimestamp": 1720000000000,
      "completedTimestamp": 1720000000450,
      "durationMillis": 450,
      "stateSize": 128934
    }
  },
  {
    "pipelineId": 1,
    "checkpoint": {
      "checkpointId": 8,
      "checkpointType": "CHECKPOINT_TYPE",
      "status": "FAILED",
      "triggerTimestamp": 1719999995000,
      "failureReason": "CHECKPOINT_EXPIRED"
    }
  }
]
```
</details>

#### Field Description

| Field | Description |
| --- | --- |
| `pipelineId` | ID of the pipeline to which the record belongs. |
| `checkpoint` | Checkpoint metadata described above. |

------------------------------------------------------------------------------------------

### Get Job Realtime Observability Metrics

These APIs are used by the Web UI realtime metrics view. They do not depend on Telemetry and do not write historical data to disk. The master only keeps recent in-memory buckets.

See [Realtime Observability](realtime-observability.md) for configuration and metric semantics.

<details>
 <summary><code>GET</code> <code><b>/metrics/realtime/jobs</b></code> <code>(List realtime metric state and window information for running jobs.)</code></summary>

#### Response

```json
{
  "jobs": [
    {
      "jobId": 12345,
      "enabled": true,
      "bucketMs": 5000,
      "retentionMinutes": 3,
      "latestBucketStartMs": 1700000000000
    }
  ]
}
```

</details>

<details>
 <summary><code>GET</code> <code><b>/metrics/realtime/jobs/{'{'}jobId{'}'}/vertices?windowMs=600000</b></code> <code>(Return Source/Transform/Sink vertex time series.)</code></summary>

#### Query Parameters

| Name | Required | Type | Description |
| --- | --- | --- | --- |
| `windowMs` | No | long | Query window in milliseconds. Defaults to 3 minutes and is capped at 10 minutes. |

#### Response Structure

```json
{
  "enabled": true,
  "bucketMs": 5000,
  "fromMs": 1700000000000,
  "toMs": 1700000600000,
  "vertices": [
    {
      "vertexId": 1,
      "points": [
        {
          "ts": 1700000550000,
          "sourceReadRatio": 0.12,
          "sourceIdleRatio": 0.45,
          "transformBusyRatio": 0.00,
          "sinkBusyRatio": 0.00
        }
      ]
    }
  ]
}
```

Ratio fields are in the range `0~1` and can be displayed as percentages. Fields that do not apply to a vertex type may be `0`.

</details>

<details>
 <summary><code>GET</code> <code><b>/metrics/realtime/jobs/{'{'}jobId{'}'}/edges?windowMs=600000</b></code> <code>(Return queue/edge downstream wait ratio and queue fill ratio time series.)</code></summary>

#### Query Parameters

| Name | Required | Type | Description |
| --- | --- | --- | --- |
| `windowMs` | No | long | Query window in milliseconds. Defaults to 3 minutes and is capped at 10 minutes. |

#### Response Structure

```json
{
  "enabled": true,
  "bucketMs": 5000,
  "fromMs": 1700000000000,
  "toMs": 1700000600000,
  "edges": [
    {
      "queueId": -101,
      "targetVertexId": 50,
      "points": [
        {
          "ts": 1700000550000,
          "bpRatio": 0.78,
          "queueFillRatio": 0.92,
          "queueSize": 46,
          "queueCapacity": 50
        }
      ]
    }
  ]
}
```

</details>

------------------------------------------------------------------------------------------

### Pause, Resume Or Delete A Job

There is no dedicated `pause`, `resume` or `delete` endpoint. Use the existing job endpoints as follows:

| Goal                                   | How                                                                                                                                                                                                        |
|-----------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Pause a running job (stop now, resume later) | Call [`/stop-job`](#stop-a-job) with `isStopWithSavePoint: true`. The job stops and a savepoint of its current state is persisted.                                                                       |
| Resume a paused job                     | Call [`/submit-job`](#submit-a-job) again with `isStartWithSavePoint: true`, the **same** `jobId` that was stopped, and the same job config. The job restores from its latest savepoint for that `jobId`. |
| Delete a job                            | There is no delete endpoint. Stop the job with [`/stop-job`](#stop-a-job) if it is still running. Once a job reaches a finished state, its record is removed automatically after `history-job-expire-minutes` (default 1440 minutes) elapses -- see [History Job Expiry Configuration](separated-cluster-deployment.md#44-history-job-expiry-configuration). |

**Note:** `isStartWithSavePoint: true` requires `jobId` to be provided in the request; submitting
without a `jobId` in that case fails with `Please provide jobId when start with save point.`
