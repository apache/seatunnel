# RESTful API V2

SeaTunnel has a monitoring API that can be used to query status and statistics of running jobs, as well as recent
completed jobs. The monitoring API is a RESTful API that accepts HTTP requests and responds with JSON data.

## Overview

The v2 version of the api uses jetty support. It is the same as the interface specification of v1 version
, you can specify the port and context-path by modifying the configuration items in `seatunnel.yaml`,
you can configure `enable-dynamic-port` to enable dynamic ports (the default port is accumulated starting from `port`), and the default is enabled,
If enable-dynamic-port is true, We will use the unused port in the range within the range of `port` and `port` + `port-range`, default range is 100

```yaml

seatunnel:
  engine:
    http:
      enable-http: true
      port: 8080
      enable-dynamic-port: true
      port-range: 100
```

Context-path can also be configured as follows:

```yaml

seatunnel:
  engine:
    http:
      enable-http: true
      port: 8080
      context-path: /seatunnel
```

## Enable HTTPS

Please refer [security](security.md)

## API reference

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
        "dynamicSlot": false,
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
  "isStartWithSavePoint": false
}
```

`jobId`, `jobName`, `jobStatus`, `createTime`, `jobDag`, `metrics` always be returned.
`envOptions`, `pluginJarsUrls`, `isStartWithSavePoint` will return when job is running.
`finishedTime`, `errorMsg` will return when job is finished.

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
> | format               | optional | string    | config format, support json, hocon and sql, default json |

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
<summary><code>POST</code> <code><b>/stop-job</b></code> <code>(Returns jobId if job stoped successfully.)</code></summary>

#### Body

```json
{
    "jobId": 733584788375666689,
    "isStopWithSavePoint": false # if job is stopped with save point
}
```

#### Responses

```json
{
"jobId": 733584788375666689
}
```

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
    "isStopWithSavePoint": false
  },
  {
    "jobId": 881432456517910529,
    "isStopWithSavePoint": false
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
For more information about customize encryption, please refer to the documentation [config-encryption-decryption](../connector-v2/Config-Encryption-Decryption.md).

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

<details><summary><code>POST</code><code><b>/update-tags</b></code><code>Because the update can only target a specific node, the current node's `ip:port` needs to be used for the update</code><code>(If the update is successful, return a success message)</code></summary>


#### update node tags
##### Body
If the request parameter is a `Map` object, it indicates that the tags of the current node need to be updated
```json
{
  "tag1": "dev_1",
  "tag2": "dev_2"
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
If the parameter is an empty `Map` object, it means that the tags of the current node need to be cleared
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


### Get Node Metrics

<details>
 <summary>
    <code>GET</code> <code><b>/metrics</b></code>  
    <code>GET</code> <code><b>/openmetrics</b></code>
</summary>

To get the metrics, you need to open `Telemetry` first, or you will get an empty response.  

More information about `Telemetry` can be found in the [Telemetry](telemetry.md) documentation.

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
