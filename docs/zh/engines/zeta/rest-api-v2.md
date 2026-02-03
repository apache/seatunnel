# RESTful API V2

SeaTunnel有一个用于监控的API，可用于查询运行作业的状态和统计信息，以及最近完成的作业。监控API是RESTful风格的，它接受HTTP请求并使用JSON数据格式进行响应。

## 概述

v2版本的api使用jetty支持，与v1版本的接口规范相同 ,可以通过修改`seatunnel.yaml`中的配置项来指定端口和context-path，
同时可以配置 `enable-dynamic-port` 开启动态端口(默认从 `port` 开始累加)，默认为开启，
如果`enable-dynamic-port`为`true`，我们将使用`port`和`port`+`port-range`范围内未使用的端口，默认范围是100。

```yaml

seatunnel:
  engine:
    http:
      enable-http: true
      port: 8080
      enable-dynamic-port: true
      port-range: 100
```

同时也可以配置context-path,配置如下：

```yaml

seatunnel:
  engine:
    http:
      enable-http: true
      port: 8080
      context-path: /seatunnel
```

## 开启 HTTPS

请参考 [security](security.md)

## API参考

### 返回Zeta集群的概览

<details>
 <summary><code>GET</code> <code><b>/overview?tag1=value1&tag2=value2</b></code> <code>(Returns an overview over the Zeta engine cluster.)</code></summary>

#### 参数

> |  参数名称  | 是否必传 | 参数类型 |           参数描述           |
> |--------|------|------|--------------------------|
> | tag键值对 | 否    | 字符串  | 一组标签值, 通过该标签值过滤满足条件的节点信息 |

#### 响应

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

**注意:**
- 当你使用`dynamic-slot`时, 返回结果中的`totalSlot`和`unassignedSlot`将始终为0. 设置为固定的slot值后, 将正确返回集群中总共的slot数量以及未分配的slot数量.
- 当添加标签过滤后, `works`, `totalSlot`, `unassignedSlot`将返回满足条件的节点的相关指标. 注意`runningJobs`等job相关指标为集群级别结果, 无法根据标签进行过滤.

</details>

------------------------------------------------------------------------------------------

### 查询作业及其当前状态的概览

<details>
 <summary><code>GET</code> <code><b>/running-jobs?page=1&rows=10</b></code> <code>(查询作业及其当前状态的概览。)</code></summary>

#### 参数

> | 参数名称 | 是否必传 | 参数类型 | 参数描述 |
> |------|------|------|------|
> | page | 否    | int  | 页号   |
> | rows | 否    | int  | 每页行数 |

#### 响应

```json
[
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

### 查看 Pending 队列详细信息

<details>
 <summary><code>GET</code> <code><b>/pending-jobs?jobId=123&limit=10</b></code> <code>(用于排查作业长时间处于 PENDING 的原因。)</code></summary>

#### 参数

> | 参数名称 | 是否必传 | 参数类型 | 描述                             |
> |----------|----------|----------|--------------------------------|
> | jobId    | 可选     | long     | 只查看指定作业的诊断信息。当同时提供 `jobId` 和 `limit` 时，`jobId` 优先生效，`limit` 将被忽略。 |
> | limit    | 可选     | integer  | 限制返回的PENDING作业数量。当提供 `jobId` 参数时此参数将被忽略。 |
> | pretty   | 可选     | boolean  | 传入 `true` 时返回格式化 JSON，并格式化时间戳。   |

#### 响应

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

当 `pretty=true` 时，接口会返回格式化后的 JSON，并把 `oldestEnqueueTimestamp`、`newestEnqueueTimestamp`、`enqueueTimestamp`、`checkTime` 转为 `yyyy-MM-dd HH:mm:ss` 字符串，方便排查。

响应中包含：

- **queueSummary**：Pending 队列整体信息总结
  - `size`：当前排队的 Job 数量。
  - `scheduleStrategy`：调度策略，决定资源不足时的处理方式。
  - `oldestEnqueueTimestamp` / `newestEnqueueTimestamp`：最久/最新进入 Pending 队列 Job 的时间戳（毫秒）。
  - `lackingTaskGroups`：尚未分配 Slot 的 TaskGroup 数量。**注意**：该值仅统计当前响应中返回的作业子集（即受 `limit` 参数限制或 `jobId` 过滤后的作业），而非整个 Pending 队列的完整统计。如需查看所有 Pending 作业的完整统计信息，请不带 `limit` 参数调用此接口。
- **clusterSnapshot**：当前集群的资源视图。
  - `totalSlots` / `assignedSlots` / `freeSlots`：Slot 总数、已分配数、剩余数。
  - `workerCount`：Worker 数量。
  - `workers[]`：
    - `address`：Worker 地址（host:port）。
    - `tags`：Worker 自带的标签。
    - `totalSlots` / `freeSlots`：Worker 的 Slot 总数与剩余数。
    - `dynamicSlot`：是否启用动态 Slot。
    - `cpuUsage` / `memUsage`：系统负载采样（只有当 `slot-allocate-strategy: SYSTEM_LOAD` 才会有该值）
    - `runningJobIds[]`：当前占用 Worker Slot 的 JobId 列表。
- **pendingJobs[]**：队列中的每个 Job 的诊断信息。
  - `jobId` / `jobName`：作业标识。
  - `pendingSourceState`：取值：`SUBMIT`,`RESTORE`。
  - `jobStatus`：物理计划记录的状态（固定为 `PENDING`）。
  - `enqueueTimestamp`：进入 Pending 队列的时间。
  - `checkTime`：最近一次Pending检查时间。
  - `waitDurationMs`：等待时长（`checkTime - enqueueTimestamp`）。
  - `checkCount`：已被调度线程检查的次数。
  - `totalTaskGroups` / `allocatedTaskGroups` / `lackingTaskGroups`：Job 全部 TaskGroup 数量、已分配 Slot 的数量、缺少 Slot 的数量。
  - `failureReason` / `failureMessage`：导致本次资源申请失败的归类及具体信息（如 `RESOURCE_NOT_ENOUGH`、`REQUEST_FAILED` 等）。
  - `tagFilter`：Job 要求的 Worker 标签（若配置）。
  - `blockingJobIds[]`：当前占用 Slot 的其他 JobId，用来分析资源竞争。
  - `pipelines[]`：按 Pipeline 细分：
    - `pipelineId` / `pipelineName`：
    - `totalTaskGroups` / `allocatedTaskGroups` / `lackingTaskGroups`：Pipeline 里 TaskGroup 的总数、已分配 Slot 数量、缺少 Slot 的数量。
    - `taskGroupDiagnostics[]`：每个 TaskGroup 的 Slot 请求状态：
      - `taskGroupLocation`（`jobId`, `pipelineId`, `taskGroupId`）。
      - `taskFullName`：方便直接定位 source/sink。
      - `allocated`：是否已经成功申请 Slot。
      - `failureReason` / `failureMessage`：TaskGroup 层面的失败原因。
  - `lackingTaskGroupDiagnostics[]`：聚合所有 `allocated=false` 的 TaskGroup，方便快速查看缺 Slot 的具体任务。

</details>

------------------------------------------------------------------------------------------

### 返回作业的详细信息

<details>
 <summary><code>GET</code> <code><b>/job-info/:jobId</b></code> <code>(返回作业的详细信息。)</code></summary>

#### 参数

> | 参数名称  | 是否必传 | 参数类型 |  参数描述  |
> |-------|------|------|--------|
> | jobId | 是    | long | job id |

#### 响应

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

`jobId`, `jobName`, `jobStatus`, `createTime`, `jobDag`, `metrics` 字段总会返回.
`envOptions`, `pluginJarsUrls`, `isStartWithSavePoint` 字段在Job在RUNNING状态时会返回
`finishedTime`, `errorMsg` 字段在Job结束时会返回，结束状态为不为RUNNING，可能为FINISHED，可能为CANCEL

#### 指标字段说明

| 字段 | 说明 |
| --- | --- |
| IntermediateQueueSize | 中间队列的大小 |
| SourceReceivedCount | 源端接收的行数 |
| SourceReceivedQPS | 源端接收速率（行/秒） |
| SourceReceivedBytes | 源端接收的字节数 |
| SourceReceivedBytesPerSeconds | 源端接收速率（字节/秒） |
| SinkWriteCount | Sink 写入尝试行数 |
| SinkWriteQPS | Sink 写入尝试速率（行/秒） |
| SinkWriteBytes | Sink 写入尝试字节数 |
| SinkWriteBytesPerSeconds | Sink 写入尝试速率（字节/秒） |
| SinkCommittedCount | checkpoint 成功后的 Sink 已提交行数 |
| SinkCommittedQPS | Sink 已提交速率（行/秒） |
| SinkCommittedBytes | checkpoint 成功后的 Sink 已提交字节数 |
| SinkCommittedBytesPerSeconds | Sink 已提交速率（字节/秒） |
| TableSourceReceived* | 按表汇总的源指标，键格式 `TableSourceReceivedXXX#<表>` |
| TableSinkWrite* | 按表汇总的 Sink 写入尝试，键格式 `TableSinkWriteXXX#<表>` |
| TableSinkCommitted* | 按表汇总的 Sink 已提交指标，键格式 `TableSinkCommittedXXX#<表>` |

当我们查询不到这个Job时，返回结果为：

```json
{
  "jobId" : ""
}
```

</details>

------------------------------------------------------------------------------------------

### 返回作业的详细信息

此API已经弃用，请使用/job-info/:jobId替代。

<details>
 <summary><code>GET</code> <code><b>/running-job/:jobId</b></code> <code>(返回作业的详细信息。)</code></summary>

#### 参数

> | 参数名称  | 是否必传 | 参数类型 |  参数描述  |
> |-------|------|------|--------|
> | jobId | 是    | long | job id |

#### 响应

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
    "sourceReceivedCount": "",
    "sinkWriteCount": ""
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

`jobId`, `jobName`, `jobStatus`, `createTime`, `jobDag`, `metrics` 字段总会返回.
`envOptions`, `pluginJarsUrls`, `isStartWithSavePoint` 字段在Job在RUNNING状态时会返回
`finishedTime`, `errorMsg` 字段在Job结束时会返回，结束状态为不为RUNNING，可能为FINISHED，可能为CANCEL

当我们查询不到这个Job时，返回结果为：

```json
{
  "jobId" : ""
}
```

</details>

------------------------------------------------------------------------------------------

### 查询已完成的作业信息

<details>
 <summary><code>GET</code> <code><b>/finished-jobs/:state?page=1&rows=10</b></code> <code>(查询已完成的作业信息。)</code></summary>

#### 参数

> | 参数名称  |   是否必传   |  参数类型  | 参数描述                                                                              |
> |-------|----------|--------|-----------------------------------------------------------------------------------|
> | state | optional | string | finished job status. `FINISHED`,`CANCELED`,`FAILED`,`SAVEPOINT_DONE`,`UNKNOWABLE` |
> | page | 否    | int  | 页号   |
> | rows | 否    | int  | 每页行数 |

#### 响应

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

### 返回系统监控信息

<details>
 <summary><code>GET</code> <code><b>/system-monitoring-information</b></code> <code>(返回系统监控信息。)</code></summary>

#### 参数

#### 响应

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

### 提交作业

<details>
<summary><code>POST</code> <code><b>/submit-job</b></code> <code>(如果作业提交成功，返回jobId和jobName。)</code></summary>

#### 参数

> |         参数名称         |   是否必传   |  参数类型  | 参数描述                              |
> |----------------------|----------|-----------------------------------|-----------------------------------|
> | jobId                | optional | string | job id                            |
> | jobName              | optional | string | job name                          |
> | isStartWithSavePoint | optional | string | if job is started with save point |
> | format               | optional | string    | 配置风格,支持json、hocon 和 sql,默认 json   |

#### 请求体

你可以选择用json、hocon或者sql的方式来传递请求体。
Json请求示例：
```json
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

Hocon请求示例：
```hocon
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

SQL请求示例：

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
#### 响应

```json
{
    "jobId": 733584788375666689,
    "jobName": "rest_api_test"
}
```

</details>

------------------------------------------------------------------------------------------
### 提交作业来源上传配置文件

<details>
<summary><code>POST</code> <code><b>/submit-job</b></code> <code>(如果作业提交成功，返回jobId和jobName。)</code></summary>

#### 参数

> |         参数名称         |   是否必传   |  参数类型  | 参数描述                              |
> |----------------------|----------|-----------------------------------|-----------------------------------|
> | jobId                | optional | string | job id                            |
> | jobName              | optional | string | job name                          |
> | isStartWithSavePoint | optional | string | if job is started with save point |

#### 请求体
上传文件key的名称是config_file，支持以下格式：
- `.json` 文件：按照 JSON 格式解析
- `.conf` 或 `.config` 文件：按照 HOCON 格式解析
- `.sql` 文件：按照 SQL 格式解析，支持 CREATE TABLE 和 INSERT INTO 语法

curl Example

```bash
# 上传 HOCON 配置文件
curl --location 'http://127.0.0.1:8080/submit-job/upload' --form 'config_file=@"/temp/fake_to_console.conf"'

# 上传 SQL 配置文件
curl --location 'http://127.0.0.1:8080/submit-job/upload' --form 'config_file=@"/temp/job.sql"'
```
#### 响应

```json
{
    "jobId": 733584788375666689,
    "jobName": "SeaTunnel_Job"
}
```

</details>

------------------------------------------------------------------------------------------

### 批量提交作业

<details>
<summary><code>POST</code> <code><b>/submit-jobs</b></code> <code>(如果作业提交成功，返回jobId和jobName。)</code></summary>

#### 参数(在请求体中params字段中添加)

> |         参数名称         |   是否必传   |  参数类型  |               参数描述                |
> |----------------------|----------|--------|-----------------------------------|
> | jobId                | optional | string | job id                            |
> | jobName              | optional | string | job name                          |
> | isStartWithSavePoint | optional | string | if job is started with save point |



#### 请求体

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

#### 响应

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

### 停止作业

<details>
<summary><code>POST</code> <code><b>/stop-job</b></code> <code>(如果作业成功停止，返回jobId。)</code></summary>

#### 请求体

```json
{
    "jobId": 733584788375666689,
    "isStopWithSavePoint": false # if job is stopped with save point
}
```

#### 响应

```json
{
"jobId": 733584788375666689
}
```

</details>


------------------------------------------------------------------------------------------

### 批量停止作业

<details>
<summary><code>POST</code> <code><b>/stop-jobs</b></code> <code>(如果作业成功停止，返回jobId。)</code></summary>

#### 请求体

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

#### 响应

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

### 加密配置

<details>
<summary><code>POST</code> <code><b>/encrypt-config</b></code> <code>(如果配置加密成功，则返回加密后的配置。)</code></summary>
有关自定义加密的更多信息，请参阅文档[配置-加密-解密](../connector-v2/Config-Encryption-Decryption.md).

#### 请求体

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

#### 响应

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

### 更新运行节点的tags

<details>
<summary><code>POST</code><code><b>/update-tags</b></code><code>因为更新只能针对于某个节点，因此需要用当前节点ip:port用于更新</code><code>(如果更新成功，则返回"success"信息)</code></summary>


#### 更新节点tags
##### 请求体
如果请求参数是`Map`对象，表示要更新当前节点的tags
```json
{
  "tag1": "dev_1",
  "tag2": "dev_2"
}
```
##### 响应

```json
{
  "status": "success",
  "message": "update node tags done."
}
```
#### 移除节点tags
##### 请求体
如果参数为空`Map`对象，表示要清除当前节点的tags
```json
{}
```
##### 响应
响应体将为：
```json
{
  "status": "success",
  "message": "update node tags done."
}
```

#### 请求参数异常
- 如果请求参数为空

##### 响应

```json
{
    "status": "fail",
    "message": "Request body is empty."
}
```
- 如果参数不是`Map`对象
##### 响应

```json
{
  "status": "fail",
  "message": "Invalid JSON format in request body."
}
```
</details>


------------------------------------------------------------------------------------------

### 获取所有节点日志内容

<details>
 <summary><code>GET</code> <code><b>/logs/:jobId</b></code> <code>(返回日志列表。)</code></summary>

#### 请求参数

#### 参数(在请求体中params字段中添加)

> |         参数名称         |   是否必传   |  参数类型  |               参数描述                |
> |----------------------|----------|--------|-----------------------------------|
> | jobId                | optional | string | job id                            |

当`jobId`为空时，返回所有节点的日志信息，否则返回指定`jobId`在所有节点的的日志列表。

#### 响应

返回请求节点的日志列表、内容

#### 返回所有日志文件列表

如果你想先查看日志列表，可以通过`GET`请求获取日志列表，`http://localhost:8080/logs?format=json`

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

当前支持的格式有`json`和`html`，默认为`html`。


#### 例子

获取所有节点jobId为`733584788375666689`的日志信息：`http://localhost:8080/logs/733584788375666689`
获取所有节点日志列表：`http://localhost:8080/logs`
获取所有节点日志列表以JSON格式返回：`http://localhost:8080/logs?format=json`
获取日志文件内容：`http://localhost:8080/logs/job-898380162133917698.log`


</details>


### 获取单节点日志内容

<details>
 <summary><code>GET</code> <code><b>/log</b></code> <code>(返回日志列表。)</code></summary>

#### 响应

返回请求节点的日志列表

#### 例子

获取当前节点的日志列表：`http://localhost:5801/log`
获取日志文件内容：`http://localhost:5801/log/job-898380162133917698.log``

</details>

### 获取节点指标信息

<details>
 <summary>
    <code>GET</code> <code><b>/metrics</b></code>  
    <code>GET</code> <code><b>/openmetrics</b></code>
</summary>
你需要先打开`Telemetry`才能获取集群指标信息。否则将返回空信息。

更多关于`Telemetry`的信息可以在[Telemetry](telemetry.md)文档中找到。

</details>

### 获取作业 Checkpoint 概览

<details>
 <summary><code>GET</code> <code><b>/jobs/checkpoints/:jobId</b></code> <code>(返回指定作业下所有 Pipeline 的 Checkpoint 概览。)</code></summary>

#### 参数

路径参数 `jobId`：必填，作业 ID。

#### 响应示例

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

#### 字段说明

| 字段 | 描述 |
| --- | --- |
| `jobId` | 作业 ID。 |
| `updatedAt` | 概览最近刷新时间（毫秒时间戳）。 |
| `pipelines` | pipeline 统计列表。 |
| `pipelines[].pipelineId` | pipeline ID。 |
| `pipelines[].counts.triggered/completed/failed/inProgress/restored` | Checkpoint 统计：<br/>- `triggered`：自作业启动以来触发次数。<br/>- `completed`：成功完成次数。<br/>- `failed`：失败次数。<br/>- `inProgress`：当前正在执行的 checkpoint 数量。<br/>- `restored`：触发恢复（包括 savepoint 恢复）的次数。 |
| `pipelines[].latestCompleted/latestFailed/latestSavepoint` | 最近一次成功/失败/保存点 checkpoint 元信息（字段同“Checkpoint 信息字段”表）。 |
| `pipelines[].inProgress` | 进行中的 checkpoint 列表，如下所示：<br/>- `checkpointId`：当前执行中的 checkpoint 编号。<br/>- `checkpointType`：类型（普通 checkpoint、savepoint 等）。<br/>- `triggerTimestamp`：该 checkpoint 触发时间（毫秒）。<br/>- `acknowledged`：已完成 ACK 的 subtask 数。<br/>- `total`：该 pipeline 中需要 ACK 的 subtask 总数。 |
| `pipelines[].history` | 环形缓冲中的历史记录（默认保留 32 条），每条包含 `pipelineId` 和对应的 checkpoint 元信息，按触发时间倒序。 |

Checkpoint 信息字段：

| 字段 | 描述                                      |
| --- |-----------------------------------------|
| `checkpointId` | checkpoint 编号。                          |
| `checkpointType` | checkpoint 类型。                          |
| `status` | 状态：`COMPLETED` / `FAILED` / `CANCELED`。 |
| `triggerTimestamp` | 触发时间（毫秒）。                               |
| `completedTimestamp` | 完成时间（毫秒，成功时存在）。                         |
| `durationMillis` | 耗时（毫秒）。                                 |
| `stateSize` | 状态大小（字节）。                               |
| `failureReason` | 失败/取消原因，可能为空。                           |

### 获取作业 Checkpoint 历史

<details>
 <summary><code>GET</code> <code><b>/jobs/checkpoints/history/:jobId</b></code> <code>(返回作业的 Checkpoint 历史记录。)</code></summary>

#### 参数

| 参数 | 说明 |
| --- | --- |
| `jobId` | 必填，作业 ID。 |
| `pipelineId` | 可选，按 pipeline 过滤。 |
| `limit` | 可选，限制返回条数，默认 20。 |
| `status` | 可选，支持 `COMPLETED`、`FAILED`、`CANCELED`。 |

#### 响应示例

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
