---

sidebar_position: 11
--------------------

# RESTful API

SeaTunnel有一个用于监控的API，可用于查询运行作业的状态和统计信息，以及最近完成的作业。监控API是RESTful风格的，它接受HTTP请求并使用JSON数据格式进行响应。

## 概述

监控API是由运行的web服务提供的，它是节点运行的一部分，每个节点成员都可以提供rest API功能。
默认情况下，该服务监听端口为5801，该端口可以在hazelcast.yaml中配置，如下所示：

```yaml
network:
    rest-api:
      enabled: true
      endpoint-groups:
        CLUSTER_WRITE:
          enabled: true
        DATA:
          enabled: true
    join:
      tcp-ip:
        enabled: true
        member-list:
          - localhost
    port:
      auto-increment: true
      port-count: 100
      port: 5801
```

## API参考

### 返回Zeta集群的概览

<details>
 <summary><code>GET</code> <code><b>/hazelcast/rest/maps/overview?tag1=value1&tag2=value2</b></code> <code>(Returns an overview over the Zeta engine cluster.)</code></summary>

#### 参数

> |  参数名称  | 是否必传 | 参数类型 |           参数描述           |
> |--------|------|------|--------------------------|
> | tag键值对 | 否    | 字符串  | 一组标签值, 通过该标签值过滤满足条件的节点信息 |

#### 响应

```json
{
    "projectVersion":"2.3.5-SNAPSHOT",
    "gitCommitAbbrev":"DeadD0d0",
    "totalSlot":"0",
    "unassignedSlot":"0",
    "works":"1",
    "runningJobs":"0",
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

### 返回所有作业及其当前状态的概览。

<details>
 <summary><code>GET</code> <code><b>/hazelcast/rest/maps/running-jobs</b></code> <code>(返回所有作业及其当前状态的概览。)</code></summary>

#### 参数

#### 响应

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
      "vertices": [
      ],
      "edges": [
      ]
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

### 返回作业的详细信息。

<details>
 <summary><code>GET</code> <code><b>/hazelcast/rest/maps/job-info/:jobId</b></code> <code>(返回作业的详细信息。)</code></summary>

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
    "vertices": [
    ],
    "edges": [
    ]
  },
  "metrics": {
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

### 返回作业的详细信息

此API已经弃用，请使用/hazelcast/rest/maps/job-info/:jobId替代。

<details>
 <summary><code>GET</code> <code><b>/hazelcast/rest/maps/running-job/:jobId</b></code> <code>(返回作业的详细信息。)</code></summary>

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
    "vertices": [
    ],
    "edges": [
    ]
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

### 返回所有已完成的作业信息。

<details>
 <summary><code>GET</code> <code><b>/hazelcast/rest/maps/finished-jobs/:state</b></code> <code>(返回所有已完成的作业信息。)</code></summary>

#### 参数

> | 参数名称  |   是否必传   |  参数类型  |                               参数描述                               |
> |-------|----------|--------|------------------------------------------------------------------|
> | state | optional | string | finished job status. `FINISHED`,`CANCELED`,`FAILED`,`UNKNOWABLE` |

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
    "jobDag": "",
    "metrics": ""
  }
]
```

</details>

------------------------------------------------------------------------------------------

### 返回系统监控信息。

<details>
 <summary><code>GET</code> <code><b>/hazelcast/rest/maps/system-monitoring-information</b></code> <code>(返回系统监控信息。)</code></summary>

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

### 提交作业。

<details>
<summary><code>POST</code> <code><b>/hazelcast/rest/maps/submit-job</b></code> <code>(如果作业提交成功，返回jobId和jobName。)</code></summary>

#### 参数

> |         参数名称         |   是否必传   |  参数类型  |               参数描述                |
> |----------------------|----------|--------|-----------------------------------|
> | jobId                | optional | string | job id                            |
> | jobName              | optional | string | job name                          |
> | isStartWithSavePoint | optional | string | if job is started with save point |

#### 请求体

```json
{
    "env": {
        "job.mode": "batch"
    },
    "source": [
        {
            "plugin_name": "FakeSource",
            "result_table_name": "fake",
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
            "source_table_name": ["fake"]
        }
    ]
}
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

### 停止作业。

<details>
<summary><code>POST</code> <code><b>/hazelcast/rest/maps/stop-job</b></code> <code>(如果作业成功停止，返回jobId。)</code></summary>

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

### 加密配置。

<details>
<summary><code>POST</code> <code><b>/hazelcast/rest/maps/encrypt-config</b></code> <code>(如果配置加密成功，则返回加密后的配置。)</code></summary>
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
            "result_table_name": "fake",
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
            "result_table_name": "fake",
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

