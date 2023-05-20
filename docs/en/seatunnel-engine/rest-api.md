---

sidebar_position: 7
-------------------

# REST API

Seatunnel has a monitoring API that can be used to query status and statistics of running jobs, as well as recent
completed jobs. The monitoring API is a REST-ful API that accepts HTTP requests and responds with JSON data.

## Overview

The monitoring API is backed by a web server that runs as part of the node, each node member can provide rest api capability.
By default, this server listens at port 5801, which can be configured in hazelcast.yaml like :

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

## API Reference

### Returns an Overview Over All Jobs and Their Current State.

<details>
 <summary><code>GET</code> <code><b>/hazelcast/rest/maps/running-jobs</b></code> <code>(Returns an overview over all jobs and their current state.)</code></summary>

#### Parameters

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

### Return Details of a Job.

<details>
 <summary><code>GET</code> <code><b>/hazelcast/rest/maps/running-job/:jobId</b></code> <code>(Return details of a job.)</code></summary>

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
```

</details>

------------------------------------------------------------------------------------------

### Returns System Monitoring Information.

<details>
 <summary><code>GET</code> <code><b>/hazelcast/rest/maps/system-monitoring-information</b></code> <code>(Returns system monitoring information.)</code></summary>

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

### Returns All Completed Jobs Information.

<details>
 <summary><code>GET</code> <code><b>/hazelcast/rest/maps/completed-jobs-information</b></code> <code>(Returns all completed jobs information.)</code></summary>

#### Parameters

#### Responses

```json
[
  {
    "jobId": 712315273570484200,
    "jobName": "fake_to_file",
    "jobStatus": "FINISHED",
    "submitTime": "2023-05-20 22:46:36",
    "finishTime": "2023-05-20 22:46:38",
    "pipelineStateMapperMap": {
      "{\"jobId\":712315273570484225,\"pipelineId\":1}": {
        "pipelineStatus": "FINISHED",
        "executionStateMap": {
          "{\"jobId\":712315273570484225,\"pipelineId\":1,\"taskGroupId\":1}": "FINISHED",
          "{\"jobId\":712315273570484225,\"pipelineId\":1,\"taskGroupId\":50000}": "FINISHED",
          "{\"jobId\":712315273570484225,\"pipelineId\":1,\"taskGroupId\":3}": "FINISHED"
        }
      }
    },
    "jobMetrics": {
      "SinkWriteQPS": [
        {
          "tags": {
            "jobId": "712315273570484225",
            "taskGroupId": "50000",
            "address": "[localhost]:5801",
            "service": "TaskExecutionService",
            "taskGroupLocation": "TaskGroupLocation{jobId=712315273570484225, pipelineId=1, taskGroupId=50000}",
            "member": "58badf7f-dfc2-4f17-b0bd-79affb4f800e",
            "taskName": "TransformSeaTunnelTask",
            "taskID": "70000",
            "pipelineId": "1"
          },
          "metric": "SinkWriteQPS",
          "value": 5.512679162072767,
          "timestamp": 1684593998469
        }
      ],
      "SourceReceivedCount": [
        {
          "tags": {
            "jobId": "712315273570484225",
            "taskGroupId": "50000",
            "address": "[localhost]:5801",
            "service": "TaskExecutionService",
            "taskGroupLocation": "TaskGroupLocation{jobId=712315273570484225, pipelineId=1, taskGroupId=50000}",
            "member": "58badf7f-dfc2-4f17-b0bd-79affb4f800e",
            "taskName": "SourceSeaTunnelTask",
            "taskID": "60000",
            "pipelineId": "1"
          },
          "metric": "SourceReceivedCount",
          "value": 10,
          "timestamp": 1684593998469
        }
      ],
      "SourceReceivedQPS": [
        {
          "tags": {
            "jobId": "712315273570484225",
            "taskGroupId": "50000",
            "address": "[localhost]:5801",
            "service": "TaskExecutionService",
            "taskGroupLocation": "TaskGroupLocation{jobId=712315273570484225, pipelineId=1, taskGroupId=50000}",
            "member": "58badf7f-dfc2-4f17-b0bd-79affb4f800e",
            "taskName": "SourceSeaTunnelTask",
            "taskID": "60000",
            "pipelineId": "1"
          },
          "metric": "SourceReceivedQPS",
          "value": 5.63063063063063,
          "timestamp": 1684593998469
        }
      ],
      "SinkWriteCount": [
        {
          "tags": {
            "jobId": "712315273570484225",
            "taskGroupId": "50000",
            "address": "[localhost]:5801",
            "service": "TaskExecutionService",
            "taskGroupLocation": "TaskGroupLocation{jobId=712315273570484225, pipelineId=1, taskGroupId=50000}",
            "member": "58badf7f-dfc2-4f17-b0bd-79affb4f800e",
            "taskName": "TransformSeaTunnelTask",
            "taskID": "70000",
            "pipelineId": "1"
          },
          "metric": "SinkWriteCount",
          "value": 10,
          "timestamp": 1684593998469
        }
      ]
    }
  }
]
```

</details>

------------------------------------------------------------------------------------------

