---

sidebar_position: 9
-------------------

# REST API

Seatunnel has a monitoring API that can be used to query status and statistics of running jobs, as well as recent
completed jobs. The monitoring API is a REST-ful API that accepts HTTP requests and responds with JSON data.

## Overview

The monitoring API is backed by a web server that runs as part of the node, each node member can provide rest api capability.
By default, this server listens at port 8080, which can be configured in hazelcast.yaml like :

```yaml
rest-server-socket-endpoint-config:
      port:
        auto-increment: true
        port-count: 100
        port: 8080
```

## API reference

### Returns an overview over all jobs and their current state.

<details>
 <summary><code>GET</code> <code><b>/hazelcast/rest/maps/running-jobs</b></code> <code>(Returns an overview over all jobs and their current state.)</code></summary>

#### Parameters

```json
{}
```

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

### Return details of a job.

<details>
 <summary><code>GET</code> <code><b>/hazelcast/rest/maps/running-job/{jobId}</b></code> <code>(Return details of a job.)</code></summary>

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

