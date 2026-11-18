---
sidebar_position: 3
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

# Separated Cluster Mode

This walkthrough launches the `zeta-session` service with one Master and one Worker. Use `yarn app` to manage the shared service and `bin/seatunnel.sh` to submit individual jobs.

## Prepare the distribution

Complete the [YARN prerequisites](yarn.mdx) and [configure the Master, Worker and client](configuration.md) before packaging the distribution. The client uses its local copy of `config/hazelcast-client.yaml`. Install the same job connectors and drivers in the distribution that you deploy to YARN.

## Stage and launch the service

Package the **contents** of the prepared distribution so `bin/` and `config/` are at the archive root. Use a new archive name when changing the distribution or configuration, and update `src_file` in the service definition.

```bash
tar -czf /tmp/zeta-session-3.1.0.tgz -C "$SEATUNNEL_HOME" .
hdfs dfs -mkdir -p /user/etl/seatunnel/releases /user/etl/seatunnel/checkpoints
hdfs dfs -put /tmp/zeta-session-3.1.0.tgz /user/etl/seatunnel/releases/
```

Save the following as `zeta-session.json`. The queue must have capacity for the service ApplicationMaster in addition to the two Zeta containers. YARN localizes the archive into each container. Each role extracts its own writable installation for logs and runtime files. The Worker starts after the Master's member port is reachable. Both launch commands keep the JVM in the foreground so YARN can track it.

```json
{
  "name": "zeta-session",
  "version": "1",
  "queue": "default",
  "configuration": {
    "files": [
      {
        "type": "STATIC",
        "src_file": "/user/etl/seatunnel/releases/zeta-session-3.1.0.tgz",
        "dest_file": "zeta.tgz"
      }
    ]
  },
  "components": [
    {
      "name": "master",
      "number_of_containers": 1,
      "resource": { "cpus": 1, "memory": "2048" },
      "readiness_check": { "type": "PORT", "properties": { "port": "5801" } },
      "launch_command": "mkdir -p seatunnel && tar -xzf zeta.tgz -C seatunnel && exec bash seatunnel/bin/seatunnel-cluster.sh -r master"
    },
    {
      "name": "worker",
      "number_of_containers": 1,
      "dependencies": ["master"],
      "resource": { "cpus": 2, "memory": "4096" },
      "configuration": { "env": { "SEATUNNEL_CONFIG": "seatunnel/config/seatunnel-worker.yaml" } },
      "launch_command": "mkdir -p seatunnel && tar -xzf zeta.tgz -C seatunnel && exec bash seatunnel/bin/seatunnel-cluster.sh -r worker"
    }
  ]
}
```

```bash
yarn app -launch zeta-session ./zeta-session.json
yarn app -status zeta-session
```

## Submit a job and check the cluster

Check that the Zeta Master reports `workers` as `1` before submitting the batch example.

```bash
curl -fsS http://master-0.zeta-session.etl.example.net:8080/overview
```

```bash
cd "$SEATUNNEL_HOME"
bin/seatunnel.sh -m cluster -c config/v2.batch.config.template
```

The CLI reports the Zeta job ID and final status. After the job reaches `FINISHED`, run `yarn app -status zeta-session` again. The same YARN application ID should still be active, and you can submit another job to the same Master.

For status checks, logs and restarts, see [YARN Operations](operations.md).
