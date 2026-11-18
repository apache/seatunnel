---
sidebar_position: 2
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

# Hybrid Cluster Mode

In hybrid mode, each Zeta member can participate in Master election and execute tasks in the same JVM. A small shared cluster can start with a single member. Coordination and task execution share the container's CPU and memory. To size and scale the two roles independently, use [Separated Cluster Mode](separated-cluster-mode.md).

## Configure the hybrid member and client

Start with the [YARN prerequisites](yarn.mdx). The example uses service `zeta-hybrid`, component `hybrid`, submitting user `etl` and Registry DNS domain `example.net`. Replace the example hostname in the member and client configuration with the address for your cluster.

In the distribution under `$SEATUNNEL_HOME`, merge the following values into `config/hazelcast.yaml`. Keep the packaged member port `5801`.

```yaml
hazelcast:
  cluster-name: zeta-hybrid
  network:
    join:
      tcp-ip:
        enabled: true
        member-list:
          - hybrid-0.zeta-hybrid.etl.example.net:5801
```

On the submission host, merge the following keys into `config/hazelcast-client.yaml` so the CLI can discover the hybrid member.

```yaml
hazelcast-client:
  cluster-name: zeta-hybrid
  network:
    cluster-members:
      - hybrid-0.zeta-hybrid.etl.example.net:5801
```

In `config/seatunnel.yaml`, set `seatunnel.engine.backup-count` to `0` for the single member and keep HTTP enabled on port `8080`. Follow [YARN Configuration](configuration.md) to configure HDFS checkpoint storage, using your HDFS URI and the namespace `/user/etl/seatunnel/checkpoints/zeta-hybrid`. In `config/jvm_options`, set the heap below the 4 GiB container limit, for example `-Xmx2g`.

## Stage and launch the service

Package the distribution with the configured files and the connectors used by your jobs. The archive root must contain `bin/` and `config/`.

```bash
tar -czf /tmp/zeta-hybrid-3.1.0.tgz -C "$SEATUNNEL_HOME" .
hdfs dfs -mkdir -p /user/etl/seatunnel/releases /user/etl/seatunnel/checkpoints/zeta-hybrid
hdfs dfs -put /tmp/zeta-hybrid-3.1.0.tgz /user/etl/seatunnel/releases/
```

Save the following as `zeta-hybrid.json`. The queue needs capacity for the YARN Service ApplicationMaster and one hybrid container. The launch command runs the default `master_and_worker` role in the foreground.

```json
{
  "name": "zeta-hybrid",
  "version": "1",
  "queue": "default",
  "configuration": {
    "files": [
      {
        "type": "STATIC",
        "src_file": "/user/etl/seatunnel/releases/zeta-hybrid-3.1.0.tgz",
        "dest_file": "zeta.tgz"
      }
    ]
  },
  "components": [
    {
      "name": "hybrid",
      "number_of_containers": 1,
      "resource": { "cpus": 2, "memory": "4096" },
      "launch_command": "mkdir -p seatunnel && tar -xzf zeta.tgz -C seatunnel && exec bash seatunnel/bin/seatunnel-cluster.sh"
    }
  ]
}
```

```bash
yarn app -launch zeta-hybrid ./zeta-hybrid.json
yarn app -status zeta-hybrid
```

## Submit a job

Check the Zeta member has joined and reports one Worker.

```bash
curl -fsS http://hybrid-0.zeta-hybrid.etl.example.net:8080/overview
```

Run the bundled batch example from the submission host. After the job finishes, the same YARN service can accept the next job.

```bash
cd "$SEATUNNEL_HOME"
bin/seatunnel.sh -m cluster -c config/v2.batch.config.template
```

## Run more than one member

Give each member a stable address and add the member addresses to both Hazelcast discovery lists. For example, a second instance of this component is `hybrid-1.zeta-hybrid.etl.example.net:5801`. When using host networking, place members on different NodeManager hosts so their fixed ports remain available. With two members on separate hosts, set `backup-count: 1` for one IMap backup. [Hybrid cluster deployment](../../engines/zeta/hybrid-cluster-deployment.md)
