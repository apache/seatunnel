---
sidebar_position: 2
---

# Local Mode

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

Run a single SeaTunnel job in one YARN component container. The CLI starts an embedded Zeta engine and exits when the job finishes. Use this mode to check a connector or job configuration before deploying a [hybrid](hybrid-cluster-mode.md) or [separated](separated-cluster-mode.md) cluster.

## Prepare the job

Start with a clean SeaTunnel 3.1.0 binary distribution under `$SEATUNNEL_HOME`. This example runs the bundled `config/v2.batch.config.template`, a bounded FakeSource job that writes to Console. To run your own job, include its configuration, connectors and drivers in the distribution and update the `-c` path in the service definition.

From the submission host, package the distribution with `bin/` and `config/` at the archive root, then stage it in HDFS. Replace the example HDFS path in both the command and the service definition.

```bash
tar -czf /tmp/zeta-local-3.1.0.tgz -C "$SEATUNNEL_HOME" .
hdfs dfs -mkdir -p /user/etl/seatunnel/releases
hdfs dfs -put /tmp/zeta-local-3.1.0.tgz /user/etl/seatunnel/releases/
```

## Launch on YARN

Save the following as `zeta-local.json`. The queue must have capacity for the YARN Service ApplicationMaster and the single job container. The container runs `seatunnel.sh -m local` in the foreground. With `restart_policy` set to `NEVER`, YARN Service finishes when this component exits and releases the containers.

```json
{
  "name": "zeta-local",
  "version": "1",
  "queue": "default",
  "configuration": {
    "files": [
      {
        "type": "STATIC",
        "src_file": "/user/etl/seatunnel/releases/zeta-local-3.1.0.tgz",
        "dest_file": "zeta.tgz"
      }
    ]
  },
  "components": [
    {
      "name": "local",
      "number_of_containers": 1,
      "restart_policy": "NEVER",
      "resource": { "cpus": 1, "memory": "4096" },
      "launch_command": "mkdir -p seatunnel && tar -xzf zeta.tgz -C seatunnel && cd seatunnel && exec bash bin/seatunnel.sh -m local -c config/v2.batch.config.template"
    }
  ]
}
```

```bash
yarn app -launch zeta-local ./zeta-local.json
```

## Check the result

Set `APP_ID` to the YARN application ID printed by the launch command. Check the ResourceManager report for `State : FINISHED` and `Final-State : SUCCEEDED`.

```bash
yarn application -status "$APP_ID"
```

If the application fails, check its diagnostics. With log aggregation enabled, use the following command to inspect container output.

```bash
yarn logs -applicationId "$APP_ID"
```

For another independent run, use a new service name in both the JSON and the launch command.
