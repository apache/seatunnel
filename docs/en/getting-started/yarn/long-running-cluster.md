---
sidebar_position: 1
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

# Long-running Zeta cluster on YARN

Deploy a shared Zeta cluster as one Hadoop YARN Service. The service's ApplicationMaster requests containers for the Zeta Master and Worker. Submit jobs to the same Zeta Master. When a job finishes, the containers keep running for the next job.

## Prepare the cluster

- The example uses Hadoop 3.3.6 with HDFS and the YARN Service API enabled (`yarn.webapp.api-service.enable=true`). The submitting user needs access to the target queue. On a Kerberos cluster, configure the service definition’s top-level `kerberos_principal` with the approved principal and keytab, and grant the service access to HDFS. Distribute credentials separately from the archive. [Hadoop YARN Service Quick Start](https://hadoop.apache.org/docs/r3.3.6/hadoop-yarn/hadoop-yarn-site/yarn-service/QuickStart.html)
- The example uses service `zeta-session`, submitting user `etl`, and Registry DNS domain `example.net`. With YARN Registry DNS, the first Master is reachable as `master-0.zeta-session.etl.example.net`. Substitute your service name, user and domain throughout. Containers and clients must resolve the Master and reach the advertised member addresses. A stable hostname also works if your placement policy pins the Master to that host. [YARN service discovery](https://hadoop.apache.org/docs/r3.3.6/hadoop-yarn/hadoop-yarn-site/yarn-service/ServiceDiscovery.html)
- Prepare a clean SeaTunnel 3.1.0 binary distribution on the submission host and set `SEATUNNEL_HOME` to its directory. Include the connectors and drivers for your jobs. The sample job uses FakeSource and Console. Make Java 11 or 17 available on the client and on the `PATH` of both container roles.

## Configure Master, Worker and client

From `$SEATUNNEL_HOME`, edit the packaged configuration files. The example runs one Master and one Worker as foreground processes. They use different ports so they can also share a NodeManager host.

| Role | Hazelcast member port | SeaTunnel HTTP port |
| --- | --- | --- |
| Master | 5801 | 8080 |
| Worker | 5802 | 8081 |

In both `config/hazelcast-master.yaml` and `config/hazelcast-worker.yaml`, set `hazelcast.cluster-name` to `zeta-session`. Replace the `join` block under `hazelcast.network` with the following. Keep each role's packaged member port.

```yaml
join:
  tcp-ip:
    enabled: true
    member-list:
      - master-0.zeta-session.etl.example.net:5801
```

On the submission host, set the corresponding keys in `config/hazelcast-client.yaml` so the CLI connects to the Master.

```yaml
hazelcast-client:
  cluster-name: zeta-session
  network:
    cluster-members:
      - master-0.zeta-session.etl.example.net:5801
```

Apply these values to `config/seatunnel.yaml`, keeping the other packaged settings. Replace the HDFS URI with your cluster's `fs.defaultFS` and grant the service user write access to the checkpoint directory. The single Master uses zero backup replicas.

```yaml
seatunnel:
  engine:
    backup-count: 0
    checkpoint:
      storage:
        type: hdfs
        plugin-config:
          storage.type: hdfs
          fs.defaultFS: hdfs://namenode.example.net:8020
          namespace: /user/etl/seatunnel/checkpoints
    http:
      enable-http: true
      port: 8080
```

```bash
cp config/seatunnel.yaml config/seatunnel-worker.yaml
```

In the copied `config/seatunnel-worker.yaml`, change `seatunnel.engine.http.port` to `8081`. In `config/jvm_master_options` and `config/jvm_worker_options`, size the heap below the YARN container limit to leave room for native memory. For the limits below, a starting point is `-Xmx1g` for the Master and `-Xmx2g` for the Worker.

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

## Operate the service

Drain or cancel active jobs before planned downtime. Stop the service to release its containers, then start it again when needed.

```bash
yarn app -stop zeta-session
yarn app -start zeta-session
```

With YARN log aggregation enabled, use `yarn logs -applicationId <application-id>` to read container output. To include engine messages, enable the console appender in `config/log4j2.properties` before packaging the distribution.

For more Workers, ensure instances that use the same fixed ports get separate hosts or routable IPs. For Master failover, place at least two Masters on different hosts, list their discovery addresses, and configure the backup count accordingly. To recover running jobs after the entire cluster stops, configure durable IMap storage for job metadata as well as shared checkpoint storage. [Separated cluster deployment](../../engines/zeta/separated-cluster-deployment.md) · [State storage and recovery](../../engines/zeta/state-storage-and-recovery.md)
