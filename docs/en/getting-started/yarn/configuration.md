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

# YARN Configuration

Prepare the SeaTunnel distribution on the submission host before uploading it to HDFS. Start in the directory where you unpacked the distribution.

```bash
cd "$SEATUNNEL_HOME"
```

## Member and client discovery

The example uses service `zeta-session`, submitting user `etl`, and Registry DNS domain `example.net`. The first Master is `master-0.zeta-session.etl.example.net`. Replace this address in all three Hazelcast files with the one for your cluster. If you use a host name instead of Registry DNS, configure YARN to place the Master on that host. Both containers and the submission client must be able to resolve the Master and reach the advertised member addresses. [YARN service discovery](https://hadoop.apache.org/docs/r3.3.6/hadoop-yarn/hadoop-yarn-site/yarn-service/ServiceDiscovery.html)

In `config/hazelcast-master.yaml` and `config/hazelcast-worker.yaml`, set `hazelcast.cluster-name` to `zeta-session`. Replace `hazelcast.network.join` with the following block and keep each file's packaged member port.

```yaml
join:
  tcp-ip:
    enabled: true
    member-list:
      - master-0.zeta-session.etl.example.net:5801
```

Set the following keys in `config/hazelcast-client.yaml` on each submission host. The CLI uses this file to find the running cluster.

```yaml
hazelcast-client:
  cluster-name: zeta-session
  network:
    cluster-members:
      - master-0.zeta-session.etl.example.net:5801
```

## Container ports and memory

The two roles run as processes on NodeManager hosts and may share a host. Use separate member and HTTP ports for the Master and Worker.

| Role | Hazelcast member port | SeaTunnel HTTP port |
| --- | --- | --- |
| Master | 5801 | 8080 |
| Worker | 5802 | 8081 |

The example requests 2 GiB for the Master container and 4 GiB for the Worker. Set `-Xmx1g` in `config/jvm_master_options` and `-Xmx2g` in `config/jvm_worker_options` as starting values. Leave room within each container for metaspace, direct buffers, and thread stacks.

## Engine configuration and storage

Merge these values into `config/seatunnel.yaml`. Replace the example `fs.defaultFS` with your HDFS URI and give the service user write access to the checkpoint directory. The single Master uses `backup-count: 0`.

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

Copy the configuration for the Worker, then change `seatunnel.engine.http.port` to `8081` in the copy. The Worker component selects this file through `SEATUNNEL_CONFIG` in the YARN service definition.

```bash
cp config/seatunnel.yaml config/seatunnel-worker.yaml
```

HDFS checkpoints store operator state. To also preserve the job metadata that Zeta uses for automatic recovery after a full cluster restart, configure a shared IMap MapStore in `config/hazelcast-master.yaml`. [State storage and recovery](../../engines/zeta/state-storage-and-recovery.md)

## Kerberos

On a secured YARN cluster, configure the service ApplicationMaster's principal and keytab in the service definition's top-level `kerberos_principal`. For keytab-based HDFS checkpoint access, configure `kerberosPrincipal` and `kerberosKeytabFilePath` in `seatunnel.engine.checkpoint.storage.plugin-config`. Provision the keytab at a protected path accessible to the relevant processes and keep it out of the shared distribution archive. [YARN Service API](https://hadoop.apache.org/docs/r3.3.6/hadoop-yarn/hadoop-yarn-site/yarn-service/YarnServiceAPI.html#KerberosPrincipal) · [HDFS checkpoint storage](../../engines/zeta/checkpoint-storage.md#hdfs)
