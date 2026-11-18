---
sidebar_position: 4
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

The examples use the submitting user `etl` and Registry DNS domain `example.net`. Use the service name and seed address for your deployment in both the member and client configurations.

| Mode | Cluster name | Member configuration under `config/` | Seed address |
| --- | --- | --- | --- |
| [Hybrid](hybrid-cluster-mode.md) | `zeta-hybrid` | `hazelcast.yaml` | `hybrid-0.zeta-hybrid.etl.example.net:5801` |
| [Separated](separated-cluster-mode.md) | `zeta-session` | `hazelcast-master.yaml`, `hazelcast-worker.yaml` | `master-0.zeta-session.etl.example.net:5801` |

Containers and clients must resolve the seed address and reach the advertised member addresses. If you use a fixed hostname, configure YARN placement so the corresponding member runs on that host. [YARN service discovery](https://hadoop.apache.org/docs/r3.3.6/hadoop-yarn/hadoop-yarn-site/yarn-service/ServiceDiscovery.html)

For each member file, set `hazelcast.cluster-name` and replace the `hazelcast.network.join` block, keeping the packaged member port. The following member and client snippets use the separated cluster's values. For hybrid mode substitute the cluster name and address from the first row.

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

Each row describes one YARN container. A hybrid member shares its JVM and heap between Master and Worker duties. In separated mode the Master and Worker use different ports so that one of each can run on the same NodeManager host.

| Component | Hazelcast port | HTTP port | Container memory | JVM options under `config/` | Example heap |
| --- | --- | --- | --- | --- | --- |
| Hybrid member | 5801 | 8080 | 4 GiB | `jvm_options` | `-Xmx2g` |
| Separated Master | 5801 | 8080 | 2 GiB | `jvm_master_options` | `-Xmx1g` |
| Separated Worker | 5802 | 8081 | 4 GiB | `jvm_worker_options` | `-Xmx2g` |

Set the heap in the file for each component. Leave room within the container for metaspace, direct buffers, and thread stacks.

## Engine configuration and storage

Merge these values into `config/seatunnel.yaml`. Replace the example `fs.defaultFS` with your HDFS URI and give the service user write access to the checkpoint directory. Both examples have one member that can store IMap data and use `backup-count: 0`. For the hybrid example, set `namespace` to `/user/etl/seatunnel/checkpoints/zeta-hybrid`.

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

In separated mode only, copy the configuration for the Worker, then change `seatunnel.engine.http.port` to `8081` in the copy. The Worker component selects this file through `SEATUNNEL_CONFIG` in the YARN service definition.

```bash
cp config/seatunnel.yaml config/seatunnel-worker.yaml
```

HDFS checkpoints store operator state. To also preserve the job metadata that Zeta uses for automatic recovery after a full cluster restart, configure a shared IMap MapStore in `config/hazelcast.yaml` on all hybrid members, or in `config/hazelcast-master.yaml` on the Masters of a separated cluster. [State storage and recovery](../../engines/zeta/state-storage-and-recovery.md)

## Kerberos

On a secured YARN cluster, configure the service ApplicationMaster's principal and keytab in the service definition's top-level `kerberos_principal`. For keytab-based HDFS checkpoint access, configure `kerberosPrincipal` and `kerberosKeytabFilePath` in `seatunnel.engine.checkpoint.storage.plugin-config`. Provision the keytab at a protected path accessible to the relevant processes and keep it out of the shared distribution archive. [YARN Service API](https://hadoop.apache.org/docs/r3.3.6/hadoop-yarn/hadoop-yarn-site/yarn-service/YarnServiceAPI.html#KerberosPrincipal) · [HDFS checkpoint storage](../../engines/zeta/checkpoint-storage.md#hdfs)
