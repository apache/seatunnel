---
sidebar_position: 5
---

# YARN Configuration

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

Prepare the SeaTunnel distribution on the submission host before uploading it to HDFS. Start in the directory where you unpacked the distribution.

```bash
cd "$SEATUNNEL_HOME"
```

## YARN Service definition

Edit the JSON in the deployment guide for the selected mode before running `yarn app -launch`.

| Field | What to configure |
| --- | --- |
| `queue` | A YARN queue the submitting user can access. Include capacity for the service ApplicationMaster in addition to the job or cluster containers. |
| `configuration.files[].src_file` | The versioned HDFS archive to localize. Its `dest_file` must match the archive name used by the component launch command. |
| `components[].number_of_containers`, `components[].resource` | Instance count per role and allocation per instance. Scaling the `worker` component changes execution capacity in separated mode. |
| `components[].restart_policy` | Use `NEVER` for the one-shot local job. The hybrid and separated examples use the service default, `ALWAYS`, to replace exited members. |
| `components[].dependencies`, `components[].readiness_check` | The separated Worker depends on the Master. A `PORT` check on the Master's port `5801` gates Worker startup. Check the registered Worker count before submitting a job. |

## Member and client discovery

[Local mode](local-mode.md) starts an embedded engine in the job container. The member discovery and client settings below apply to the two shared cluster modes.

The examples use the submitting user `etl` and Registry DNS domain `example.net`. Use the service name and seed address for your deployment in both the member and client configurations.

| Mode | Cluster name | Member configuration under `config/` | Seed address |
| --- | --- | --- | --- |
| [Hybrid](hybrid-cluster-mode.md) | `zeta-hybrid` | `hazelcast.yaml` | `hybrid-0.zeta-hybrid.etl.example.net:5801` |
| [Separated](separated-cluster-mode.md) | `zeta-session` | `hazelcast-master.yaml`, `hazelcast-worker.yaml` | `master-0.zeta-session.etl.example.net:5801` |

Containers and clients must resolve the seed address and reach the advertised member addresses. If you use a fixed hostname, configure YARN placement so the corresponding member runs on that host. [YARN service discovery](https://hadoop.apache.org/docs/r3.3.6/hadoop-yarn/hadoop-yarn-site/yarn-service/ServiceDiscovery.html)

Merge the following into the packaged `config/hazelcast-master.yaml`. For `config/hazelcast-worker.yaml`, use the same cluster name and seed address, and set the member port to `5802`. Keep the packaged heartbeat and failure detector settings in both files. The launch command selects the role with `-r master` or `-r worker`. For a hybrid cluster, merge the block into `config/hazelcast.yaml` using the hybrid values in the table.

```yaml
hazelcast:
  cluster-name: zeta-session
  network:
    port:
      port: 5801
      auto-increment: false
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

### Reach a Master after YARN reschedules it

For process containers, YARN Registry DNS resolves each instance name to the IP of its current NodeManager host. Ask the Hadoop administrator to run Registry DNS with `hadoop.registry.dns.enabled=true`, the cluster's `hadoop.registry.zk.quorum`, and a `hadoop.registry.dns.domain-name` in `core-site.xml`. Configure the DNS resolvers used by submission hosts and NodeManagers to forward that zone to Registry DNS. [Registry DNS setup](https://hadoop.apache.org/docs/r3.3.6/hadoop-project-dist/hadoop-common/registry/registry-dns.html) · [YARN service discovery](https://hadoop.apache.org/docs/r3.3.6/hadoop-yarn/hadoop-yarn-site/yarn-service/ServiceDiscovery.html)

Once the Master starts, run `getent hosts master-0.zeta-session.etl.example.net` from the submission host and Worker host. Both must reach the Master at the returned address on port 5801. Hazelcast uses that address to join the cluster, then connects directly to advertised member addresses. Allow traffic between the member hosts on their configured Hazelcast ports.

### Place fixed-port Workers on different hosts

The example uses ordinary process containers. Two Workers allocated to the same NodeManager share its IP and both try to bind ports 5802 and 8081. Before launching a service that you plan to scale, add the following field to its existing `worker` component in `zeta-session.json`. The target tag matches the component name. [YARN Service placement policy](https://hadoop.apache.org/docs/r3.3.6/hadoop-yarn/hadoop-yarn-site/yarn-service/YarnServiceAPI.html)

```json
{
  "placement_policy": {
    "constraints": [
      {
        "type": "ANTI_AFFINITY",
        "scope": "NODE",
        "target_tags": ["worker"]
      }
    ]
  }
}
```

Ask the Hadoop administrator to enable placement constraints on the ResourceManager. Hadoop 3.3.6 defaults `yarn.resourcemanager.placement-constraints.handler` to `disabled`. Set it to `placement-processor` or, with CapacityScheduler, `scheduler`. This rule places at most one Worker of this service on each eligible node. Two Workers need two eligible nodes with the ports free. If the available hosts cannot satisfy the request, YARN cannot bring the component to its requested size. [Hadoop placement constraints](https://hadoop.apache.org/docs/r3.3.6/hadoop-yarn/hadoop-yarn-site/PlacementConstraints.html)

## Container resources and ports

Each YARN container requests one allocation in vcores and MiB. The examples use the container limits from the corresponding Kubernetes [local](../kubernetes/local-mode.md), [hybrid](../kubernetes/hybrid-cluster-mode.md), and [separated](../kubernetes/separated-cluster-mode.md) deployments.

| Component | vcores | Memory | Hazelcast / HTTP ports | JVM options under `config/` | Example heap |
| --- | --- | --- | --- | --- | --- |
| Local job | 1 | 4 GiB | No client endpoint required | `jvm_client_options` | — |
| Hybrid member | 2 | 4 GiB | 5801 / 8080 | `jvm_options` | `-Xmx2g` |
| Separated Master | 8 | 24 GiB | 5801 / 8080 | `jvm_master_options` | `-Xms16g -Xmx16g` |
| Separated Worker | 8 | 24 GiB | 5802 / 8081 | `jvm_worker_options` | `-Xms16g -Xmx16g` |

A hybrid member shares one JVM heap between Master and Worker duties. In separated mode, set the heap flags in each role's options file, one flag per line. The rest of each allocation provides room for metaspace, direct buffers, and thread stacks.

## Engine configuration and checkpoint storage

Merge these settings into `config/seatunnel.yaml` before packaging the distribution. This example gives each 8-vcore separated Worker eight fixed slots. `WAIT` queues jobs when the required slots are unavailable. The separated Master manages scheduling and checkpoints, and the Workers supply execution slots.

```yaml
seatunnel:
  engine:
    backup-count: 0
    slot-service:
      dynamic-slot: false
      slot-num: 8
    job-schedule-strategy: WAIT
    checkpoint:
      interval: 180000
      timeout: 30000
      storage:
        type: hdfs
        max-retained: 3
        plugin-config:
          storage.type: hdfs
          fs.defaultFS: hdfs://namenode.example.net:8020
          namespace: /user/etl/seatunnel/checkpoints/zeta-session/
    http:
      enable-http: true
      port: 8080
```

The interval and timeout are in milliseconds. This example starts a checkpoint every three minutes, allows 30 seconds for it to complete, and sets the retention target to three completed checkpoints per pipeline. Size the timeout for the state and storage latency of your jobs. Replace `fs.defaultFS` with your HDFS URI. Run the following as the service user, or grant it read and write access to the two directories.

```bash
hdfs dfs -mkdir -p /user/etl/seatunnel/checkpoints/zeta-session /user/etl/seatunnel/imap
```

For `zeta-hybrid`, use `/user/etl/seatunnel/checkpoints/zeta-hybrid/` as the checkpoint namespace. The 2-vcore hybrid member shares CPU and memory between scheduling and tasks. For a trial, keep `dynamic-slot: true` and omit `slot-num` and `job-schedule-strategy`. To cap concurrency, set a smaller fixed slot count that can accommodate your job and use `WAIT`. [Slot allocation](../../engines/zeta/slot-allocation-strategy.md)

**Separated mode only.** After setting the engine and storage options, copy the completed Master configuration, then set `seatunnel.engine.http.port` to `8081` in the copy. The service definition selects that file for the Worker through `SEATUNNEL_CONFIG`.

```bash
cp config/seatunnel.yaml config/seatunnel-worker.yaml
```

## Persist job metadata across a cluster restart

Checkpoints hold operator state such as source offsets. To recover a running job automatically after every member stops, Zeta also needs the job metadata in its Hazelcast IMaps. Merge the following `map` entry under the existing `hazelcast` key in `config/hazelcast-master.yaml` on every separated Master. Keep the network and discovery settings. Separated Workers do not own IMap partitions.

```yaml
hazelcast:
  map:
    engine*:
      map-store:
        enabled: true
        initial-mode: EAGER
        factory-class-name: org.apache.seatunnel.engine.server.persistence.FileMapStoreFactory
        properties:
          type: hdfs
          storage.type: hdfs
          fs.defaultFS: hdfs://namenode.example.net:8020
          namespace: /user/etl/seatunnel/imap
          clusterName: zeta-session
```

For hybrid mode, put the same `map` entry in `config/hazelcast.yaml` on every hybrid member, with `clusterName: zeta-hybrid`. Keep the MapStore namespace separate from checkpoints. Use the same filesystem, namespace and `clusterName` when restarting a service so its members find the saved metadata. Give independent clusters different `clusterName` values.

The one-Master and one-hybrid-member examples use `backup-count: 0`. To keep an in-memory backup on another host, run at least two data-owning members on different hosts and set `backup-count: 1`. A separated Worker does not count as a backup member. Restarting the YARN service replaces its processes. Resuming from a previous checkpoint depends on the job metadata, a usable completed checkpoint and access to the original job dependencies and external systems. [State storage and recovery](../../engines/zeta/state-storage-and-recovery.md)

### Using S3 or OSS

Keep `type: hdfs` for the storage plugin and select `storage.type: s3` or `storage.type: oss` in the checkpoint `plugin-config` and MapStore `properties`. S3 uses `s3.bucket` (`s3a://bucket`) and `fs.s3a.*` options. OSS uses `oss.bucket` (`oss://bucket`) and `fs.oss.*` options. Include the filesystem dependencies in the staged distribution, supply credentials to the processes that access the bucket, and use distinct prefixes for checkpoints and IMap data. [Checkpoint storage](../../engines/zeta/checkpoint-storage.md) · [IMap storage examples](../../engines/zeta/separated-cluster-deployment.md)

## Kerberos and HDFS access

On a Kerberized YARN cluster, merge the following field into the top level of the service JSON. Use the service principal and a keytab provisioned on every host eligible to run the ApplicationMaster. Hadoop 3.3.6 substitutes the AM hostname in `_HOST`. [Hadoop YARN Service security](https://hadoop.apache.org/docs/r3.3.6/hadoop-yarn/hadoop-yarn-site/yarn-service/QuickStart.html#Run_with_security)

```json
{
  "kerberos_principal": {
    "principal_name": "seatunnel/_HOST@EXAMPLE.COM",
    "keytab": "file:///etc/security/keytabs/seatunnel-am.keytab"
  }
}
```

For HDFS keytab login, merge these keys into the checkpoint `plugin-config` and, when enabled, the HDFS MapStore `properties`. The path must be readable by the relevant Zeta processes on each eligible host. Keep keytabs out of the distribution archive.

```yaml
kerberosPrincipal: etl@EXAMPLE.COM
kerberosKeytabFilePath: /etc/security/keytabs/etl.keytab
```

For an HA nameservice, set `fs.defaultFS` to its logical URI in both storage configurations and point `hdfs_site_path` to an accessible `hdfs-site.xml` containing the nameservice and failover settings. [HDFS checkpoint storage](../../engines/zeta/checkpoint-storage.md#hdfs)

## Package connectors, drivers and configuration

On the submission host, select the connectors for your jobs in `config/plugin_config`. The batch example uses `connector-fake` and `connector-console`. To install additional connectors for the example release, run:

```bash
cd "$SEATUNNEL_HOME"
bash bin/install-plugin.sh 3.1.0
```

| Contents | Location in the prepared distribution |
| --- | --- |
| Connector implementation JARs | `connectors/`. For a custom connector, include its entry in `connectors/plugin-mapping.properties`. |
| Connector-specific dependencies | `plugins/<connector-artifact>/`. Use the artifact name from `connectors/plugin-mapping.properties` so Zeta loads these JARs with that connector. [Isolated connector dependencies](../../connectors/connector-isolated-dependency.md) |
| JDBC driver for the Zeta JDBC connector | Place the vendor's compatible driver JAR in `lib/`, as specified by the [JDBC connector](../../connectors/sink/Jdbc.md#zeta-engine). Follow the chosen connector's instructions for other drivers. |
| Transform JARs and process-wide libraries | `lib/` is on the client and server JVM classpaths. Keep the packaged `seatunnel-transforms-v2.jar` and add any custom Transform JAR here. |

Package `bin/`, `config/`, `connectors/`, `lib/` and `plugins/` with the rest of the distribution after configuring the selected mode. Keep matching JAR versions on the submission client and in every component. Provision keytabs and other credentials separately at the paths used by the container processes.

When changing configuration or JARs, upload a new archive, for example `zeta-hybrid-3.1.0-r2.tgz`, and update `configuration.files[].src_file` in the service definition used for the deployment. Retain the previous archive while containers can still be restarted from its definition. A replacement container localizes its own copy from HDFS.

### Connector paths on NodeManager hosts

With the default JAR settings in SeaTunnel 3.1.0, the cluster client sends absolute file paths for the job's connector JARs. Each hybrid member, Master and Worker must be able to read those paths. The launch command extracts the archive under each container's working directory.

For example, if the client runs from `/opt/seatunnel/3.1.0`, install the same `connector-fake-3.1.0.jar` and `connector-console-3.1.0.jar` under `/opt/seatunnel/3.1.0/connectors/` on every eligible NodeManager, or mount that directory read-only at the same path. Apply this to any other local JAR paths submitted with the job. A missing path causes `NOT-FOUND-JAR` even when the archive contains the file. In local mode the client and engine share the container's extracted installation.

## Capture container logs

Hybrid members and separated Masters and Workers use `config/log4j2.properties`. By default they write rolling files under `logs/` in the extracted installation. To also send engine messages to the container's stdout and stderr, uncomment both references before packaging:

```properties
rootLogger.appenderRef.consoleStdout.ref = consoleStdoutAppender
rootLogger.appenderRef.consoleStderr.ref = consoleStderrAppender
```

The submission CLI and the local-mode job use `config/log4j2_client.properties`, which enables both console appenders by default. With `yarn.log-aggregation-enable=true`, retrieve container output by YARN application ID. CLI output from a submission host stays on that host. [Read container logs](operations.md#read-container-logs)
