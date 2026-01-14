---
sidebar_position: 9
---

# Config Engine Jar Storage Mode

:::caution warn

Please note that this feature is currently in an experimental stage, and there are many areas that still need improvement. Therefore, we recommend exercising caution when using this feature to avoid potential issues and unnecessary risks.
We are committed to ongoing efforts to enhance and stabilize this functionality, ensuring a better experience for you.

:::

We can enable the optimization job submission process, which is configured in the `seatunel.yaml`. After enabling the optimization of the Seatunnel job submission process configuration item,
users can use the Seatunnel engine(Zeta) as the execution engine without placing the connector jar packages required for task execution or the third-party jar packages that the connector relies on in each engine `connector` directory.
Users only need to place all the jar packages for task execution on the client that submits the job, and the client will automatically upload the jars required for task execution to the Zeta engine. It is necessary to enable this configuration item when submitting jobs in Docker or k8s mode,
which can fundamentally solve the problem of large container images caused by the heavy weight of the Seatunnel Zeta engine. In the image, only the core framework package of the Zeta engine needs to be provided,
and then the jar package of the connector and the third-party jar package that the connector relies on can be separately uploaded to the pod for distribution.

After enabling the optimization job submission process configuration item, you do not need to place the following two types of jar packages in the Zeta engine:
- COMMON_PLUGIN_JARS
- CONNECTOR_PLUGIN_JARS

COMMON_ PLUGIN_ JARS refers to the third-party jar package that the connector relies on, CONNECTOR_ PLUGIN_ JARS refers to the connector jar package.
When common jars do not exist in Zeta's `lib`, it can upload the local common jars of the client to the `lib` directory of all engine nodes.
This way, even if the user does not place a jar on all nodes in Zeta's `lib`, the task can still be executed normally.
However, we do not recommend relying on the configuration item of opening the optimization job submission process to upload the third-party jar package that the connector relies on.
If you use Zeta Engine, please add the third-party jar package files that the connector relies on to `$SEATUNNEL_HOME/lib/` directory on each node, such as jdbc drivers.

# ConnectorJar Storage Strategy

You can configure the storage strategy of the current connector jar package and the third-party jar package that the connector depends on through the configuration file.
There are two storage strategies that can be configured, namely shared jar package storage strategy and isolated jar package storage strategy.
Two different storage strategies provide a more flexible storage mode for jar files. You can configure the storage strategy to share the same jar package file with multiple execution jobs in the engine.

## Related Configuration

|              Parameter              | Default Value |                                                                      Describe                                                                      |
|-------------------------------------|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| connector-jar-storage-enable        | false         | Whether to enable uploading the connector jar package to the engine. The default enabled state is false.                                           |
| connector-jar-storage-mode          | SHARED        | Engine-side jar package storage mode selection. There are two optional modes, SHARED and ISOLATED. The default Jar package storage mode is SHARED. |
| connector-jar-storage-path          | " "           | User-defined jar package storage path.                                                                                                             |
| connector-jar-cleanup-task-interval | 3600s         | Engine-side jar package cleaning scheduled task execution interval.                                                                                |
| connector-jar-expiry-time           | 600s          | Engine-side jar package storage expiration time.                                                                                                   |

## IsolatedConnectorJarStorageStrategy

Before the job is submitted, the connector Jjr package will be uploaded to an independent file storage path on the Master node.
The connector jar packages of different jobs are in different storage paths, so the connector jar packages of different jobs are isolated from each other.
The jar package files required for the execution of a job have no influence on other jobs. When the current job execution ends, the jar package file in the storage path generated based on the JobId will be deleted.

Example:

```yaml
jar-storage:
   connector-jar-storage-enable: true
   connector-jar-storage-mode: ISOLATED
   connector-jar-storage-path: ""
   connector-jar-cleanup-task-interval: 3600
   connector-jar-expiry-time: 600
```

Detailed explanation of configuration parameters:
- connector-jar-storage-enable: Enable uploading the connector jar package before executing the job.
- connector-jar-storage-mode: Connector jar package storage mode, two storage modes are available: shared mode (SHARED) and isolation mode (ISOLATED).
- connector-jar-storage-path: The local storage path of the user-defined connector jar package on the Zeta engine.
- connector-jar-cleanup-task-interval: Zeta engine connector jar package scheduled cleanup task interval, the default is 3600 seconds.
- connector-jar-expiry-time: The expiration time of the connector jar package. The default is 600 seconds.

## SharedConnectorJarStorageStrategy

Before the job is submitted, the connector jar package will be uploaded to the Master node. Different jobs can share connector jars on the Master node if they use the same Jar package file.
All jar package files are persisted to a shared file storage path, and jar packages that reference the Master node can be shared between different jobs. After the task execution is completed,
the SharedConnectorJarStorageStrategy will not immediately delete all jar packages related to the current task execution，but instead has an independent thread responsible for cleaning up the work.
The configuration in the following configuration file sets the running time of the cleaning work and the survival time of the jar package.

Example:

```yaml
jar-storage:
   connector-jar-storage-enable: true
   connector-jar-storage-mode: SHARED
   connector-jar-storage-path: ""
   connector-jar-cleanup-task-interval: 3600
   connector-jar-expiry-time: 600
```

Detailed explanation of configuration parameters:
- connector-jar-storage-enable: Enable uploading the connector jar package before executing the job.
- connector-jar-storage-mode: Connector jar package storage mode, two storage modes are available: shared mode (SHARED) and isolation mode (ISOLATED).
- connector-jar-storage-path: The local storage path of the user-defined connector jar package on the Zeta engine.
- connector-jar-cleanup-task-interval: Zeta engine connector Jjr package scheduled cleanup task interval, the default is 3600 seconds.
- connector-jar-expiry-time: The expiration time of the connector jar package. The default is 600 seconds.

