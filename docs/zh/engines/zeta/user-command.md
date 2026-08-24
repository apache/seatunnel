---
sidebar_position: 13
---

# 客户端命令行工具

SeaTunnel Engine 提供了一个命令行工具，用于管理 SeaTunnel Engine 的作业。您可以使用命令行工具提交、停止、暂停、恢复、删除作业，查看作业状态和监控指标等。

可以通过如下命令获取命令行工具的帮助信息：

```shell
bin/seatunnel.sh -h
```

输出如下：

```shell

Usage: seatunnel.sh [options]
  Options:
    --async                                   Run the job asynchronously, when the job
                                              is submitted, the client will exit
                                              (default: false)
    -can, --cancel, --cancel-job              Cancel job(s) by JobId
    -f, --force-cancel, --force-cancel-job    Force Cancel job(s) by jobId
    --check                                   Whether check config (default: false)
    -cj, --close, --close-job                 Close client the task will also be closed
                                              (default: true)
    -cn, --cluster                            The name of cluster
    -c, --config                              Config file
    --decrypt                                 Decrypt config file, When both --decrypt
                                              and --encrypt are specified, only
                                              --encrypt will take effect (default:
                                              false)
    -d, --dry-run                             Validate or preview without running sinks.
                                              Supported modes: [static, connect, sample]
    -m, --master, -e, --deploy-mode           SeaTunnel job submit master, support
                                              [local, cluster] (default: cluster)
    --encrypt                                 Encrypt config file, when both --decrypt
                                              and --encrypt are specified, only
                                              --encrypt will take effect (default:
                                              false)
    --get_running_job_metrics                 Gets metrics for running jobs (default:
                                              false)
    -h, --help                                Show the usage message
    -j, --job-id                              Get job status by JobId
    -l, --list                                list job status (default: false)
    --metrics                                 Get job metrics by JobId
    -n, --name                                SeaTunnel job name (default: SeaTunnel)
    -r, --restore, --restore-job              按 jobId 从最新 Savepoint 恢复
    --restore-with-checkpoint                 按 jobId 从最新成功完成的 Checkpoint 恢复
    -s, --savepoint, --savepoint-job          savepoint job by jobId
    --sample-limit                            Maximum rows forwarded from each source by sample dry-run mode (default: 10, max: 10000)
    --sample-print-data                       Print sampled row values to persistent logs (default: false)
    -i, --variable                            Variable substitution, such as -i
                                              city=beijing, or -i date=20190318.We use
                                              ',' as separator, when inside "", ',' are
                                              treated as normal characters instead of
                                              delimiters. (default: [])

```

## 提交作业

```shell
bin/seatunnel.sh --config $SEATUNNEL_HOME/config/v2.batch.config.template
```

**--async** 参数可以让作业在后台运行，当作业提交后，客户端会退出。

```shell
./bin/seatunnel.sh --config $SEATUNNEL_HOME/config/v2.batch.config.template --async
```

**-n** 或 **--name** 参数可以指定作业的名称

```shell
./bin/seatunnel.sh --config $SEATUNNEL_HOME/config/v2.batch.config.template --async -n myjob
```

## 验证作业配置 (Dry Run)

```shell
bin/seatunnel.sh --config $SEATUNNEL_HOME/config/v2.batch.config.template --dry-run static
```

使用 `--dry-run static`（或者 `--check`）参数可以在**不提交作业**的前提下静态校验配置文件（包括 HOCON/YAML 语法、插件可加载性、DAG 拓扑、必填项与未知配置键等）。它不会执行完整的数据管道。插件加载可能读取本地 JAR，因此这是配置文件的离线校验，不是严格的零 I/O 沙箱。

```shell
bin/seatunnel.sh --config $SEATUNNEL_HOME/config/v2.batch.config.template --dry-run connect
```

使用 `--dry-run connect` 参数会先执行静态校验，然后通过连接器 dry-run 钩子推断 source schema、校验 sink schema 兼容性，并检查连接器连通性。该模式可能连接外部系统以校验凭据、权限以及 source 或 sink 是否存在，但框架不会创建 source/sink 运行时实例、提交作业、读取 source 数据、创建 sink writer、执行 save-mode 逻辑或写入目标数据。Dry-run 校验仅通过 CLI 提供。

**连通性校验由连接器按需实现（opt-in）。** 只有实现了 `SupportSourceDryRunValidation` / `SupportSinkDryRunValidation` 接口的连接器才会真正对外部系统进行校验。当前支持的连接器：

| 连接器 | Source | Sink |
|--------|--------|------|
| Jdbc   | 支持（连通性 + schema 推断） | 支持（连通性 + 表存在性 + 字段兼容性） |
| FakeSource | 支持（仅 schema 推断，无外部系统） | - |

作业中的每个插件都会在校验汇总中报告以下两种状态之一：

- `VALIDATED` – 连接器执行了真实的连通性和/或 schema 校验。
- `SKIPPED` – 连接器不支持 connect dry-run 校验。**对于 `SKIPPED` 的插件，`--dry-run connect` 成功并不代表其凭据或可达性得到了验证。** 对于不支持 dry-run 的 source，配置中显式声明的 schema 字段（`schema` / `tableConfigs` / `table_list` 中的 `fields` 或 `columns`）仍会用于下游 schema 校验；如果配置中也没有声明 schema 字段，则该管道的下游 transform/sink schema 校验同样会报告为 `SKIPPED`，而不是基于占位 schema 进行校验。

### 预览样例数据

```shell
sh bin/seatunnel.sh --master local --config $SEATUNNEL_HOME/config/v2.batch.config.template --dry-run sample --sample-limit 10 --sample-print-data
```

`--dry-run sample` 模式具有以下行为：

- 在本地运行配置的 source 和 transform，并输出表路径和物理行 schema。schema 输出不包含连接器选项或凭据。
- 仅当设置 `--sample-print-data` 时，才输出限定数量的 source 和 transform 行数据。默认不输出行内容，因为持久化的引擎日志可能暴露敏感数据。
- 所有 action 都使用并行度 `1`，包括已配置更高并行度的 source，以确保行数限制作用于整个 source，并使预览输出具有确定性。
- 默认最多从每个 source 向样例管道发送 `10` 行，`--sample-limit` 最大为 `10000`。source reader 停止前可能会完成当前正在执行的 poll 或批次。
- 使用内部无操作 sink 替换配置的 sink，跳过 sink 插件创建和 save-mode 操作，并禁用 checkpoint。
- 可能从外部 source 读取数据，但不会向配置的目标系统写入数据。
- 仅支持本地执行。不支持集群模式、异步提交、恢复、savepoint、校验或作业控制操作。未选择 sample 模式时，sample 相关选项也会被拒绝。

## 查看作业列表

```shell
./bin/seatunnel.sh -l
```

该命令会输出所有当前集群中的作业列表（包含运行完成的历史作业和正在运行的作业）

## 查看作业状态

```shell
./bin/seatunnel.sh -j <jobId>
```

该命令会输出指定作业的状态信息

## 获取正在运行的作业监控信息

```shell
./bin/seatunnel.sh --get_running_job_metrics
```

该命令会输出正在运行的作业的监控信息

## 获取指定作业监控信息

--metrics 参数可以获取指定作业的监控信息

```shell
./bin/seatunnel.sh --metrics <jobId>
```

## 暂停作业

```shell
./bin/seatunnel.sh -s <jobId>
```

该命令会暂停指定作业，注意，只有开启了checkpoint的作业才支持暂停作业(实时同步作业默认开启checkpoint，批处理作业默认不开启checkpoint需要通过在 `env` 中配置checkpoint.interval来开启checkpoint)。

暂停作业是以split为最小单位的，即暂停作业后，会等待当前正在运行的split运行完成后再暂停。任务恢复后，会从暂停的split继续运行。

## 恢复作业

```shell
./bin/seatunnel.sh -r <jobId> -c $SEATUNNEL_HOME/config/v2.batch.config.template
```

该命令会恢复指定作业，注意，只有开启了checkpoint的作业才支持恢复作业(实时同步作业默认开启checkpoint，批处理作业默认不开启checkpoint需要通过在 `env` 中配置checkpoint.interval来开启checkpoint)。

恢复作业需要指定jobId和作业的配置文件。

运行失败的作业和通过seatunnel.sh -s &lt;jobId&gt;暂停的作业都可以通过该命令恢复。

## 取消作业

```shell
./bin/seatunnel.sh -can <jobId1> [<jobId2> <jobId3> ...]
```

该命令会取消指定作业，取消作业后，作业会被停止，作业的状态会变为`CANCELED`。

支持批量取消作业，可以一次取消多个作业。

被cancel的作业的所有断点信息都将被删除，无法通过seatunnel.sh -r &lt;jobId&gt;恢复。

## 强制取消作业

```shell
./bin/seatunnel.sh -f <jobId1> [<jobId2> <jobId3> ...]
```

该命令用于强制取消指定的作业。
作业被取消后，将立即停止执行，其状态将变更为 `CANCELED`。

该命令支持批量操作，可一次性强制取消多个作业。

被cancel的作业的所有断点信息都将被删除，无法通过seatunnel.sh -r &lt;jobId&gt;恢复。

**注意事项**
- 当作业状态为 `DOING_SAVEPOINT` 且 Savepoint 未能成功完成时，启用强制取消（force 选项生效）将直接把作业状态设置为 CANCELED。
- 强制取消可能会导致 Checkpoint 或 Savepoint 数据不完整或处于不一致状态， 仅建议在异常或紧急情况下使用该操作。

## 配置JVM参数

我们可以通过以下方式为 SeaTunnel Engine 客户端配置 JVM 参数：

1. 添加JVM参数到`$SEATUNNEL_HOME/config/jvm_client_options`文件中。

   在 `$SEATUNNEL_HOME/config/jvm_client_options` 文件中修改 JVM 参数。请注意，该文件中的 JVM 参数将应用于使用 `seatunnel.sh` 提交的所有作业，包括 Local 模式和 Cluster 模式。

2. 在提交作业时添加 JVM 参数。例如，`sh bin/seatunnel.sh --config $SEATUNNEL_HOME/config/v2.batch.config.template -DJvmOption="-Xms2G -Xmx2G"`


# 服务端命令行工具

SeaTunnel Engine 提供了服务端管理命令，用于启动、停止和管理 SeaTunnel Engine 集群节点。

```shell
sh bin/seatunnel-cluster.sh -h
```

服务器命令支持以下参数：

```shell
Usage: seatunnel-cluster.sh [options]
  Options:
    -cn, --cluster      集群名称
    -d, --daemon        以守护进程模式运行
    -r, --role          集群节点角色，支持 master、worker、master_and_worker (默认: master_and_worker)
    -m, --member        显示集群成员信息
    -h, --help          显示帮助信息
```

## 启动集群

可以通过如下命令获取服务器命令的帮助信息：

```shell
# 前台启动
sh bin/seatunnel-cluster.sh

# 后台启动（守护进程模式）
sh bin/seatunnel-cluster.sh -d
```

## 查看集群成员信息

您可以使用以下命令查看集群成员信息：

```shell
sh bin/seatunnel-cluster.sh -m -cn my_cluster
```

该命令会输出集群中所有成员的详细信息，包括：
- **Member ID（成员ID）**: 每个集群成员的唯一标识符
- **Address（地址）**: 成员的IP地址和端口
- **Role（角色）**: 成员角色（ACTIVE MASTER、MASTER 或 WORKER）
- **Version（版本）**: 成员运行的 Hazelcast 版本

**输出示例：**
```
Member ID                            Address              Role                 Version
a1b2c3d4-e5f6-7890-abcd-ef1234567890 192.168.1.100:5701  ACTIVE MASTER        5.3.0
b2c3d4e5-f6g7-8901-bcde-f23456789012 192.168.1.101:5701  MASTER               5.3.0
c3d4e5f6-g7h8-9012-cdef-345678901234 192.168.1.102:5701  WORKER               5.3.0
```

**注意**: 必须使用 `-cn` 参数指定集群名称。集群必须处于运行状态才能执行此命令。

## 停止集群

SeaTunnel 提供了专门的停止脚本来关闭集群节点：

```shell
sh bin/stop-seatunnel-cluster.sh -h
```

停止命令支持以下参数：

```shell
Usage: stop-seatunnel-cluster.sh [options]
  Options:
    -cn, --cluster      要关闭的集群名称 (默认: seatunnel_default_cluster)
    -h, --help          显示帮助信息
```

### 停止默认集群

```shell
# 停止默认集群 (seatunnel_default_cluster)
sh bin/stop-seatunnel-cluster.sh
```

### 停止指定集群

```shell
# 停止指定名称的集群
sh bin/stop-seatunnel-cluster.sh -cn my_cluster
```
