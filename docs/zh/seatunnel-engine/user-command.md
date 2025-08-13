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
    --async                         Run the job asynchronously, when the job 
                                    is submitted, the client will exit 
                                    (default: false)
    -can, --cancel-job              Cancel job by JobId
    --check                         Whether check config (default: false)
    -cj, --close-job                Close client the task will also be closed 
                                    (default: true)
    -cn, --cluster                  The name of cluster
    -c, --config                    Config file
    --decrypt                       Decrypt config file, When both --decrypt 
                                    and --encrypt are specified, only 
                                    --encrypt will take effect (default: 
                                    false) 
    -m, --master, -e, --deploy-mode SeaTunnel job submit master, support 
                                    [local, cluster] (default: cluster)
    --encrypt                       Encrypt config file, when both --decrypt 
                                    and --encrypt are specified, only 
                                    --encrypt will take effect (default: 
                                    false) 
    --get_running_job_metrics       Gets metrics for running jobs (default: 
                                    false) 
    -h, --help                      Show the usage message
    -j, --job-id                    Get job status by JobId
    -l, --list                      list job status (default: false)
    --metrics                       Get job metrics by JobId
    -n, --name                      SeaTunnel job name (default: SeaTunnel)
    -r, --restore                   restore with savepoint by jobId
    -s, --savepoint                 savepoint job by jobId
    -i, --variable                  Variable substitution, such as -i 
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