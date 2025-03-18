---
sidebar_position: 4
---

# 以Local模式运行作业

仅用于测试。

Local模式下每个任务都会启动一个独立的进程，任务运行完成后进程会退出。在该模式下有以下限制：

1. 不支持任务的暂停、恢复。
2. 不支持获取任务列表查看。
3. 不支持通过命令取消作业，只能通过Kill进程的方式终止任务。
4. 不支持RESTful API。

最推荐在生产环境中使用SeaTunnel Engine的[分离集群模式](separated-cluster-deployment.md)

## 本地模式部署SeaTunnel Engine

本地模式下，不需要部署SeaTunnel Engine集群，只需要使用如下命令即可提交作业即可。系统会在提交提交作业的进程中启动SeaTunnel Engine(Zeta)服务来运行提交的作业，作业完成后进程退出。

该模式下只需要将下载和制作好的安装包拷贝到需要运行的服务器上即可，如果需要调整作业运行的JVM参数，可以修改$SEATUNNEL_HOME/config/jvm_client_options文件。

## 提交作业

```shell
$SEATUNNEL_HOME/bin/seatunnel.sh --config $SEATUNNEL_HOME/config/v2.batch.config.template -e local
```

### 配置本地模式的JVM参数

本地模式支持两种设置JVM参数的方式：

1. 添加JVM参数到`$SEATUNNEL_HOME/config/jvm_client_options`文件中。

   修改`$SEATUNNEL_HOME/config/jvm_client_options`文件中的JVM参数。 请注意，该文件中的JVM参数会应用到所有使用`seatunnel.sh`提交的作业。包括Local模式和集群模式。

2. 在启动Local模式时添加JVM参数。例如，`$SEATUNNEL_HOME/bin/seatunnel.sh --config $SEATUNNEL_HOME/config/v2.batch.config.template -m local -DJvmOption="-Xms2G -Xmx2G"`

## 作业运维

Local模式下提交的作业会在提交作业的进程中运行，作业完成后进程会退出，如果要中止作业只需要退出提交作业的进程即可。作业的运行日志会输出到提交作业的进程的标准输出中。

不支持其它运维操作。
