---
sidebar_position: 3
title: 快速开始
---

# YARN 快速开始

## 1. 准备发行包

### 使用官方发行包

从 [SeaTunnel 下载页](https://seatunnel.apache.org/download/)下载包含 Application Mode 的二进制发行包并解压。正式版本已经包含运行所需的代码，不需要在使用机器上执行 Maven 构建。

检查发行包包含以下目录和文件：

```text
apache-seatunnel-<version>/
├── bin/seatunnel-application.sh
├── config/
├── starter/seatunnel-starter.jar
├── lib/
├── connectors/
└── resource-managers/yarn/
```

### 从源码构建

需要验证尚未发布的功能或修改代码时，在仓库根目录执行：

```bash
./mvnw -Prelease,seatunnel \
  -pl seatunnel-dist -am -DskipTests package
```

产物为 `seatunnel-dist/target/apache-seatunnel-<version>-bin.tar.gz`。

作业需要的 Connector、Format、Transform 和驱动必须在提交前放入发行包。

## 2. 检查集群并选择作业

用提交作业的身份执行以下检查。`HADOOP_CONF_DIR` 应包含本集群的 `core-site.xml`、`hdfs-site.xml` 和 `yarn-site.xml`。Kerberos 集群可先用 `kinit` 获取票据，再用 `klist` 检查，也可配置提交端 keytab。详见 [Kerberos](configuration.md#kerberos)。

```bash
export HADOOP_CONF_DIR=/etc/hadoop/conf
yarn node -list
yarn queue -status default
hdfs dfs -mkdir -p .seatunnel/applications
hdfs dfs -ls -d .seatunnel/applications
```

目标队列需能同时容纳一个 AM 和所申请的 worker。相对 HDFS 暂存路径从提交身份的工作目录解析。使用共享发行包时，还要检查该身份能否读取归档。

发行包模板在 `yarn.local-resources.job.content` 中内置了 FakeSource 到 Console 的示例作业。可以直接编辑，也可将 `yarn.job-config-file` 指向自己的本地文件或 HDFS URI。若使用 `-c job.conf` 单独提交作业，删除模板的示例作业组和对应的 `job-config-file` 设置。

## 3. 创建部署配置

复制发行包中的模板，并按目标集群修改：

```bash
cp config/v1.yarn.conf.template yarn-deployment.conf
```

保留模板中的作业块，按集群情况设置发行包路径和 Hadoop 配置目录。

```hocon
yarn.distribution = "/opt/packages/apache-seatunnel-<version>-bin.tar.gz"
yarn.config-dir = "/etc/hadoop/conf"
```

`yarn.distribution` 支持包含 `starter/seatunnel-starter.jar` 的 `.tar.gz`、`.tgz` 和 `.zip` 发行包。本地路径和普通 HDFS URI（例如 `hdfs://nameservice1/releases/seatunnel.tgz`）使用本次应用的私有副本。要直接复用已有发行包，可填写 staging 所在 HDFS 的具名快照路径，例如 `hdfs://nameservice1/releases/.snapshot/v1/seatunnel.tgz`。请在应用运行期间保留快照及其读取权限。

要在作业运行时打开 Web UI，可在部署文件中添加 `yarn.master.http-port = 8095`，并确认候选 AM 节点上该端口可用。上面的源码构建会打包页面资源。在干净的源码目录使用 `-Dskip.ui=true` 构建时，HTTP 仅提供 REST API。

## 4. 提交并等待结果

在解压后的 SeaTunnel 目录执行：

```bash
bin/seatunnel-application.sh submit --target yarn \
  --application-config yarn-deployment.conf --wait
```

命令会打印 YARN application ID 和原生 Zeta job ID。保留 application ID 用于状态与取消；需要 checkpoint 恢复时还要保留 Zeta job ID。

去掉 `--wait` 可在 application 启动后退出客户端。关闭客户端不会取消远端作业。

作业已经存放在 HDFS 时，可用 `yarn.job-config-file` 指定 URI。下面的命令从 `HADOOP_CONF_DIR` 读取 Hadoop 配置，发行包和作业文件均通过命令行指定，可以同时省略 `-a` 和 `-c`。

```bash
bin/seatunnel-application.sh submit --target yarn \
  -Dyarn.distribution=hdfs://nameservice1/releases/seatunnel.tgz \
  -Dyarn.job-config-file=hdfs://nameservice1/jobs/orders.conf --wait
```

## 5. 查询或取消

模板运行的是短批作业，预期状态为 `SUCCEEDED`，Console 输出可在 worker 日志中找到。对于长时间运行的作业，可以另开终端查询状态。启用了 AM 的 Web 服务端口时，可在运行期间打开 `http://<AM-host>:8095/`。引擎启用 TLS 时使用 `https://<AM-host>:8095/`。完成后可执行 `yarn logs -applicationId "$APP_ID"` 查看聚合日志。

```bash
bin/seatunnel-application.sh status --target yarn \
  --id application_0000000000000_0001 \
  --application-config yarn-deployment.conf

bin/seatunnel-application.sh cancel --target yarn \
  --id application_0000000000000_0001 \
  --application-config yarn-deployment.conf
```

查询和取消应使用相同的 Hadoop 配置和 `yarn.staging-dir`。命令行 `-Dkey=value` 可以覆盖部署配置，例如 `-Dapplication.worker-count=3`。

下一步请阅读[命令](commands.md)、[配置参考](configuration.md)、[Checkpoint 恢复](checkpoint-recovery.md)和 [FAQ](faq.md)。
