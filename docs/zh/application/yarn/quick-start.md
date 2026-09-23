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
  -pl seatunnel-dist -am -DskipTests -Dskip.ui=true package
```

产物为 `seatunnel-dist/target/apache-seatunnel-<version>-bin.tar.gz`。

作业需要的 Connector、Format、Transform 和驱动必须在提交前放入发行包。

## 2. 创建作业配置

保存为 `job.conf`：

```hocon
env {
  job.mode = "BATCH"
  parallelism = 2
}
source {
  FakeSource {
    row.num = 10
    split.num = 2
    schema { fields { id = int, name = string } }
  }
}
sink { Console {} }
```

## 3. 创建部署配置

复制发行包中的模板，并按目标集群修改：

```bash
cp config/v1.yarn.conf.template yarn-deployment.conf
```

生成的 `yarn-deployment.conf` 应包含：

```hocon
application {
  name = "example-yarn-application"
  worker-count = 2
  worker.memory-mb = 1024
  worker.cpu-cores = 1
  worker.slots = 2
  master.memory-mb = 1024
  master.cpu-cores = 1
  startup-timeout-millis = 120000
}
yarn {
  distribution = "/opt/packages/apache-seatunnel-<version>-bin.tar.gz"
  config-dir = "/etc/hadoop/conf"
  staging-dir = "hdfs:///user/seatunnel/applications"
  queue = "default"
}
```

`yarn.distribution` 指向提交机器可读取的本地归档。Provider 支持 `.tar.gz`、`.tgz` 和 `.zip`，会自动识别包含 `starter/seatunnel-starter.jar` 的顶层目录。

## 4. 提交并等待结果

在解压后的 SeaTunnel 目录执行：

```bash
bin/seatunnel-application.sh submit --target yarn \
  --config job.conf --deployment-config yarn-deployment.conf --wait
```

命令会打印 YARN application ID 和原生 Zeta job ID。保留 application ID 用于状态与取消；需要 checkpoint 恢复时还要保留 Zeta job ID。

去掉 `--wait` 可在 application 启动后退出客户端。关闭客户端不会取消远端作业。

## 5. 查询或取消

```bash
bin/seatunnel-application.sh status --target yarn \
  --id application_0000000000000_0001 \
  --deployment-config yarn-deployment.conf

bin/seatunnel-application.sh cancel --target yarn \
  --id application_0000000000000_0001 \
  --deployment-config yarn-deployment.conf
```

查询和取消应使用相同的 Hadoop 配置和 `yarn.staging-dir`。命令行 `-Dkey=value` 可以覆盖部署配置，例如 `-Dapplication.worker-count=3`。

下一步请阅读[配置参考](configuration.md)、[Checkpoint 恢复](checkpoint-recovery.md)和 [FAQ](faq.md)。
