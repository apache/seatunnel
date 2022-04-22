# 部署和运行

`seatunnel for Flink` 依赖于Java运行时环境和 `Flink`。关于详细的 `seatunnel` 安装步骤，请参考安装[seatunnel](./installation.md)。

下面重点介绍不同平台的运行方式：

> 首先编辑解压后 `seatunnel` 目录下的 `config/seatunnel-env.sh`，并指定需要的环境配置 `FLINK_HOME`。

## 在Flink独立集群上运行seatunnel

```bash
bin/start-seatunnel-flink.sh \
--config config-path

# -p 2 specifies that the parallelism of flink job is 2. You can also specify more parameters, use flink run -h to view
bin/start-seatunnel-flink.sh \
-p 2 \
--config config-path
```

## 在Yarn集群上运行seatunnel

```bash
bin/start-seatunnel-flink.sh \
-m yarn-cluster \
--config config-path

# -ynm seatunnel specifies the name displayed in the yarn webUI as seatunnel, you can also specify more parameters, use flink run -h to view
bin/start-seatunnel-flink.sh \
-m yarn-cluster \
-ynm seatunnel \
--config config-path
```

请参考: [Flink Yarn Setup](https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/deployment/resource-providers/yarn)