# 下载和安装

## 下载

```bash
https://github.com/apache/incubator-seatunnel/releases
```

## 环境准备

### 准备 JDK1.8

`Seatunnel` 依赖`JDK1.8`.

### 准备Spark

`Seatunnel` 支持`Spark`引擎。如果希望在`Spark`中使用`SeaTunnel`，需要提前准备好`Spark`环境。[下载 Spark](https://spark.apache.org/downloads.html) , 选择 `Spark 版本 >= 2.x.x 
`，目前还不支持spark3.x。 在下载和解压后, 
不需要任何配置就可以指定 
`deploy-mode = local` 提交任务。 如果需要使用其他模式，如 `Standalone cluster`，`Yarn cluster` ，`Mesos cluster`, 请参考官方文档。

## 安装 Seatunnel

下载`seatunnel` 安装包并解压:

```bash
wget https://github.com/apache/incubator-seatunnel/releases/download/v<version>/seatunnel-<version>.zip -O seatunnel-<version>.zip
unzip seatunnel-<version>.zip
ln -s seatunnel-<version> seatunnel
```

这里没有完整的安装和配置步骤。 请参考 [快速开始](./quick-start.md) 和 [配置](./configuration)去使用`seatunnel`。
