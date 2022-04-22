# 下载并安装

## 下载

```bash
https://github.com/apache/incubator-seatunnel/releases
```

## 环境准备

### 准备好JDK1.8

seatunnel依赖于JDK1.8的运行环境。

### 准备好Flink

请先 [下载 Flink](https://flink.apache.org/downloads.html), 请选择Flink版本>=1.9.0。下载完成后要 [安装 flink](https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/deployment/resource-providers/standalone/overview) 
### 安装 seatunnel

下载seatunnel安装包并解压。

```bash
wget https://github.com/apache/incubator-seatunnel/releases/download/v<version>/seatunnel-<version>.zip -O seatunnel-<version>.zip
unzip seatunnel-<version>.zip
ln -s seatunnel-<version> seatunnel
```

没有任何复杂的安装和配置步骤，请参考 [快速开始](./quick-start.md) 了解seatunnel的用法，并参考 [配置](./configuration)。

如果你想把 `seatunnel` 部署到 `Flink Standalone/Yarn cluster` 上运行，请参考 [seatunnel 部署](./deployment.md)