---
sidebar_position: 3
title: 下载
---

# 下载与构建 Edge Agent 安装包

## 步骤 1：环境准备

在边缘主机上准备：

* Java：安装 [Java](https://www.java.com/zh-CN/download/) 8 或 11（更高版本 LTS 一般可用），并设置 JAVA_HOME。
* 磁盘：queue.sqlite-path（WAL 与位点）及日志目录可写。
* 网络：边缘主机可访问 output.endpoint 配置的 EdgeSocket 地址。

边缘主机 不需要 安装完整的 SeaTunnel Engine 分发包。

## 步骤 2：下载二进制包

Edge Agent 以 独立 tarball 发布，与 `apache-seatunnel-<version>-bin.tar.gz` 分离：

`apache-seatunnel-edge-agent-<version>-bin.tar.gz`

在 [SeaTunnel 下载页](https://seatunnel.apache.org/download) 提供后，可执行：

```shell
export version="<seatunnel-version>"
wget "https://archive.apache.org/dist/seatunnel/${version}/apache-seatunnel-edge-agent-${version}-bin.tar.gz"
tar -xzf "apache-seatunnel-edge-agent-${version}-bin.tar.gz"
cd "apache-seatunnel-edge-agent-${version}"
```

解压后，安装根目录为包含 bin/、config/、starter/ 的目录。

:::tip 安装根目录变量

下文中的 EDGE_AGENT_HOME 即指该安装根目录。启动脚本按自身路径解析目录；建议导出 EDGE_AGENT_HOME 便于运维与 systemd 配置。

:::

## 步骤 3：源码构建

```shell
git clone https://github.com/apache/seatunnel.git
cd seatunnel
./mvnw clean package -pl seatunnel-dist -am -DskipTests -Dskip.spotless=true
```

构建产物：

```text
seatunnel-dist/target/apache-seatunnel-edge-agent-<version>-bin.tar.gz
```

将 tarball 拷贝到边缘主机并解压。

:::note 无需 install-plugin

与主 SeaTunnel 分发包不同，Edge Agent 不需要 执行 bin/install-plugin.sh。内置输入、传输与序列化能力已打入 seatunnel-edge-agent-starter.jar。

:::

## 步骤 4：确认目录结构

```text
apache-seatunnel-edge-agent-<version>/
  bin/
    seatunnel-edge-agent.sh
    seatunnel-edge-agent.cmd
  config/
    agent.yaml          # 示例配置
    log4j2.properties
  starter/
    seatunnel-edge-agent-starter.jar
    logging/            # 日志依赖
  README.md
```

扩展（高级）：默认安装包不含 lib/ 目录；仅在你自行构建扩展插件时，可将额外 jar 放入 lib/。

## 下一步

* [快速开始](quick-start.md)
* [部署指南](deployment-guide.md)
