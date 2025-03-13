---
sidebar_position: 1
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 部署

## 准备工作

在开始本地运行前，您需要确保您已经安装了SeaTunnel所需要的以下软件：

* 安装[Java](https://www.java.com/en/download/) (Java 8 或 11， 其他高于Java 8的版本理论上也可以工作) 以及设置 `JAVA_HOME`。

## 下载 SeaTunnel 发行包

### 下载二进制包

进入[SeaTunnel下载页面](https://seatunnel.apache.org/download)下载最新版本的二进制安装包`seatunnel-<version>-bin.tar.gz`

或者您也可以通过终端下载：

```shell
export version="2.3.10"
wget "https://archive.apache.org/dist/seatunnel/${version}/apache-seatunnel-${version}-bin.tar.gz"
tar -xzvf "apache-seatunnel-${version}-bin.tar.gz"
```

### 下载连接器插件

从2.2.0-beta版本开始，二进制包不再默认提供连接器依赖，因此在第一次使用时，您需要执行以下命令来安装连接器：(当然，您也可以从 [Apache Maven Repository](https://repo.maven.apache.org/maven2/org/apache/seatunnel/) 手动下载连接器，然后将其移动至`connectors/`目录下，如果是2.3.5之前则需要放入`connectors/seatunnel`目录下)。

```bash
sh bin/install-plugin.sh
```

如果您需要指定的连接器版本，以2.3.10为例，您需要执行如下命令：

```bash
sh bin/install-plugin.sh 2.3.10
```

通常情况下，你不需要所有的连接器插件。你可以通过配置`config/plugin_config`来指定所需的插件。例如，如果你想让示例应用程序正常工作，你将需要`connector-console`和`connector-fake`插件。你可以修改`plugin_config`配置文件，如下所示：

```plugin_config
--seatunnel-connectors--
connector-fake
connector-console
--end--
```

您可以在`${SEATUNNEL_HOME}/connectors/plugins-mapping.properties`下找到所有支持的连接器和相应的plugin_config配置名称。

:::tip 提示

如果您想通过手动下载连接器的方式来安装连接器插件，则需要下载您所需要的连接器插件即可，并将它们放在`${SEATUNNEL_HOME}/connectors/`目录下。

:::

## 从源码构建SeaTunnel

### 下载源码

从源码构建SeaTunnel。下载源码的方式与下载二进制包的方式相同。
您可以从[下载页面](https://seatunnel.apache.org/download/)下载源码，或者从[GitHub仓库](https://github.com/apache/seatunnel/releases)克隆源码。

### 构建源码

```shell
cd seatunnel
sh ./mvnw clean install -DskipTests -Dskip.spotless=true
# 获取构建好的二进制包
cp seatunnel-dist/target/apache-seatunnel-2.3.10-bin.tar.gz /The-Path-You-Want-To-Copy

cd /The-Path-You-Want-To-Copy
tar -xzvf "apache-seatunnel-${version}-bin.tar.gz"
```

当从源码构建时，所有的连接器插件和一些必要的依赖（例如：mysql驱动）都包含在二进制包中。您可以直接使用连接器插件，而无需单独安装它们。

# 启动SeaTunnel

现在您已经下载了SeaTunnel二进制包和连接器插件。接下来，您可以选择不同的引擎选项来运行同步任务。

如果您使用Flink来运行同步任务，则无需部署SeaTunnel引擎服务集群。您可以参考[Flink 引擎快速开始](quick-start-flink.md)来运行您的同步任务。

如果您使用Spark来运行同步任务，则无需部署SeaTunnel引擎服务集群。您可以参考[Spark 引擎快速开始](quick-start-spark.md)来运行您的同步任务。

如果您使用内置的SeaTunnel引擎（Zeta）来运行任务，则需要先部署SeaTunnel引擎服务。请参考[SeaTunnel 引擎快速开始](quick-start-seatunnel-engine.md)。
