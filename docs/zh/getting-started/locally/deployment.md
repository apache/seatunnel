---
sidebar_position: 1
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 部署

## 准备工作

在开始本地运行前，您需要确保您已经安装了SeaTunnel所需要的以下软件：

* 安装[Java](https://www.java.com/en/download/)（标准 `-bin` 包使用 Java 8 或 11；**Spark 4.1 需要 JDK 17+**）并设置 `JAVA_HOME`。

## 下载 SeaTunnel 发行包

### 下载二进制包

进入[SeaTunnel下载页面](https://seatunnel.apache.org/download)下载最新版本：

- `apache-seatunnel-<version>-bin.tar.gz` — Spark 2.4 / 3.3、Flink 与 SeaTunnel Engine（JDK 8+）
- `apache-seatunnel-<version>-spark41-bin.tar.gz` — Spark 4.1 starter 与精简 connector 集合（**JDK 17+**）。详见 [SeaTunnel With Spark](../../engines/spark.md#spark-41-distribution)。

或者您也可以通过终端下载标准包：

```shell
export version="3.0.0"
wget "https://archive.apache.org/dist/seatunnel/${version}/apache-seatunnel-${version}-bin.tar.gz"
tar -xzvf "apache-seatunnel-${version}-bin.tar.gz"
```

### 下载连接器插件

从2.2.0-beta版本开始，二进制包不再默认提供连接器依赖，因此在第一次使用时，您需要执行以下命令来安装连接器：(当然，您也可以从 [Apache Maven Repository](https://repo.maven.apache.org/maven2/org/apache/seatunnel/) 手动下载连接器，然后将其移动至`connectors/`目录下，如果是2.3.5之前则需要放入`connectors/seatunnel`目录下)。

```bash
sh bin/install-plugin.sh
```

如果您需要指定的连接器版本，以3.0.0为例，您需要执行如下命令：

```bash
sh bin/install-plugin.sh 3.0.0
```

对于正式发布的连接器版本，`install-plugin.sh` 通过 HTTPS 直接下载 JAR 及其校验文件，因此 Linux 和 macOS 不需要 Maven。该方式需要 `curl`、`mktemp`，以及 `sha512sum`、`sha1sum`、`shasum` 或 `openssl` 中的任意一个。Windows 的 `install-plugin.cmd` 仍使用发行包内置的 Maven Wrapper。如果需要为 `install-plugin.sh` 使用 Maven 兼容的 HTTPS 镜像，可以通过 `SEATUNNEL_MAVEN_REPOSITORY` 指定仓库根地址：

```bash
SEATUNNEL_MAVEN_REPOSITORY=https://repo.example.com/maven2 \
  sh bin/install-plugin.sh 3.0.0
```

直接下载仅支持提供 `.sha512` 或 `.sha1` 校验文件的不可变正式版本。`SNAPSHOT`、`LATEST`、`RELEASE` 和版本范围需要解析 Maven 元数据，因此脚本会自动使用发行包内置的 Maven Wrapper。如果需要继续使用 Maven `settings.xml` 中的镜像、认证仓库、代理或自定义 TLS 策略，也可以设置 `SEATUNNEL_PLUGIN_DOWNLOAD_METHOD=maven`。

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

:::note 开发者说明

本地部署指南默认面向使用官方二进制发行包的用户。如果您需要验证未发布代码、调试 SeaTunnel 源码，或构建自定义发行包，请参考[搭建开发环境](../../developer/setup.md)。

:::

# 启动SeaTunnel

现在您已经下载了SeaTunnel二进制包和连接器插件。接下来，您可以选择不同的引擎选项来运行同步任务。

:::tip 提示

如果您是第一次使用 SeaTunnel，建议优先从 [SeaTunnel 引擎快速开始](quick-start-seatunnel-engine.md) 入手。
这是默认引擎，通常也是第一次跑通任务的最短路径。

:::

如果您使用Flink来运行同步任务，则无需部署SeaTunnel引擎服务集群。您可以参考[Flink 引擎快速开始](quick-start-flink.md)来运行您的同步任务。

如果您使用Spark来运行同步任务，则无需部署SeaTunnel引擎服务集群。您可以参考[Spark 引擎快速开始](quick-start-spark.md)来运行您的同步任务。

如果您使用内置的SeaTunnel引擎（Zeta）来运行任务，则需要先部署SeaTunnel引擎服务。请参考[SeaTunnel 引擎快速开始](quick-start-seatunnel-engine.md)。
