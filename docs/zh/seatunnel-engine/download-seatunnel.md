---
sidebar_position: 2
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 下载和制作安装包

## 步骤 1: 准备工作

在开始下载SeaTunnel之前，您需要确保您已经安装了SeaTunnel所需要的以下软件：

* 安装[Java](https://www.java.com/en/download/) (Java 8 或 11， 其他高于Java 8的版本理论上也可以工作) 以及设置 `JAVA_HOME`。

## 步骤 2: 下载 SeaTunnel

进入[SeaTunnel下载页面](https://seatunnel.apache.org/download)下载最新版本的发布版安装包`seatunnel-<version>-bin.tar.gz`

或者您也可以通过终端下载

```shell
export version="2.3.10"
wget "https://archive.apache.org/dist/seatunnel/${version}/apache-seatunnel-${version}-bin.tar.gz"
tar -xzvf "apache-seatunnel-${version}-bin.tar.gz"
```

## 步骤 3: 下载连接器插件

从2.2.0-beta版本开始，二进制包不再默认提供连接器依赖，因此在第一次使用它时，您需要执行以下命令来安装连接器：(当然，您也可以从 [Apache Maven Repository](https://repo.maven.apache.org/maven2/org/apache/seatunnel/) 手动下载连接器，然后将其移动至`connectors/seatunnel`目录下)。

```bash
sh bin/install-plugin.sh 2.3.10
```

如果您需要指定的连接器版本，以2.3.10为例，您需要执行如下命令

```bash
sh bin/install-plugin.sh 2.3.10
```

通常您并不需要所有的连接器插件，所以您可以通过配置`config/plugin_config`来指定您所需要的插件，例如，您只需要`connector-console`插件，那么您可以修改plugin.properties配置文件如下

```plugin_config
--seatunnel-connectors--
connector-console
--end--
```

如果您希望示例应用程序能正常工作，那么您需要添加以下插件

```plugin_config
--seatunnel-connectors--
connector-fake
connector-console
--end--
```

您可以在`${SEATUNNEL_HOME}/connectors/plugins-mapping.properties`下找到所有支持的连接器和相应的plugin_config配置名称。

:::tip 提示

如果您想通过手动下载连接器的方式来安装连接器插件，您只需要下载您所需要的连接器插件即可，并将它们放在`${SEATUNNEL_HOME}/connectors/`目录下

:::

现在你已经完成了SeaTunnel安装包的下载和连接器插件的下载。接下来，您可以根据您的需求选择不同的运行模式来运行或部署SeaTunnel。

如果你使用SeaTunnel自带的SeaTunnel Engine(Zeta)来运行任务，需要先部署SeaTunnel Engine服务。参考[SeaTunnel Engine(Zeta)服务部署](deployment.md)
