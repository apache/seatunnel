---
sidebar_position: 2
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 下载和制作安装包

## 步骤 1: 准备工作

在开始下载SeaTunnel之前，您需要确保您已经安装了SeaTunnel所需要的以下软件：

* 安装[Java](https://www.java.com/en/download/) (Java 11 或 Java 17) 以及设置 `JAVA_HOME`。

## 步骤 2: 下载 SeaTunnel

进入[SeaTunnel下载页面](https://seatunnel.apache.org/download)下载最新版本的发布版安装包`seatunnel-<version>-bin.tar.gz`

或者您也可以通过终端下载

```shell
export version="3.0.0"
wget "https://archive.apache.org/dist/seatunnel/${version}/apache-seatunnel-${version}-bin.tar.gz"
tar -xzvf "apache-seatunnel-${version}-bin.tar.gz"
```

## 步骤 3: 下载连接器插件

从2.2.0-beta版本开始，二进制包不再默认提供连接器依赖，因此在第一次使用它时，您需要执行以下命令来安装连接器：(当然，您也可以从 [Apache Maven Repository](https://repo.maven.apache.org/maven2/org/apache/seatunnel/) 手动下载连接器，然后将其移动至`connectors/seatunnel`目录下)。

```bash
sh bin/install-plugin.sh 3.0.0
```

如果您需要指定的连接器版本，以3.0.0为例，您需要执行如下命令

```bash
sh bin/install-plugin.sh 3.0.0
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

## 步骤 4: seatunnel-shade 相关jar包下载地址

从 3.0.0 版本开始，shade 模块已迁移到独立项目 [seatunnel-shade](https://github.com/apache/seatunnel-shade)。此变更带来以下好处：

- **独立版本管理**：shade 项目可以独立发版，不再受主项目发版周期限制
- **缩短构建时间**：从主项目中移除 shade 模块显著减少了编译时间
- **简化依赖管理**：主项目现在直接使用 Maven Central 上预构建的 shade jar

### 可用的 shade jar

| artifactId                   | version      | Maven                                                                                                      |
|------------------------------| ------------ |------------------------------------------------------------------------------------------------------------|
| seatunnel-shade-hadoop3-uber | 3.1.4-3.0.0   | [Maven](https://repo.maven.apache.org/maven2/org/apache/seatunnel/seatunnel-shade-hadoop3-uber/3.1.4-3.0.0/seatunnel-shade-hadoop3-uber-3.1.4-3.0.0.jar) |
| seatunnel-shade-hadoop-aws   | 3.1.4-3.0.0 | [Maven](https://repo.maven.apache.org/maven2/org/apache/seatunnel/seatunnel-shade-hadoop-aws/3.1.4-3.0.0/seatunnel-shade-hadoop-aws-3.1.4-3.0.0.jar)   |

### 重要说明

:::warning 重要
当需要变更 shade jar 时，必须先发版 [seatunnel-shade](https://github.com/apache/seatunnel-shade) 项目。发版流程如下：

1. 在 [seatunnel-shade](https://github.com/apache/seatunnel-shade) 项目中进行修改并合并
2. 发布 shade 项目的新版本
3. 在主 SeaTunnel 项目的 `pom.xml` 中更新 shade 版本
4. 然后发布主 SeaTunnel 项目

这确保 shade 依赖在主项目发版前已在 Maven Central 上可用。
:::
