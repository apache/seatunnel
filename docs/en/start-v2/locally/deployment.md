---

sidebar_position: 2
-------------------

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Download and Make Installation Packages

## Step 1: Preparation

Before starting to download SeaTunnel, you need to ensure that you have installed the following software required by SeaTunnel:

* Install [Java](https://www.java.com/en/download/) (Java 8 or 11, and other versions higher than Java 8 can theoretically work) and set `JAVA_HOME`.

## Step 2: Download SeaTunnel

Visit the [SeaTunnel Download Page](https://seatunnel.apache.org/download) to download the latest binary package `seatunnel-<version>-bin.tar.gz`.

Or you can also download it through the terminal:

```shell
export version="2.3.7"
wget "https://archive.apache.org/dist/seatunnel/${version}/apache-seatunnel-${version}-bin.tar.gz"
tar -xzvf "apache-seatunnel-${version}-bin.tar.gz"
```

## Step 3: Download The Connector Plugins

Starting from the 2.2.0-beta version, the binary package no longer provides the connector dependencies by default. Therefore, when using it for the first time, you need to execute the following command to install the connectors (Of course, you can also manually download the connector from the [Apache Maven Repository](https://repo.maven.apache.org/maven2/org/apache/seatunnel/), and then move it to the `connectors/seatunnel` directory) :

```bash
sh bin/install-plugin.sh
```

If you need a specific connector version, taking 2.3.7 as an example, you need to execute the following command:

```bash
sh bin/install-plugin.sh 2.3.7
```

Usually you don't need all connector plugins, so you can specify the plugins you need through configuring `config/plugin_config`. For example, if you only need the `connector-console` plugin, you can modify the plugin.properties configuration file as follows:

```plugin_config
--seatunnel-connectors--
connector-console
--end--
```

If you want the example application to work properly, you need to add the following plugins.

```plugin_config
--seatunnel-connectors--
connector-fake
connector-console
--end--
```

You can find all supported connectors and the corresponding plugin_config configuration names under `${SEATUNNEL_HOME}/connectors/plugins-mapping.properties`.

:::tip Tip

If you want to install connector plugins by manually downloading connectors, you only need to download the related connector plugins and place them in the `${SEATUNNEL_HOME}/connectors/` directory.

:::

Now you have downloaded the SeaTunnel binary package and the connector plugins. Next, you can choose different engine option to run synchronization tasks.

If you use Flink to run the synchronization task, there is no need to deploy the SeaTunnel Engine service cluster. You can refer to [Quick Start of SeaTunnel Flink Engine](quick-start-flink.md) to run your synchronization task.

If you use Spark to run the synchronization task, there is no need to deploy the SeaTunnel Engine service cluster. You can refer to [Quick Start of SeaTunnel Spark Engine](quick-start-spark.md) to run your synchronization task.

If you use the builtin SeaTunnel Engine (Zeta) to run tasks, you need to deploy the SeaTunnel Engine service first. Refer to [Deployment of SeaTunnel Engine (Zeta) Service](quick-start-seatunnel-engine.md).
