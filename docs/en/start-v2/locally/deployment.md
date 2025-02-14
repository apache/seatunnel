---
sidebar_position: 2
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Deployment

## Preparation

Before starting to download SeaTunnel, you need to ensure that you have installed the following software required by SeaTunnel:

* Install [Java](https://www.java.com/en/download/) (Java 8 or 11, and other versions higher than Java 8 can theoretically work) and set `JAVA_HOME`.

## Download SeaTunnel Release Package

### Download The Binary Package

Visit the [SeaTunnel Download Page](https://seatunnel.apache.org/download) to download the latest binary package `seatunnel-<version>-bin.tar.gz`.

Or you can also download it through the terminal:

```shell
export version="2.3.10"
wget "https://archive.apache.org/dist/seatunnel/${version}/apache-seatunnel-${version}-bin.tar.gz"
tar -xzvf "apache-seatunnel-${version}-bin.tar.gz"
```

### Download The Connector Plugins

Starting from version 2.2.0-beta, the binary package no longer provides connector dependencies by default. Therefore, the first time you use it, you need to run the following command to install the connectors (Alternatively, you can manually download the connectors from the [Apache Maven Repository](https://repo.maven.apache.org/maven2/org/apache/seatunnel/) and move them to the `connectors/` directory. For versions before 2.3.5, place them in the `connectors/seatunnel` directory)：

```bash
sh bin/install-plugin.sh
```

If you need a specific connector version, taking 2.3.10 as an example, you need to execute the following command:

```bash
sh bin/install-plugin.sh 2.3.10
```

Typically, you do not need all the connector plugins. You can specify the required plugins by configuring `config/plugin_config`. For example, if you want the sample application to work properly, you will need the `connector-console` and `connector-fake` plugins. You can modify the `plugin_config` configuration file as follows:

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

## Build SeaTunnel From Source Code

### Download The Source Code

Build from source code. The way of downloading the source code is the same as the way of downloading the binary package.
You can download the source code from the [download page](https://seatunnel.apache.org/download/) or clone the source code from the [GitHub repository](https://github.com/apache/seatunnel/releases)

### Build The Source Code

```shell
cd seatunnel
sh ./mvnw clean install -DskipTests -Dskip.spotless=true
# get the binary package
cp seatunnel-dist/target/apache-seatunnel-2.3.10-bin.tar.gz /The-Path-You-Want-To-Copy

cd /The-Path-You-Want-To-Copy
tar -xzvf "apache-seatunnel-${version}-bin.tar.gz"
```

When built from the source code, all the connector plugins and some necessary dependencies (eg: mysql driver) are included in the binary package. You can directly use the connector plugins without the need to install them separately.

# Run SeaTunnel

Now you have downloaded the SeaTunnel binary package and the connector plugins. Next, you can choose different engine option to run synchronization tasks.

If you use Flink to run the synchronization task, there is no need to deploy the SeaTunnel Engine service cluster. You can refer to [Quick Start With Flink](quick-start-flink.md) to run your synchronization task.

If you use Spark to run the synchronization task, there is no need to deploy the SeaTunnel Engine service cluster. You can refer to [Quick Start With Spark](quick-start-spark.md) to run your synchronization task.

If you use the builtin SeaTunnel Engine (Zeta) to run tasks, you need to deploy the SeaTunnel Engine service first. Refer to [Quick Start With SeaTunnel Engine](quick-start-seatunnel-engine.md).
