---

sidebar_position: 1
-------------------

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Deployment

## Step 1: Prepare the environment

Before you getting start the local run, you need to make sure you already have installed the following software which SeaTunnel required:

* [Java](https://www.java.com/en/download/) (Java 8 or 11, other versions greater than Java 8 can theoretically work as well) installed and `JAVA_HOME` set.

## Step 2: Download SeaTunnel

Enter the [seatunnel download page](https://seatunnel.apache.org/download) and download the latest version of distribute
package `seatunnel-<version>-bin.tar.gz`

Or you can download it by terminal

```shell
export version="2.3.2"
wget "https://archive.apache.org/dist/seatunnel/${version}/apache-seatunnel-${version}-bin.tar.gz"
tar -xzvf "apache-seatunnel-${version}-bin.tar.gz"
```

<!-- TODO: We should add example module as quick start which is no need for install Spark or Flink -->

## Step 3: Install connectors plugin

Since 2.2.0-beta, the binary package does not provide connector dependencies by default, so when using it for the first time, you need to execute the following command to install the connector: (Of course, you can also manually download the connector from [Apache Maven Repository](https://repo.maven.apache.org/maven2/org/apache/seatunnel/) to download, then manually move to the `connectors/seatunnel` directory).

```bash
sh bin/install-plugin.sh 2.3.2
```

If you need to specify the version of the connector, take 2.3.2 as an example, you need to execute

```bash
sh bin/install-plugin.sh 2.3.2
```

Usually you don't need all the connector plugins, so you can specify the plugins you need by configuring `config/plugin_config`, for example, you only need the `connector-console` plugin, then you can modify plugin.properties as

```plugin_config
--seatunnel-connectors--
connector-console
--end--
```

If you'd like to make a sample application to work properly, you need to add the following plugins

```plugin_config
--seatunnel-connectors--
connector-fake
connector-console
--end--
```

You can find out all supported connectors and corresponding plugin_config configuration names under `${SEATUNNEL_HOME}/connectors/plugins-mapping.properties`.

:::tip

If you'd like to install the connector plugin by manually downloading the connector, you need to pay special attention to the following

The connectors directory contains the following subdirectories, if they do not exist, you need to create them manually

```
flink
flink-sql
seatunnel
spark
```

If you'd like to install the V2 connector plugin manually, you only need to download the V2 connector plugin you need and put them in the seatunnel directory

:::

## What's More

For now, you are already deployment SeaTunnel complete. You can follow [Quick Start](quick-start-seatunnel-engine.md) to configure and run a data synchronization job.
