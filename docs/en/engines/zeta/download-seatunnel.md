---
sidebar_position: 2
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Download And Make Installation Packages

## Step 1: Preparation

Before starting to download SeaTunnel, you need to ensure that you have installed the following software required by SeaTunnel:

* Install [Java](https://www.java.com/en/download/) (Java 8 or 11, and other versions higher than Java 8 can theoretically work) and set `JAVA_HOME`.

## Step 2: Download SeaTunnel

Go to the [Seatunnel Download Page](https://seatunnel.apache.org/download) to download the latest version of the release version installation package `seatunnel-<version>-bin.tar.gz`.

Or you can also download it through the terminal.

```shell
export version="3.0.0"
wget "https://archive.apache.org/dist/seatunnel/${version}/apache-seatunnel-${version}-bin.tar.gz"
tar -xzvf "apache-seatunnel-${version}-bin.tar.gz"
```

## Step 3: Download The Connector Plugin

Starting from the 2.2.0-beta version, the binary package no longer provides the connector dependency by default. Therefore, when using it for the first time, you need to execute the following command to install the connector: (Of course, you can also manually download the connector from the [Apache Maven Repository](https://repo.maven.apache.org/maven2/org/apache/seatunnel/), and then move it to the `connectors/seatunnel` directory).

```bash
sh bin/install-plugin.sh
```

If you need a specific connector version, taking 3.0.0 as an example, you need to execute the following command.

```bash
sh bin/install-plugin.sh 3.0.0
```

Usually you don't need all the connector plugins, so you can specify the plugins you need through configuring `config/plugin_config`, for example, if you only need the `connector-console` plugin, then you can modify the plugin.properties configuration file as follows.

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

If you want to install connector plugins by manually downloading connectors, you only need to download the connector plugins you need and place them in the `${SEATUNNEL_HOME}/connectors/` directory

:::

Now you have completed the download of the SeaTunnel installation package and the download of the connector plugin. Next, you can choose different running modes according to your needs to run or deploy SeaTunnel.

If you use the SeaTunnel Engine (Zeta) that comes with SeaTunnel to run tasks, you need to deploy the SeaTunnel Engine service first. Refer to [Deployment Of SeaTunnel Engine (Zeta) Service](deployment.md).

## Step 4: seatunnel-shade jar package download address

Starting from version 3.0.0, the shade modules have been moved to a separate project [seatunnel-shade](https://github.com/apache/seatunnel-shade). This change brings the following benefits:

- **Independent versioning**: The shade project can release versions independently, without being tied to the main SeaTunnel release cycle
- **Reduced build time**: Removing shade modules from the main project significantly reduces compilation time
- **Simplified dependencies**: The main project now uses pre-built shade jars from Maven Central

### Available shade jars

| artifactId                   | version      | Maven                                                                                                        |
|------------------------------|--------------|--------------------------------------------------------------------------------------------------------------|
| seatunnel-shade-hadoop3-uber | 3.1.4-3.0.0   | [Maven](https://repo.maven.apache.org/maven2/org/apache/seatunnel/seatunnel-shade-hadoop3-uber/3.1.4-3.0.0/seatunnel-shade-hadoop3-uber-3.1.4-3.0.0.jar) |
| seatunnel-shade-hadoop-aws   | 3.1.4-3.0.0 | [Maven](https://repo.maven.apache.org/maven2/org/apache/seatunnel/seatunnel-shade-hadoop-aws/3.1.4-3.0.0/seatunnel-shade-hadoop-aws-3.1.4-3.0.0.jar)   |

### Important notes

:::warning Important
When shade jar changes are required, the [seatunnel-shade](https://github.com/apache/seatunnel-shade) project must be released first. The release process is as follows:

1. Make and merge changes in the [seatunnel-shade](https://github.com/apache/seatunnel-shade) project
2. Release a new version of the shade project
3. Update the shade version in the main SeaTunnel project's `pom.xml`
4. Then release the main SeaTunnel project

This ensures that the shade dependencies are available in Maven Central before the main project release.
:::
