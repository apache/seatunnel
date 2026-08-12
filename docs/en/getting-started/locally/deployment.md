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
export version="3.0.0"
wget "https://archive.apache.org/dist/seatunnel/${version}/apache-seatunnel-${version}-bin.tar.gz"
tar -xzvf "apache-seatunnel-${version}-bin.tar.gz"
```

On Windows, download the `.zip` archive from the [SeaTunnel Download Page](https://seatunnel.apache.org/download), then extract it with File Explorer or PowerShell:

```powershell
$version = "3.0.0"
Invoke-WebRequest `
  "https://archive.apache.org/dist/seatunnel/$version/apache-seatunnel-$version-bin.zip" `
  -OutFile "apache-seatunnel-$version-bin.zip"
Expand-Archive "apache-seatunnel-$version-bin.zip" -DestinationPath .
Set-Location "apache-seatunnel-$version"
```

### Download The Connector Plugins

Starting from version 2.2.0-beta, the binary package no longer provides connector dependencies by default. Therefore, the first time you use it, you need to run the following command to install the connectors (Alternatively, you can manually download the connectors from the [Apache Maven Repository](https://repo.maven.apache.org/maven2/org/apache/seatunnel/) and move them to the `connectors/` directory. For versions before 2.3.5, place them in the `connectors/seatunnel` directory)：

```bash
sh bin/install-plugin.sh
```

On Windows, run the bundled batch script from the extracted directory. It uses the Maven Wrapper, so a separate Maven installation is not required:

```bat
cd apache-seatunnel-3.0.0
bin\install-plugin.cmd
```

To install connectors for a specific release, pass the version to the same script:

```bat
bin\install-plugin.cmd 3.0.0
```

If you need a specific connector version, taking 3.0.0 as an example, you need to execute the following command:

```bash
sh bin/install-plugin.sh 3.0.0
```

For released connector versions, `install-plugin.sh` downloads JARs and their checksums directly over HTTPS, so Maven is not required on Linux and macOS. This path requires `curl`, `mktemp`, and one of `sha512sum`, `sha1sum`, `shasum`, or `openssl`. The Windows `install-plugin.cmd` script continues to use the bundled Maven Wrapper. To use an HTTPS Maven-compatible mirror with `install-plugin.sh`, set `SEATUNNEL_MAVEN_REPOSITORY` to its base URL:

```bash
SEATUNNEL_MAVEN_REPOSITORY=https://repo.example.com/maven2 \
  sh bin/install-plugin.sh 3.0.0
```

The direct download path supports immutable release versions from repositories that publish `.sha512` or `.sha1` checksum files. `SNAPSHOT`, `LATEST`, `RELEASE`, and version ranges automatically use the bundled Maven wrapper because Maven metadata must be resolved. You can also set `SEATUNNEL_PLUGIN_DOWNLOAD_METHOD=maven` to preserve Maven `settings.xml` behavior such as mirrors, authenticated repositories, proxies, and custom TLS policies.

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

:::note Developer note

This local deployment guide assumes that you use an official binary release package. If you need to validate unreleased code, debug SeaTunnel source code, or prepare a custom distribution, see [Set Up Develop Environment](../../developer/setup.md).

:::

# Run SeaTunnel

Now you have downloaded the SeaTunnel binary package and the connector plugins. Next, you can choose different engine option to run synchronization tasks.

:::tip

If you are new to SeaTunnel, start with [Quick Start With SeaTunnel Engine](quick-start-seatunnel-engine.md).
It is the default engine and usually the shortest path to a successful first run.

:::

If you use Flink to run the synchronization task, there is no need to deploy the SeaTunnel Engine service cluster. You can refer to [Quick Start With Flink](quick-start-flink.md) to run your synchronization task.

If you use Spark to run the synchronization task, there is no need to deploy the SeaTunnel Engine service cluster. You can refer to [Quick Start With Spark](quick-start-spark.md) to run your synchronization task.

If you use the builtin SeaTunnel Engine (Zeta) to run tasks, you need to deploy the SeaTunnel Engine service first. Refer to [Quick Start With SeaTunnel Engine](quick-start-seatunnel-engine.md).
