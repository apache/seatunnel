---
sidebar_position: 3
title: Download
---

# Download And Build Edge Agent Package

## Step 1: Preparation

Before installing Edge Agent, prepare the edge host:

* Java: install [Java](https://www.java.com/en/download/) 8 or 11 (newer LTS versions generally work) and set JAVA_HOME.
* Disk: writable directory for queue.sqlite-path (WAL and source positions) and logs.
* Network: outbound connectivity from the edge host to the EdgeSocket endpoint configured in output.endpoint.

Edge Agent does not require the full SeaTunnel Engine distribution on the same machine.

## Step 2: Download the binary package

Edge Agent is published as a standalone tarball, separate from `apache-seatunnel-<version>-bin.tar.gz`:

`apache-seatunnel-edge-agent-<version>-bin.tar.gz`

When available on the [SeaTunnel download page](https://seatunnel.apache.org/download), download and unpack:

```shell
export version="<seatunnel-version>"
wget "https://archive.apache.org/dist/seatunnel/${version}/apache-seatunnel-edge-agent-${version}-bin.tar.gz"
tar -xzf "apache-seatunnel-edge-agent-${version}-bin.tar.gz"
cd "apache-seatunnel-edge-agent-${version}"
```

After unpacking, the install root is the directory that contains bin/, config/, and starter/.

:::tip Install root variable

Throughout the deployment guides, EDGE_AGENT_HOME means this install root. The launcher scripts resolve paths from their own location; exporting EDGE_AGENT_HOME is recommended for operations and systemd units.

:::

## Step 3: Build from source

From the SeaTunnel source repository:

```shell
git clone https://github.com/apache/seatunnel.git
cd seatunnel
./mvnw clean package -pl seatunnel-dist -am -DskipTests -Dskip.spotless=true
```

Binary artifact (after a successful build):

```text
seatunnel-dist/target/apache-seatunnel-edge-agent-<version>-bin.tar.gz
```

Copy the tarball to the edge host and unpack as in Step 2.

:::note No connector plugin install step

Unlike the main SeaTunnel distribution, the Edge Agent package does not use bin/install-plugin.sh. Built-in input, transport, and payload plugins are bundled in seatunnel-edge-agent-starter.jar.

:::

## Step 4: Verify package layout

```text
apache-seatunnel-edge-agent-<version>/
  bin/
    seatunnel-edge-agent.sh
    seatunnel-edge-agent.cmd
  config/
    agent.yaml          # sample configuration
    log4j2.properties
  starter/
    seatunnel-edge-agent-starter.jar
    logging/            # logging dependencies
  README.md
```

Extension (advanced): the default tarball has no lib/ directory. Custom plugin JARs may be added under lib/ only if you build and ship extensions yourself.

## Next steps

* [Quick Start](quick-start.md)
* [Deployment Guide](deployment-guide.md)
