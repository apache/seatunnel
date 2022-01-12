# Download and install

## Download

```bash
https://github.com/InterestingLab/seatunnel/releases
```

## Environmental preparation

### Prepare JDK1.8

seatunnel relies on the JDK1.8 runtime environment.

### Get Flink ready

Please [download Flink](https://flink.apache.org/downloads.html) first, please choose Flink version >= 1.9.0. The download is complete to [install flink](https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/deployment/resource-providers/standalone/overview)

### Install seatunnel

Download the seatunnel installation package and unzip

```bash
wget https://github.com/InterestingLab/seatunnel/releases/download/v<version>/seatunnel-<version>.zip -O seatunnel-<version>.zip
unzip seatunnel-<version>.zip
ln -s seatunnel-<version> seatunnel
```

Without any complicated installation and configuration steps, please refer to [Quick Start](./quick-start.md) for the usage of seatunnel, and refer to Configuration for [configuration](./configuration).

If you want to deploy `seatunnel` to run on `Flink Standalone/Yarn cluster` , please refer to [seatunnel deployment](./deployment.md)
