# Download and install

## download

```bash
https://github.com/InterestingLab/seatunnel/releases
```

## Environmental preparation

### Prepare JDK1.8

`seatunnel` relies on the `JDK1.8` operating environment.

### Get Spark ready

`Seatunnel` relies on `Spark` . Before installing `seatunnel` , you need to prepare `Spark` . Please [download Spark](https://spark.apache.org/downloads.html) first, please select `Spark version >= 2.x.x`. After downloading and decompressing, you can submit the Spark `deploy-mode = local` mode task without any configuration. If you expect the task to run on the `Standalone cluster` or `Yarn cluster` or `Mesos cluster`, please refer to the Spark official website configuration document.

## Install seatunnel

Download the `seatunnel` installation package and unzip:

```bash
wget https://github.com/InterestingLab/seatunnel/releases/download/v<version>/seatunnel-<version>.zip -O seatunnel-<version>.zip
unzip seatunnel-<version>.zip
ln -s seatunnel-<version> seatunnel
```

There are no complicated installation and configuration steps. Please refer to [Quick Start](./quick-start.md) for how to use `seatunnel` , and refer to Configuration for [configuration](./configuration).
