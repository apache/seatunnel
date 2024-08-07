---

sidebar_position: 3
-------------------

# Set Up with Docker in local mode

## Zeta Engine

### Download

```shell
docker pull apache/seatunnel:<version_tag>
```

How to submit job in local mode

```shell
docker run --rm -it apache/seatunnel bash ./bin/seatunnel.sh -e local -c <CONFIG_FILE>


# eg: a fake source to console sink
docker run --rm -it apache/seatunnel bash ./bin/seatunnel.sh -e local -c config/v2.batch.config.template

```

### Build Image By Yourself

```Dockerfile
FROM openjdk:8

ARG VERSION
# Build from Source Code And Copy it into image
COPY ./target/apache-seatunnel-${VERSION}-bin.tar.gz /opt/

# Download From Internet
# Please Note this file only include fake/console connector, You'll need to download the other connectors manually
# wget -P /opt https://dlcdn.apache.org/seatunnel/2.3.6/apache-seatunnel-${VERSION}-bin.tar.gz

RUN cd /opt && \
    tar -zxvf apache-seatunnel-${VERSION}-bin.tar.gz && \
    mv apache-seatunnel-${VERSION} seatunnel && \
    rm apache-seatunnel-${VERSION}-bin.tar.gz

WORKDIR /opt/seatunnel
```

## Spark or Flink Engine

### Download And Install the connectors you needed

refer the step as Zeta Engine

### Mount Spark/Flink library

By default, Spark home is `/opt/spark`, Flink home is `/opt/flink`.
If you need run with spark/flink, you can mount the related library to `/opt/spark` or `/opt/flink`.

```shell
docker run \ 
 -v <SPARK_BINARY_PATH>:/opt/spark \
 -v <FLINK_BINARY_PATH>:/opt/flink \
  ...
```

Or you can change the `SPARK_HOME`, `FLINK_HOME` environment variable in Dockerfile and re-build your  and mount the spark/flink to related path.

```Dockerfile
FROM apache/seatunnel

ENV SPARK_HOME=<YOUR_CUSTOMIZATION_PATH>

...

```

```shell
docker run \ 
 -v <SPARK_BINARY_PATH>:<YOUR_CUSTOMIZATION_PATH> \
  ...
```

### Submit job

The command is different for different engines and different versions of the same engine, please choose the correct command.

- Spark

```shell
# spark2
docker run --rm -it apache/seatunnel bash ./bin/start-seatunnel-spark-2-connector-v2.sh -c config/v2.batch.config.template

# spark3
docker run --rm -it apache/seatunnel bash ./bin/start-seatunnel-spark-3-connector-v2.sh -c config/v2.batch.config.template
```

- Flink
  before you submit job, you need start flink cluster first.

```shell
# flink version between `1.12.x` and `1.14.x`
docker run --rm -it apache/seatunnel bash -c '<YOUR_FLINK_HOME>/bin/start-cluster.sh && ./bin/start-seatunnel-flink-13-connector-v2.sh -c config/v2.streaming.conf.template'
# flink version between `1.15.x` and `1.16.x`
docker run --rm -it apache/seatunnel bash -c '<YOUR_FLINK_HOME>/bin/start-cluster.sh && ./bin/start-seatunnel-flink-15-connector-v2.sh -c config/v2.streaming.conf.template'
```

