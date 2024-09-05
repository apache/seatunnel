---
sidebar_position: 3
---

# Set Up With Docker In Local Mode

## Zeta Engine

### Download

```shell
docker pull apache/seatunnel:<version_tag>
```

How to submit job in local mode

```shell
# Run fake source to console sink
docker run --rm -it apache/seatunnel:<version_tag> ./bin/seatunnel.sh -m local -c config/v2.batch.config.template

# Run job with custom config file
docker run --rm -it -v /<The-Config-Directory-To-Mount>/:/config apache/seatunnel:<version_tag> ./bin/seatunnel.sh -m local -c /config/fake_to_console.conf

# Example
# If you config file is in /tmp/job/fake_to_console.conf
docker run --rm -it -v /tmp/job/:/config apache/seatunnel:<version_tag> ./bin/seatunnel.sh -m local -c /config/fake_to_console.conf

# Set JVM options when running
docker run --rm -it -v /tmp/job/:/config apache/seatunnel:<version_tag> ./bin/seatunnel.sh -DJvmOption="-Xms4G -Xmx4G" -m local -c /config/fake_to_console.conf
```

### Build Image By Yourself

Build from source code. The way of downloading the source code is the same as the way of downloading the binary package.
You can download the source code from the [download page](https://seatunnel.apache.org/download/) or clone the source code from the [GitHub repository](https://github.com/apache/seatunnel/releases)

#### Build With One Command
```shell
cd seatunnel
# Use already sett maven profile
sh ./mvnw -B clean install -Dmaven.test.skip=true -Dmaven.javadoc.skip=true -Dlicense.skipAddThirdParty=true -D"docker.build.skip"=false -D"docker.verify.skip"=false -D"docker.push.skip"=true -D"docker.tag"=2.3.8 -Dmaven.deploy.skip --no-snapshot-updates -Pdocker,seatunnel

# Check the docker image
docker images | grep apache/seatunnel
```

#### Build Step By Step
```shell
# Build binary package from source code
sh ./mvnw clean package -DskipTests -Dskip.spotless=true

# Build docker image
cd seatunnel-dist
docker build -f src/main/docker/Dockerfile --build-arg VERSION=2.3.8 -t apache/seatunnel:2.3.8 .

# If you build from dev branch, you should add SNAPSHOT suffix to the version
docker build -f src/main/docker/Dockerfile --build-arg VERSION=2.3.8-SNAPSHOT -t apache/seatunnel:2.3.8-SNAPSHOT .

# Check the docker image
docker images | grep apache/seatunnel
```

The Dockerfile is like this:
```dockerfile
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

```dockerfile
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

