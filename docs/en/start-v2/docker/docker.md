---

sidebar_position: 3
-------------------

# Set Up with Docker in local mode

## Zeta Engine

### 1. Download basic image

```shell
docker pull seatunnel-basic-image:version
```

### 2. Install the connectors you needed

there has multiple ways to install the connector jar.

a. rebuild customized image with the basic image

```Dockerfile
FROM seatunnel-basic-image:<version>

# change the connector name and version 
RUN wget https://repo1.maven.org/maven2/org/apache/seatunnel/<CONNECTOR_NAME>/<VERSION>/<CONNECTOR_NAME>-<VERSION>.jar -P /opt/seatunnel/connectors/seatunel && \
    wget https://repo1.maven.org/maven2/org/apache/seatunnel/<CONNECTOR_NAME>/<VERSION>/<CONNECTOR_NAME>-<VERSION>.jar -P /opt/seatunnel/connectors/seatunel

# eg:
# RUN wget https://repo1.maven.org/maven2/org/apache/seatunnel/connector-kafka/2.3.0/connector-kafka-2.3.0.jar -P /opt/seatunnel/connectors/seatunel
...

```

b. Mount the lib locally
you can download the lib from maven to local manually, and mount this folder to container.

```shell
docker run -v <YOUR_LOCAL_JAR_PATH>:/opt/seatunnel/connectors/seatunel \ 
    ...
    seatunnel-basic-image:version
```

### 3. Start The job

```shell
# batch process
docker run --rm -it seatunnel-basic-image:version bash ./bin/seatunnel.sh -e local -c config/v2.batch.config.template

# start streaming process 
docker run --rm -it seatunnel-basic-image:version bash ./bin/seatunnel.sh -e local -c config/v2.streaming.conf.template
```

## Spark or Flink Engine

### 1. Download basic image

refer the step as Zeta Engine

### 2. Install the connectors you needed

refer the step as Zeta Engine

### 3. Mount Spark/Flink library

By default, Spark home is `/opt/spark`, Flink home is `/opt/flink`.
If you need run with spark/flink, you can mount the related library to `/opt/spark` or `/opt/spark`.

```shell
docker run \ 
 -v <SPARK_BINARY_PATH>:/opt/spark \
  ...
```

Or you can change the `SPARK_HOME`, `FLINK_HOME` environment variable in Dockerfile and mount the spark/flink to related path.

```Dockerfile
FROM seatunnel-basic-image:version

ENV SPARK_HOME=<YOUR_CUSTOMIZATION_PATH>

...

```

```shell
docker run \ 
 -v <SPARK_BINARY_PATH>:<YOUR_CUSTOMIZATION_PATH> \
  ...
```

### Start The job

```shell
# spark2
docker run --rm -it seatunnel-basic-image:version bash ./bin/start-seatunnel-spark-2-connector-v2.sh -c config/v2.batch.config.template

# spark3
docker run --rm -it seatunnel-basic-image:version bash ./bin/start-seatunnel-spark-3-connector-v2.sh -c config/v2.batch.config.template

# flink version between `1.12.x` and `1.14.x`
docker run --rm -it seatunnel-basic-image:version bash ./bin/start-seatunnel-flink-13-connector-v2.sh -c config/v2.streaming.conf.template
# flink version between `1.15.x` and `1.16.x`
docker run --rm -it seatunnel-basic-image:version bash ./bin/start-seatunnel-flink-15-connector-v2.sh -c config/v2.streaming.conf.template
```

