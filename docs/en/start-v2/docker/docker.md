---

sidebar_position: 3
-------------------

# Set Up with Docker in local mode

## Zeta Engine

### Download

```shell
docker pull apache/seatunnel
```

How to submit job in local mode

```shell
docker run --rm -it apache/seatunnel bash ./bin/seatunnel.sh -e local -c <CONFIG_FILE>


# eg: a fake source to console sink
docker run --rm -it apache/seatunnel bash ./bin/seatunnel.sh -e local -c config/v2.batch.config.template

```

### Install the connectors you needed

We only provide the fake source and console sink connector in this image, if you need other connectors, you can follow the below steps to install the connector.

- Option 1: re-build your image
  Here is a example Dockerfile

```Dockerfile
FROM apache/seatunnel

# change the connector name and version. or maven url if you can't connector.  
RUN wget https://repo1.maven.org/maven2/org/apache/seatunnel/<CONNECTOR_NAME>/<VERSION>/<CONNECTOR_NAME>-<VERSION>.jar -P /opt/seatunnel/connectors/seatunel && \
    wget https://repo1.maven.org/maven2/org/apache/seatunnel/<CONNECTOR_NAME>/<VERSION>/<CONNECTOR_NAME>-<VERSION>.jar -P /opt/seatunnel/connectors/seatunel

# eg:
# RUN wget https://repo1.maven.org/maven2/org/apache/seatunnel/connector-kafka/2.3.0/connector-kafka-2.3.0.jar -P /opt/seatunnel/connectors/seatunel

...

```

```shell
docker run --rm -it <YOUR_CUSTOMIZED_IMAGE_NAME>:<TAG> bash ./bin/seatunnel.sh -e local -c <CONFIG_FILE>
```

- Option 2: download the connector jar manually, mount the lib in runtime.
  download the connector from [Apache Maven Repository](https://repo.maven.apache.org/maven2/org/apache/seatunnel/)

```shell
docker run \
--rm -it \
-v <YOUR_LOCAL_JAR_PATH>:/opt/seatunnel/connectors/seatunel \ 
apache/seatunnel \
bash ./bin/seatunnel.sh -e local -c <CONFIG_FILE>
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

