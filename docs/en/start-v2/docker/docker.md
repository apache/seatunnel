---

sidebar_position: 3
-------------------

# Set Up with Docker

### Download basic image

```shell
docker pull seatunnel-light:version
```

### Install the connector your needed

there has multiple ways to install the connector jar.
1. rebuild customized image with the basic image

```Dockerfile
FROM seatunnel-light:version

# change the connector name and version 
RUN wget https://repo1.maven.org/maven2/org/apache/seatunnel/<CONNECTOR_NAME>/<VERSION>/<CONNECTOR_NAME>-<VERSION>.jar -P /opt/seatunnel/connectors/seatunel && \
    wget https://repo1.maven.org/maven2/org/apache/seatunnel/<CONNECTOR_NAME>/<VERSION>/<CONNECTOR_NAME>-<VERSION>.jar -P /opt/seatunnel/connectors/seatunel

...

```

2. Mount the lib locally
   you can download the lib from maven to local manually, and mount this folder to container.

```shell
docker run -v <YOUR_LOCAL_JAR_PATH>:/opt/seatunnel/connectors/seatunel \ 
    ...
    seatunnel-basic-image:version
```

### Start The job

```shell
## batch process
docker run --rm -it seatunnel-basic-image:version bash ./bin/seatunnel.sh -e local --config config/v2.batch.config.template

# start streaming process 
docker run --rm -it seatunnel-basic-image:version bash ./bin/seatunnel.sh -e local --config config/v2.streaming.conf.template
```

