---

sidebar_position: 3
-------------------

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Set Up with Docker

This section provides a quick guide to using SeaTunnel with Docker.

## Prerequisites

We assume that you have a local installations of the following:
- [docker](https://docs.docker.com/)
- [A seatunnel that has already executed the install-plugin.sh](https://seatunnel.apache.org/docs/start-v2/locally/deployment/)

## Installation

### SeaTunnel docker image

## Step 1:

To run the image with SeaTunnel, first compress your local seatuunel into a compressed package:

```shell
sh apache-seatunnel-2.3.3/bin/install-plugin.sh
tar -czvf  apache-seatunnel-2.3.3-bin.tar.gz  apache-seatunnel-2.3.3
```

## Step 2:

Create a `Dockerfile`:

```Dockerfile
FROM openjdk:8

ENV SEATUNNEL_VERSION="2.3.3"
COPY /apache-seatunnel-${SEATUNNEL_VERSION}-bin.tar.gz /opt/apache-seatunnel-${SEATUNNEL_VERSION}-bin.tar.gz
WORKDIR /opt
RUN tar -xzvf apache-seatunnel-${SEATUNNEL_VERSION}-bin.tar.gz
RUN mv apache-seatunnel-${SEATUNNEL_VERSION} seatunnel
RUN rm -f /opt/apache-seatunnel-${SEATUNNEL_VERSION}-bin.tar.gz
WORKDIR /opt/seatunnel
ENTRYPOINT ["sh","-c"," bin/seatunnel.sh --config $config  -e local"]

```

## Step 3:

Then run the following commands to build the image:

```bash
docker build -t seatunnel:2.3.3 -f Dockerfile .
```

## Step 4:

You can start the container using the following command:

```bash
docker run   -e config="/container/path" -v /host/path:/container/path -d seatunnel:2.3.3
```

For example

```bash
docker run   -e config="/data/seatunnel.streaming.conf" -v /data/apache-seatunnel-2.3.3/config/v2.streaming.conf.template:/data/seatunnel.streaming.conf  -d  seatunnel:test
```

## Step 5:

Finally, you can use the following command to view the logs

```bash
docker logs CONTAINER ID
```

