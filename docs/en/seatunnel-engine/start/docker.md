---

sidebar_position: 10
-------------------

# Docker quick start tutorial for Zeta

This tutorial is for you to quickly experience the Zeta engine through docker.

## Prepare

- [Docker](https://docs.docker.com/engine/install/) 1.13.1+

## Start Server

### Build project

Build the project with `./mvnw clean install -Dmaven.test.skip`.

### Enter `seatunnel-dist` to build docker image

```shell
SEATUNNEL_VERSION=2.3.1
cd seatunnel-dist
docker build -f ./src/main/docker/Dockerfile -t apache-seatunnel:$SEATUNNEL_VERSION-snapshot .
```

### Experience the Zeta Engine

```shell
docker run --rm  apache-seatunnel:$SEATUNNEL_VERSION-snapshot --config ./config/v2.streaming.conf.template -m local
```

