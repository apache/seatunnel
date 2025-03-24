---
sidebar_position: 3
---

# Set Up With Docker

## Set Up With Docker In Local Mode

### Zeta Engine

#### Download

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

#### Build Image By Yourself

Build from source code. The way of downloading the source code is the same as the way of downloading the binary package.
You can download the source code from the [download page](https://seatunnel.apache.org/download/) or clone the source code from the [GitHub repository](https://github.com/apache/seatunnel/releases)

##### Build With One Command
```shell
cd seatunnel
# Use already sett maven profile
sh ./mvnw -B clean install -Dmaven.test.skip=true -Dmaven.javadoc.skip=true -Dlicense.skipAddThirdParty=true -D"docker.build.skip"=false -D"docker.verify.skip"=false -D"docker.push.skip"=true -D"docker.tag"=2.3.10 -Dmaven.deploy.skip -D"skip.spotless"=true --no-snapshot-updates -Pdocker,seatunnel

# Check the docker image
docker images | grep apache/seatunnel
```

##### Build Step By Step
```shell
# Build binary package from source code
sh ./mvnw clean package -DskipTests -Dskip.spotless=true

# Build docker image
cd seatunnel-dist
docker build -f src/main/docker/Dockerfile --build-arg VERSION=2.3.10 -t apache/seatunnel:2.3.10 .

# If you build from dev branch, you should add SNAPSHOT suffix to the version
docker build -f src/main/docker/Dockerfile --build-arg VERSION=2.3.10-SNAPSHOT -t apache/seatunnel:2.3.10-SNAPSHOT .

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
# wget -P /opt https://dlcdn.apache.org/seatunnel/${VERSION}/apache-seatunnel-${VERSION}-bin.tar.gz

RUN cd /opt && \
    tar -zxvf apache-seatunnel-${VERSION}-bin.tar.gz && \
    mv apache-seatunnel-${VERSION} seatunnel && \
    rm apache-seatunnel-${VERSION}-bin.tar.gz && \
    sed -i 's/#rootLogger.appenderRef.consoleStdout.ref/rootLogger.appenderRef.consoleStdout.ref/' seatunnel/config/log4j2.properties && \
    sed -i 's/#rootLogger.appenderRef.consoleStderr.ref/rootLogger.appenderRef.consoleStderr.ref/' seatunnel/config/log4j2.properties && \
    sed -i 's/rootLogger.appenderRef.file.ref/#rootLogger.appenderRef.file.ref/' seatunnel/config/log4j2.properties && \    
    cp seatunnel/config/hazelcast-master.yaml seatunnel/config/hazelcast-worker.yaml

WORKDIR /opt/seatunnel
```

### Spark or Flink Engine


#### Mount Spark/Flink library

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



## Set Up With Docker In Cluster Mode

there has 2 ways to create cluster within docker.

### Use Docker Directly

#### create a network
```shell
docker network create seatunnel-network
```

#### start the nodes
- start master node
```shell
## start master and export 5801 port 
docker run -d --name seatunnel_master \
    --network seatunnel-network \
    --rm \
    -p 5801:5801 \
    apache/seatunnel \
    ./bin/seatunnel-cluster.sh -r master
```

- get created container ip
```shell
docker inspect seatunnel_master
```
run this command to get the pod ip.

- start worker node
```shell
# you need update yourself master container ip to `ST_DOCKER_MEMBER_LIST`
docker run -d --name seatunnel_worker_1 \
    --network seatunnel-network \
    --rm \
    -e ST_DOCKER_MEMBER_LIST=172.18.0.2:5801 \
    apache/seatunnel \
    ./bin/seatunnel-cluster.sh -r worker

## start worker2
# you need update yourself master container ip to `ST_DOCKER_MEMBER_LIST`
docker run -d --name seatunnel_worker_2 \
    --network seatunnel-network \
    --rm \
     -e ST_DOCKER_MEMBER_LIST=172.18.0.2:5801 \
    apache/seatunnel \
    ./bin/seatunnel-cluster.sh -r worker    

```

#### Scale your Cluster

run this command to start master node.
```shell
# you need update yourself master container ip to `ST_DOCKER_MEMBER_LIST`
docker run -d --name seatunnel_master \
    --network seatunnel-network \
    --rm \
    -e ST_DOCKER_MEMBER_LIST=172.18.0.2:5801 \
    apache/seatunnel \
    ./bin/seatunnel-cluster.sh -r master
```

run this command to start worker node.
```shell
# you need update yourself master container ip to `ST_DOCKER_MEMBER_LIST`
docker run -d --name seatunnel_worker_1 \
    --network seatunnel-network \
    --rm \
    -e ST_DOCKER_MEMBER_LIST=172.18.0.2:5801 \
    apache/seatunnel \
    ./bin/seatunnel-cluster.sh -r worker
```


### Use Docker-compose

> docker cluster mode is only support zeta engine.

The `docker-compose.yaml` file is :
```yaml
version: '3.8'

services:
  master:
    image: apache/seatunnel
    container_name: seatunnel_master
    environment:
      - ST_DOCKER_MEMBER_LIST=172.16.0.2,172.16.0.3,172.16.0.4    
    entrypoint: >
      /bin/sh -c "
      /opt/seatunnel/bin/seatunnel-cluster.sh -r master
      "    
    ports:
      - "5801:5801"  
    networks:
      seatunnel_network:
        ipv4_address: 172.16.0.2

  worker1:
    image: apache/seatunnel
    container_name: seatunnel_worker_1
    environment:
      - ST_DOCKER_MEMBER_LIST=172.16.0.2,172.16.0.3,172.16.0.4
    entrypoint: >
      /bin/sh -c "
      /opt/seatunnel/bin/seatunnel-cluster.sh -r worker
      " 
    depends_on:
      - master
    networks:
      seatunnel_network:
        ipv4_address: 172.16.0.3

  worker2:
    image: apache/seatunnel
    container_name: seatunnel_worker_2
    environment:
      - ST_DOCKER_MEMBER_LIST=172.16.0.2,172.16.0.3,172.16.0.4
    entrypoint: >
      /bin/sh -c "
      /opt/seatunnel/bin/seatunnel-cluster.sh -r worker
      " 
    depends_on:
      - master
    networks:
      seatunnel_network:
        ipv4_address: 172.16.0.4

networks:
  seatunnel_network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.16.0.0/24

```

run `docker-compose up -d` command to start the cluster.


You can run `docker logs -f seatunne_master`, `docker logs -f seatunnel_worker_1` to check the node log.
And when you call `http://localhost:5801/hazelcast/rest/maps/system-monitoring-information`, you will see there are 2 nodes as we excepted.

After that, you can use client or restapi to submit job to this cluster.

#### Scale your Cluster

If you want to increase cluster node, like add a new work node.

```yaml
version: '3.8'

services:
  master:
    image: apache/seatunnel
    container_name: seatunnel_master
    environment:
      - ST_DOCKER_MEMBER_LIST=172.16.0.2,172.16.0.3,172.16.0.4    
    entrypoint: >
      /bin/sh -c "
      /opt/seatunnel/bin/seatunnel-cluster.sh -r master
      "    
    ports:
      - "5801:5801"  
    networks:
      seatunnel_network:
        ipv4_address: 172.16.0.2

  worker1:
    image: apache/seatunnel
    container_name: seatunnel_worker_1
    environment:
      - ST_DOCKER_MEMBER_LIST=172.16.0.2,172.16.0.3,172.16.0.4
    entrypoint: >
      /bin/sh -c "
      /opt/seatunnel/bin/seatunnel-cluster.sh -r worker
      " 
    depends_on:
      - master
    networks:
      seatunnel_network:
        ipv4_address: 172.16.0.3

  worker2:
    image: apache/seatunnel
    container_name: seatunnel_worker_2
    environment:
      - ST_DOCKER_MEMBER_LIST=172.16.0.2,172.16.0.3,172.16.0.4
    entrypoint: >
      /bin/sh -c "
      /opt/seatunnel/bin/seatunnel-cluster.sh -r worker
      " 
    depends_on:
      - master
    networks:
      seatunnel_network:
        ipv4_address: 172.16.0.4
  ####
  ## add new worker node
  ####      
  worker3:
    image: apache/seatunnel
    container_name: seatunnel_worker_3
    environment:
      - ST_DOCKER_MEMBER_LIST=172.16.0.2,172.16.0.3,172.16.0.4,172.16.0.5 # add ip to here
    entrypoint: >
      /bin/sh -c "
      /opt/seatunnel/bin/seatunnel-cluster.sh -r worker
      " 
    depends_on:
      - master
    networks:
      seatunnel_network:
        ipv4_address: 172.16.0.5        # use a not used ip

networks:
  seatunnel_network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.16.0.0/24

```

and run `docker-compose up -d` command, the new worker node will start, and the current node won't restart.


### Job Operation on cluster

#### use docker as a client
- submit job :
```shell
# you need update yourself master container ip to `ST_DOCKER_MEMBER_LIST`
docker run --name seatunnel_client \
    --network seatunnel-network \
    -e ST_DOCKER_MEMBER_LIST=172.18.0.2:5801 \
    --rm \
    apache/seatunnel \
    ./bin/seatunnel.sh  -c config/v2.batch.config.template
```

- list job
```shell
# you need update yourself master container ip to `ST_DOCKER_MEMBER_LIST`
docker run --name seatunnel_client \
    --network seatunnel-network \
    -e ST_DOCKER_MEMBER_LIST=172.18.0.2:5801 \
    --rm \
    apache/seatunnel \
    ./bin/seatunnel.sh  -l
```

more command please refer [user-command](../../seatunnel-engine/user-command.md)



#### use rest api

please refer [Submit A Job](../../seatunnel-engine/rest-api-v2.md#submit-a-job)

