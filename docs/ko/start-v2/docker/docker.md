---
sidebar_position: 3
---

# Docker로 설정하기

## 로컬 모드에서 Docker 사용

### Zeta 엔진

#### 이미지 다운로드

```shell
docker pull apache/seatunnel:<version_tag>
```

로컬 모드에서 작업 제출하기

```shell
# Fake Source → Console Sink 예제 실행
docker run --rm -it apache/seatunnel:<version_tag> ./bin/seatunnel.sh -m local -c config/v2.batch.config.template

# 사용자 정의 설정 파일로 실행
docker run --rm -it -v /<The-Config-Directory-To-Mount>/:/config apache/seatunnel:<version_tag> ./bin/seatunnel.sh -m local -c /config/fake_to_console.conf

# 예시
# 설정 파일이 /tmp/job/fake_to_console.conf 에 있을 때
docker run --rm -it -v /tmp/job/:/config apache/seatunnel:<version_tag> ./bin/seatunnel.sh -m local -c /config/fake_to_console.conf

# 실행 시 JVM 옵션 지정
docker run --rm -it -v /tmp/job/:/config apache/seatunnel:<version_tag> ./bin/seatunnel.sh -DJvmOption="-Xms4G -Xmx4G" -m local -c /config/fake_to_console.conf
```

#### 이미지를 직접 빌드하기

소스 코드에서 직접 이미지를 빌드할 수 있습니다. 소스 코드 다운로드 방법은 바이너리 패키지를 받을 때와 같습니다.
[다운로드 페이지](https://seatunnel.apache.org/download/)에서 압축 파일을 받거나 [GitHub 저장소](https://github.com/apache/seatunnel/releases)를 클론하세요.

##### 원커맨드 빌드
```shell
cd seatunnel
# 미리 정의된 Maven 프로필 사용
sh ./mvnw -B clean install -Dmaven.test.skip=true -Dmaven.javadoc.skip=true -Dlicense.skipAddThirdParty=true -D"docker.build.skip"=false -D"docker.verify.skip"=false -D"docker.push.skip"=true -D"docker.tag"=2.3.13 -Dmaven.deploy.skip -D"skip.spotless"=true --no-snapshot-updates -Pdocker,seatunnel

# 생성된 Docker 이미지 확인
docker images | grep apache/seatunnel
```

##### 단계별 빌드
```shell
# 소스 코드에서 바이너리 패키지 빌드
sh ./mvnw clean package -DskipTests -Dskip.spotless=true

# Docker 이미지 빌드
cd seatunnel-dist
docker build -f src/main/docker/Dockerfile --build-arg VERSION=2.3.13 -t apache/seatunnel:2.3.13 .

# dev 브랜치에서 빌드한 경우 SNAPSHOT 접미사를 붙이세요
docker build -f src/main/docker/Dockerfile --build-arg VERSION=2.3.13-SNAPSHOT -t apache/seatunnel:2.3.13-SNAPSHOT .

# 생성된 Docker 이미지 확인
docker images | grep apache/seatunnel
```

Dockerfile 예시는 다음과 같습니다.
```dockerfile
FROM openjdk:8

ARG VERSION
# 소스 코드로 빌드한 산출물을 이미지에 복사
COPY ./target/apache-seatunnel-${VERSION}-bin.tar.gz /opt/

# 인터넷에서 직접 다운로드하는 방법
# 참고: 이 파일은 fake/console 커넥터만 포함하므로 다른 커넥터는 수동으로 받아야 합니다.
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

### Spark 또는 Flink 엔진

#### Spark/Flink 라이브러리 마운트

기본값으로 Spark 홈은 `/opt/spark`, Flink 홈은 `/opt/flink`입니다.
Spark 또는 Flink 엔진을 사용하려면 해당 바이너리를 `/opt/spark` 또는 `/opt/flink`에 마운트하세요.

```shell
docker run \
 -v <SPARK_BINARY_PATH>:/opt/spark \
 -v <FLINK_BINARY_PATH>:/opt/flink \
  ...
```

또는 Dockerfile에서 `SPARK_HOME`, `FLINK_HOME` 환경 변수를 원하는 경로로 바꾼 뒤 이미지를 다시 빌드하고, 해당 경로에 Spark/Flink를 마운트할 수 있습니다.

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

### 작업 제출

엔진 종류와 버전에 따라 명령이 다르므로 상황에 맞는 명령을 선택하세요.

- Spark

```shell
# Spark 2
docker run --rm -it apache/seatunnel bash ./bin/start-seatunnel-spark-2-connector-v2.sh -c config/v2.batch.config.template

# Spark 3
docker run --rm -it apache/seatunnel bash ./bin/start-seatunnel-spark-3-connector-v2.sh -c config/v2.batch.config.template
```

- Flink
  작업을 제출하기 전에 Flink 클러스터를 먼저 기동해야 합니다.

```shell
# Flink 1.12.x ~ 1.14.x 버전
docker run --rm -it apache/seatunnel bash -c '<YOUR_FLINK_HOME>/bin/start-cluster.sh && ./bin/start-seatunnel-flink-13-connector-v2.sh -c config/v2.streaming.conf.template'
# Flink 1.15.x ~ 1.16.x 버전
docker run --rm -it apache/seatunnel bash -c '<YOUR_FLINK_HOME>/bin/start-cluster.sh && ./bin/start-seatunnel-flink-15-connector-v2.sh -c config/v2.streaming.conf.template'
```

## 클러스터 모드에서 Docker 사용

Docker로 클러스터를 구성하는 방법은 두 가지가 있습니다.

### Docker 명령으로 직접 구성

#### 네트워크 생성
```shell
docker network create seatunnel-network
```

#### 노드 기동
- 마스터 노드 시작
```shell
## 마스터를 기동하고 5801 포트를 노출
docker run -d --name seatunnel_master \
    --network seatunnel-network \
    --rm \
    -p 5801:5801 \
    apache/seatunnel \
    ./bin/seatunnel-cluster.sh -r master
```

- 생성된 컨테이너 IP 확인
```shell
docker inspect seatunnel_master
```
위 명령으로 컨테이너 IP를 확인하세요.

- 워커 노드 시작
```shell
# `ST_DOCKER_MEMBER_LIST`에 마스터 컨테이너 IP를 입력해야 합니다.
docker run -d --name seatunnel_worker_1 \
    --network seatunnel-network \
    --rm \
    -e ST_DOCKER_MEMBER_LIST=172.18.0.2:5801 \
    apache/seatunnel \
    ./bin/seatunnel-cluster.sh -r worker

## 두 번째 워커 시작
# `ST_DOCKER_MEMBER_LIST`에 마스터 컨테이너 IP를 입력해야 합니다.
docker run -d --name seatunnel_worker_2 \
    --network seatunnel-network \
    --rm \
    -e ST_DOCKER_MEMBER_LIST=172.18.0.2:5801 \
    apache/seatunnel \
    ./bin/seatunnel-cluster.sh -r worker
```

#### 클러스터 확장

다음 명령으로 마스터 노드를 다시 시작할 수 있습니다.
```shell
# `ST_DOCKER_MEMBER_LIST`에 마스터 컨테이너 IP를 입력해야 합니다.
docker run -d --name seatunnel_master \
    --network seatunnel-network \
    --rm \
    -e ST_DOCKER_MEMBER_LIST=172.18.0.2:5801 \
    apache/seatunnel \
    ./bin/seatunnel-cluster.sh -r master
```

워커 노드를 추가할 때는 아래 명령을 실행하세요.
```shell
# `ST_DOCKER_MEMBER_LIST`에 마스터 컨테이너 IP를 입력해야 합니다.
docker run -d --name seatunnel_worker_1 \
    --network seatunnel-network \
    --rm \
    -e ST_DOCKER_MEMBER_LIST=172.18.0.2:5801 \
    apache/seatunnel \
    ./bin/seatunnel-cluster.sh -r worker
```

### Docker Compose 사용

> Docker 기반 클러스터 모드는 Zeta 엔진만 지원합니다.

`docker-compose.yaml` 예시는 다음과 같습니다.
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

`docker-compose up -d` 명령으로 클러스터를 시작합니다.

`docker logs -f seatunnel_master`, `docker logs -f seatunnel_worker_1` 명령으로 각 노드 로그를 확인할 수 있습니다.
또한 `http://localhost:5801/hazelcast/rest/maps/system-monitoring-information`에 접속하면 기대한 대로 두 개의 노드가 표시됩니다.

이후에는 클라이언트나 REST API를 통해 이 클러스터에 작업을 제출할 수 있습니다.

#### 클러스터 확장

새 워커 노드를 추가하려면 아래와 같이 설정을 확장하세요.

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
  ## 새 워커 노드 추가
  ####
  worker3:
    image: apache/seatunnel
    container_name: seatunnel_worker_3
    environment:
      - ST_DOCKER_MEMBER_LIST=172.16.0.2,172.16.0.3,172.16.0.4,172.16.0.5 # 새 IP 추가
    entrypoint: >
      /bin/sh -c "
      /opt/seatunnel/bin/seatunnel-cluster.sh -r worker
      "
    depends_on:
      - master
    networks:
      seatunnel_network:
        ipv4_address: 172.16.0.5        # 사용하지 않은 IP 지정

networks:
  seatunnel_network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.16.0.0/24

```

이 상태에서 `docker-compose up -d` 명령을 실행하면 기존 노드를 재시작하지 않고 새 워커 노드가 추가됩니다.

### 클러스터에서 작업 조작

#### Docker를 클라이언트로 사용
- 작업 제출
```shell
# `ST_DOCKER_MEMBER_LIST`에 마스터 컨테이너 IP를 입력해야 합니다.
docker run --name seatunnel_client \
    --network seatunnel-network \
    -e ST_DOCKER_MEMBER_LIST=172.18.0.2:5801 \
    --rm \
    apache/seatunnel \
    ./bin/seatunnel.sh -c config/v2.batch.config.template
```

- 작업 목록 확인
```shell
# `ST_DOCKER_MEMBER_LIST`에 마스터 컨테이너 IP를 입력해야 합니다.
docker run --name seatunnel_client \
    --network seatunnel-network \
    -e ST_DOCKER_MEMBER_LIST=172.18.0.2:5801 \
    --rm \
    apache/seatunnel \
    ./bin/seatunnel.sh -l
```

추가 명령은 [user-command](../../seatunnel-engine/user-command.md) 문서를 참고하세요.
