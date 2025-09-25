---
sidebar_position: 6
---

# 분리형 클러스터 모드 배포

분리형 모드에서는 SeaTunnel Engine의 Master 서비스와 Worker 서비스가 각각 독립 프로세스로 실행됩니다. Master 노드는 잡 스케줄링, REST API, 작업 제출 등 제어 기능만 담당하며 Imap 데이터도 Master에만 저장됩니다. Worker 노드는 작업 실행만 담당하며 마스터 선출에 참여하지 않고 Imap 데이터를 보관하지도 않습니다.

여러 Master 노드 중 동시에 동작하는 것은 하나뿐이며, 나머지는 대기 상태입니다. 현재 Master가 장애를 일으키거나 하트비트가 타임아웃되면 다른 Master 노드 중 하나가 자동으로 승격됩니다.

이 방식이 가장 권장되는 운영 방법입니다. Master 부하가 매우 낮아지고, 스케줄링·장애 복구 모니터링·REST API 제공 등에 더 많은 리소스를 활용할 수 있어 안정성이 높아집니다. Worker는 Imap 데이터를 저장하지 않으므로, Worker 부하나 장애가 발생해도 Imap 재분배가 일어나지 않습니다.

## 1. 다운로드

[SeaTunnel 설치 패키지 다운로드 및 생성](download-seatunnel.md)

## 2. SEATUNNEL_HOME 설정

`/etc/profile.d/seatunnel.sh` 파일을 추가해 `SEATUNNEL_HOME`을 설정하세요.

```
export SEATUNNEL_HOME=${seatunnel 설치 경로}
export PATH=$PATH:$SEATUNNEL_HOME/bin
```

## 3. JVM 옵션 설정

Master/Worker 각각 전용 옵션 파일을 사용합니다.

- Master: `$SEATUNNEL_HOME/config/jvm_master_options`
- Worker: `$SEATUNNEL_HOME/config/jvm_worker_options`

```shell
# JVM Heap
-Xms2g
-Xmx2g

# JVM Dump
-XX:+HeapDumpOnOutOfMemoryError
-XX:HeapDumpPath=/tmp/seatunnel/dump/zeta-server

# Metaspace
-XX:MaxMetaspaceSize=2g

# G1GC
-XX:+UseG1GC
```

## 4. SeaTunnel Engine 설정

주요 설정은 `seatunnel.yaml`에 정의합니다.

### 4.1 Imap 백업 수 설정 (Worker에는 적용되지 않음)

SeaTunnel Engine은 [Hazelcast IMDG](https://docs.hazelcast.com/imdg/4.1/) 위에서 클러스터를 관리하며, 잡 상태는 [Hazelcast IMap](https://docs.hazelcast.com/imdg/4.1/data-structures/map)에 저장됩니다. 데이터는 노드 전체에 분산되며, 파티션별 동기 백업 수(`backup-count`)를 지정할 수 있습니다. 권장 값은 `max(1, min(5, N/2))` (N=노드 수)입니다.

```yaml
seatunnel:
  engine:
    backup-count: 1
```

:::tip
분리형 모드에서 Worker는 Imap 데이터를 저장하지 않으므로 Worker의 `backup-count` 설정은 무시됩니다. Master와 Worker를 같은 머신에서 실행해 `seatunnel.yaml`을 공유해도 Worker는 해당 설정을 읽지 않습니다.
:::

### 4.2 슬롯 설정 (Master에는 적용되지 않음)

슬롯 수는 동시에 실행 가능한 태스크 그룹 수를 결정합니다. 기본값은 동적 슬롯(`dynamic-slot: true`)이며 제한이 없습니다. 정적 슬롯으로 설정하려면 CPU 코어 수의 2배 정도를 권장합니다.

```yaml
seatunnel:
  engine:
    slot-service:
      dynamic-slot: true
```

```yaml
seatunnel:
  engine:
    slot-service:
      dynamic-slot: false
      slot-num: 20
```

:::tip
Master는 작업을 실행하지 않으므로 Slot 서비스를 시작하지 않습니다. Master/Worker가 같은 설정 파일을 공유해도 Master는 `slot-service` 설정을 무시합니다.
:::

### 4.3 체크포인트 관리자 (Worker에는 적용되지 않음)

- **interval**: 체크포인트 간격(ms)
- **timeout**: 제한 시간. 초과 시 실패 처리
- **min-pause**: 연속 체크포인트 사이 최소 간격(ms)

```yaml
seatunnel:
  engine:
    backup-count: 1
    print-execution-info-interval: 10
    slot-service:
      dynamic-slot: true
    checkpoint:
      interval: 300000
      timeout: 10000
      min-pause: 5000
```

체크포인트는 장애 복구 시 상태 정보를 복원하는 메커니즘입니다. 다중 노드 환경에서는 공유/분산 스토리지를 사용해야 하며, 자세한 내용은 [Checkpoint Storage](checkpoint-storage.md)를 참고하세요.

:::tip
체크포인트 설정은 Master만 읽으며 Worker는 무시합니다.
:::

### 4.4 완료된 잡 정보 만료 설정

`history-job-expire-minutes`(기본 1440분)로 완료된 잡 정보 보관 기간을 조정해 메모리 사용을 관리할 수 있습니다.

```yaml
seatunnel:
  engine:
    history-job-expire-minutes: 1440
```

### 4.5 클래스 로더 캐시 모드

메타스페이스 누수 방지를 위한 설정으로, 활성화 시 잡 종료 후에도 클래스 로더를 유지해 재사용합니다. 기본값은 true입니다.

```yaml
seatunnel:
  engine:
    classloader-cache-mode: true
```

### 4.6 IMap 영속화 설정 (Worker에는 적용되지 않음)

:::tip
분리형 모드에서는 Master만 Imap 데이터를 저장하므로 Worker는 이 설정을 읽지 않습니다.
:::

IMap 상태를 외부 스토리지(HDFS/OSS/로컬 파일 등)에 저장하면 전체 노드가 중단되어도 데이터를 보존할 수 있습니다.

```yaml
map:
  engine*:
    map-store:
      enabled: true
      initial-mode: EAGER
      factory-class-name: org.apache.seatunnel.engine.server.persistence.FileMapStoreFactory
      properties:
        type: hdfs
        namespace: /tmp/seatunnel/imap
        clusterName: seatunnel-cluster
        storage.type: hdfs
        fs.defaultFS: hdfs://localhost:9000
```

단일 노드·로컬 파일:

```yaml
map:
  engine*:
    map-store:
      enabled: true
      initial-mode: EAGER
      factory-class-name: org.apache.seatunnel.engine.server.persistence.FileMapStoreFactory
      properties:
        type: hdfs
        namespace: /tmp/seatunnel/imap
        clusterName: seatunnel-cluster
        storage.type: hdfs
        fs.defaultFS: file:///
```

OSS 사용:

```yaml
map:
  engine*:
    map-store:
      enabled: true
      initial-mode: EAGER
      factory-class-name: org.apache.seatunnel.engine.server.persistence.FileMapStoreFactory
      properties:
        type: hdfs
        namespace: /tmp/seatunnel/imap
        clusterName: seatunnel-cluster
        storage.type: oss
        block.size: block size(bytes)
        oss.bucket: oss://bucket name/
        fs.oss.accessKeyId: OSS access key id
        fs.oss.accessKeySecret: OSS access key secret
        fs.oss.endpoint: OSS endpoint
```

OSS 사용 시 다음 JAR가 `lib`에 있어야 합니다.
```
aliyun-sdk-oss-3.13.2.jar
hadoop-aliyun-3.3.6.jar
jdom2-2.0.6.jar
netty-buffer-4.1.89.Final.jar
netty-common-4.1.89.Final.jar
seatunnel-hadoop3-3.1.4-uber.jar
```

### 4.7 작업 스케줄 전략

```yaml
seatunnel:
  engine:
    job-schedule-strategy: WAIT
```

- `WAIT`: 자원 확보까지 대기
- `REJECT`: 작업 거부 (기본값)

`dynamic-slot: true`인 경우 `WAIT` 설정은 강제로 `REJECT`로 변경됩니다.

### 4.8 코디네이터 서비스

JobMaster 생성 전 단계(LogicalDag→ExecutionDag→PhysicalDag)와 스케줄링/모니터링을 담당합니다.

```yaml
coordinator-service:
  core-thread-num: 30
  max-thread-num: 1000
```

### 4.9 잡 메트릭 파티션 수 (Worker에는 적용되지 않음)

`job-metrics-partition-count`로 Hazelcast IMap에서 메트릭을 저장할 파티션 수를 지정합니다. 값이 클수록 동시 업데이트 경합을 줄일 수 있으나, 너무 크면 오버헤드가 증가합니다. 잡 시작 전 설정하고, 변경 후 Seatunnel 재시작을 권장합니다.

```yaml
seatunnel:
  engine:
    job-metrics-partition-count: 4
```

## 5. 네트워크 서비스 설정

분리형 모드에서는 Master와 Worker가 각각 다른 설정 파일(`hazelcast-master.yaml`, `hazelcast-worker.yaml`)을 사용합니다.

### 5.1 cluster-name

노드 간 클러스터 여부는 `cluster-name`으로 판별합니다.

### 5.2 네트워크

TCP 기반 구성을 권장합니다. Master와 Worker는 서로 다른 포트를 사용해야 합니다.

`hazelcast-master.yaml`

```yaml
hazelcast:
  cluster-name: seatunnel
  network:
    rest-api:
      enabled: true
      endpoint-groups:
        CLUSTER_WRITE:
          enabled: true
        DATA:
          enabled: true
    join:
      tcp-ip:
        enabled: true
        member-list:
          - master-node-1:5801
          - master-node-2:5801
          - worker-node-1:5802
          - worker-node-2:5802
    port:
      auto-increment: false
      port: 5801
  properties:
    hazelcast.heartbeat.failuredetector.type: phi-accrual
    hazelcast.heartbeat.interval.seconds: 2
    hazelcast.max.no.heartbeat.seconds: 180
    hazelcast.heartbeat.phiaccrual.failuredetector.threshold: 10
    hazelcast.heartbeat.phiaccrual.failuredetector.sample.size: 200
    hazelcast.heartbeat.phiaccrual.failuredetector.min.std.dev.millis: 100
```

`hazelcast-worker.yaml`

```yaml
hazelcast:
  cluster-name: seatunnel
  network:
    join:
      tcp-ip:
        enabled: true
        member-list:
          - master-node-1:5801
          - master-node-2:5801
          - worker-node-1:5802
          - worker-node-2:5802
    port:
      auto-increment: false
      port: 5802
  properties:
    hazelcast.heartbeat.failuredetector.type: phi-accrual
    hazelcast.heartbeat.interval.seconds: 2
    hazelcast.max.no.heartbeat.seconds: 180
    hazelcast.heartbeat.phiaccrual.failuredetector.threshold: 10
    hazelcast.heartbeat.phiaccrual.failuredetector.sample.size: 200
    hazelcast.heartbeat.phiaccrual.failuredetector.min.std.dev.millis: 100
```

추가 디스커버리 방식은 [Hazelcast Network](https://docs.hazelcast.com/imdg/4.1/clusters/setting-up-clusters)를 참고하세요.

## 6. Master 노드 기동

```shell
mkdir -p $SEATUNNEL_HOME/logs
./bin/seatunnel-cluster.sh -d -r master
```

로그: `$SEATUNNEL_HOME/logs/seatunnel-engine-master.log`

## 7. Worker 노드 기동

```shell
mkdir -p $SEATUNNEL_HOME/logs
./bin/seatunnel-cluster.sh -d -r worker
```

로그: `$SEATUNNEL_HOME/logs/seatunnel-engine-worker.log`

## 8. 잡 제출 및 관리

### 8.1 클라이언트로 잡 제출하기

#### 클라이언트 설치

서버와 동일하게 `SEATUNNEL_HOME`을 설정하세요.

```
export SEATUNNEL_HOME=${seatunnel 설치 경로}
export PATH=$PATH:$SEATUNNEL_HOME/bin
```

#### 클라이언트 설정

`hazelcast-client.yaml`에서 서버와 동일한 `cluster-name`을 사용하고 Master 노드 주소를 모두 등록합니다.

```yaml
hazelcast-client:
  cluster-name: seatunnel
  properties:
    hazelcast.logging.type: log4j2
  network:
    cluster-members:
      - master-node-1:5801
      - master-node-2:5801
```

#### 잡 제출 및 관리

[잡 제출 및 관리](user-command.md) 문서를 참고하세요.

### 8.2 REST API 사용

REST API를 통해서도 작업을 제출·관리할 수 있습니다. 자세한 내용은 [REST API V2](rest-api-v2.md)를 참고하세요.
