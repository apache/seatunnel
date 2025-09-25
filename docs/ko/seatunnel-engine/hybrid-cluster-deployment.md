---
sidebar_position: 5
---

# 하이브리드 모드 클러스터 배포

하이브리드 모드에서는 SeaTunnel Engine의 Master 서비스와 Worker 서비스가 동일한 프로세스에 공존합니다. 모든 노드가 작업을 실행할 수 있고, 동시에 마스터 선출에도 참여합니다. 즉, 마스터 노드 역시 동기화 작업을 수행합니다. 이 모드에서는 작업 상태 정보를 저장해 장애 복구를 도와주는 Imap 데이터가 모든 노드에 분산 저장됩니다.

> **권장 사항**: 장기적으로는 [분리형 클러스터 모드](separated-cluster-deployment.md) 사용을 권장합니다. 하이브리드 모드에서는 마스터 노드가 작업도 함께 실행하므로, 작업 규모가 커지면 마스터 안정성이 떨어질 수 있습니다. 마스터 장애나 하트비트 타임아웃으로 노드가 전환되면, 실행 중인 모든 작업이 장애 복구를 수행하게 되어 클러스터 부하가 크게 늘어납니다. 이런 문제를 피하기 위해 분리형 모드를 사용하는 것이 좋습니다.

## 1. 다운로드

[SeaTunnel 설치 패키지 다운로드 및 생성](download-seatunnel.md)

## 2. SEATUNNEL_HOME 설정

`/etc/profile.d/seatunnel.sh` 파일을 추가해 `SEATUNNEL_HOME`을 설정할 수 있습니다.

```
export SEATUNNEL_HOME=${seatunnel 설치 경로}
export PATH=$PATH:$SEATUNNEL_HOME/bin
```

## 3. SeaTunnel Engine JVM 옵션 설정

JVM 옵션은 두 가지 방법으로 지정할 수 있습니다.

1. `$SEATUNNEL_HOME/config/jvm_options` 파일에 JVM 옵션을 추가합니다.
2. 엔진 실행 시 JVM 옵션을 전달합니다. 예: `seatunnel-cluster.sh -DJvmOption="-Xms2G -Xmx2G"`

## 4. SeaTunnel Engine 설정

엔진 관련 주요 설정은 `seatunnel.yaml`에 정의합니다.

### 4.1 Imap 데이터 백업 수 설정

SeaTunnel Engine은 [Hazelcast IMDG](https://docs.hazelcast.com/imdg/4.1/) 기반으로 클러스터를 관리하며, 작업 상태·리소스 상태 등은 [Hazelcast IMap](https://docs.hazelcast.com/imdg/4.1/data-structures/map)에 저장됩니다. IMap 데이터는 클러스터의 모든 노드에 분산되며, 파티션별 백업 수를 지정할 수 있습니다. 이를 통해 ZooKeeper 같은 외부 서비스 없이도 클러스터 HA를 구현할 수 있습니다.

`backup-count`는 동기 백업 개수를 의미합니다. 1이면 다른 노드 1곳에, 2이면 2곳에 백업합니다. 권장 값은 `max(1, min(5, N/2))` (N은 노드 수)입니다.

```yaml
seatunnel:
  engine:
    backup-count: 1
```

### 4.2 슬롯 설정

슬롯 수는 노드가 동시에 실행할 수 있는 태스크 그룹 수를 결정합니다. 작업에 필요한 슬롯 수는 `2 + P`(작업 병렬도)입니다. 기본적으로 동적 슬롯(`dynamic-slot: true`)이 활성화되어 제한이 없습니다. 정적 슬롯으로 제한하려면 CPU 코어 수의 2배 정도를 권장합니다.

```yaml
# 동적 슬롯 (기본값)
seatunnel:
  engine:
    slot-service:
      dynamic-slot: true
```

```yaml
# 정적 슬롯 예시
seatunnel:
  engine:
    slot-service:
      dynamic-slot: false
      slot-num: 20
```

### 4.3 체크포인트 관리자

SeaTunnel Engine은 Flink와 마찬가지로 Chandy–Lamport 알고리즘을 지원해 데이터 손실·중복 없이 동기화를 수행합니다.

- **interval**: 체크포인트 간격(ms). 잡 구성의 `env.checkpoint.interval`이 있으면 해당 값을 사용.
- **timeout**: 체크포인트 제한 시간. 초과 시 실패로 간주. 잡 설정에 `checkpoint.timeout`이 있으면 우선.
- **min-pause**: 연속 체크포인트 사이 최소 간격(ms).

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

**checkpoint storage**: 체크포인트는 장애 복구 시 상태를 복원하는 데 사용됩니다. 다중 노드 클러스터에서는 공유 저장소/HDFS/OSS 등 분산 스토리지를 사용해야 합니다. 자세한 내용은 [Checkpoint Storage](checkpoint-storage.md)를 참고하세요.

### 4.4 완료된 잡 정보의 만료 설정

잡 상태, 카운터, 에러 로그 등은 IMap에 저장됩니다. 작업 수가 많아지면 메모리를 압박하므로, `history-job-expire-minutes`(기본 1440분)를 조정해 만료 시간을 설정할 수 있습니다.

```yaml
seatunnel:
  engine:
    history-job-expire-minutes: 1440
```

### 4.5 클래스 로더 캐시 모드

클래스 로더 생성·소멸 반복으로 인한 메타스페이스 누수를 방지하기 위한 설정입니다. 활성화하면 잡이 끝나도 클래스 로더를 해제하지 않고 재사용합니다(기본값 true).

```yaml
seatunnel:
  engine:
    classloader-cache-mode: true
```

### 4.6 작업 스케줄 전략

자원이 부족할 때 잡 스케줄링 전략을 지정할 수 있습니다.

- `WAIT`: 자원이 확보될 때까지 대기
- `REJECT`: 작업 거부 (기본값)

```yaml
seatunnel:
  engine:
    job-schedule-strategy: WAIT
```

단, `dynamic-slot: true`이면 이 설정은 강제로 `REJECT`로 변경됩니다.

### 4.7 코디네이터 서비스

CoordinatorService는 LogicalDag → ExecutionDag → PhysicalDag으로 잡을 변환하고, JobMaster를 생성해 스케줄링·실행·상태 모니터링을 담당합니다.

- **core-thread-num**: 코디네이터 실행 스레드 풀 corePoolSize
- **max-thread-num**: 동시에 처리 가능한 최대 잡 수

```yaml
coordinator-service:
  core-thread-num: 30
  max-thread-num: 1000
```

### 4.8 잡 메트릭 파티션 수 (Worker 노드에서는 효과 없음)

`job-metrics-partition-count`로 Hazelcast IMap에 저장되는 잡 메트릭 파티션 수를 설정합니다.

- 기본값: 1
- 사용 예: 값이 크면 메트릭 업데이트 경합을 줄일 수 있음.

```yaml
seatunnel:
  engine:
    job-metrics-partition-count: 4
```

작업 수가 2만 개 이상일 때 효과가 큽니다. 실무에서는 1000~2000 정도가 적당하며, 클러스터 규모와 워크로드에 따라 조정하세요. 너무 크게 설정하면 분산/병합 오버헤드가 증가할 수 있으므로, 변경 후 Seatunnel을 재시작하는 것이 좋습니다.

## 5. 네트워크 서비스 설정

네트워크 관련 설정은 모두 `hazelcast.yaml`에 정의됩니다.

### 5.1 cluster-name

노드는 `cluster-name`이 같을 때만 같은 클러스터로 인식합니다. 이름이 다르면 요청이 거부됩니다.

### 5.2 네트워크

SeaTunnel Engine 클러스터는 Hazelcast의 다양한 디스커버리 메커니즘을 활용해 자동으로 구성되며, 통신은 항상 TCP/IP로 이루어집니다. 기본적으로 TCP 방식을 권장합니다. 자세한 설정은 [TCP](tcp.md)를 참고하세요.

```yaml
hazelcast:
  cluster-name: seatunnel
  network:
    join:
      tcp-ip:
        enabled: true
        member-list:
          - hostname1
    port:
      auto-increment: false
      port: 5801
  properties:
    hazelcast.logging.type: log4j2
```

그 밖의 디스커버리 방식은 [Hazelcast Network](https://docs.hazelcast.com/imdg/4.1/clusters/setting-up-clusters)를 참고하세요.

### 5.3 IMap 영속화 설정

IMap은 작업 상태를 저장해 장애 발생 시 복구에 사용됩니다. 기본적으로 메모리에만 저장되므로, 모든 노드를 종료하면 데이터가 사라집니다. 이를 방지하려면 HDFS/OSS/로컬 파일 등 외부 스토리지에 MapStore를 구성해 데이터를 영속화할 수 있습니다.

**주요 속성**
- `type`: 현재 `hdfs`만 지원
- `namespace`: 비즈니스별 구분 경로(예: OSS 버킷 경로)
- `clusterName`: 클러스터 구분용 이름
- `fs.defaultFS`: HDFS/파일 시스템 설정

예시(HDFS 사용):

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

단일 노드에서 로컬 파일 사용:

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

OSS를 사용하려면 다음 JAR가 `lib` 디렉터리에 있어야 합니다.
```
aliyun-sdk-oss-3.13.2.jar
hadoop-aliyun-3.3.6.jar
jdom2-2.0.6.jar
netty-buffer-4.1.89.Final.jar
netty-common-4.1.89.Final.jar
seatunnel-hadoop3-3.1.4-uber.jar
```

## 6. SeaTunnel Engine 클라이언트 설정

클라이언트 설정은 `hazelcast-client.yaml`에 정의합니다.

### 6.1 cluster-name

클라이언트도 서버와 동일한 `cluster-name`을 사용해야 합니다.

### 6.2 network

`cluster-members`에 모든 서버 노드 주소를 추가합니다.

```yaml
hazelcast-client:
  cluster-name: seatunnel
  properties:
    hazelcast.logging.type: log4j2
  network:
    cluster-members:
      - hostname1:5801
```

## 7. 서버 노드 기동

데몬 옵션(-d)으로 실행할 수 있습니다.

```shell
mkdir -p $SEATUNNEL_HOME/logs
./bin/seatunnel-cluster.sh -d
```

로그는 `$SEATUNNEL_HOME/logs/seatunnel-engine-server.log`에 저장됩니다.

## 8. 잡 제출 및 관리

### 8.1 클라이언트로 잡 제출하기

#### 클라이언트 설치

서버 노드의 `$SEATUNNEL_HOME` 디렉터리를 클라이언트 노드에 복사하고 동일하게 `SEATUNNEL_HOME`을 설정하면 됩니다.

#### 잡 제출 및 관리

클러스터가 준비되면 다음 가이드로 작업을 제출·관리할 수 있습니다: [잡 제출 및 관리](user-command.md)

### 8.2 REST API로 잡 제출하기

SeaTunnel Engine은 REST API를 제공하며, 자세한 내용은 [REST API V2](rest-api-v2.md)를 참고하세요.
