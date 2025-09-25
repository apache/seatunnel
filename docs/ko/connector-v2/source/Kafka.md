import ChangeLog from '../changelog/connector-kafka.md';

# Kafka

> Kafka 소스 커넥터

## 지원 엔진

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 주요 기능

- [x] 배치 모드
- [x] 스트리밍
- [x] Exactly-Once
- [ ] 컬럼 프로젝션
- [x] 병렬 처리
- [ ] 사용자 정의 Split

## 의존성

`connector-kafka` 의존성을 install-plugin.sh 또는 Maven Central에서 다운로드해 `${SEATUNNEL_HOME}/plugins/`(Spark/Flink) 또는 `${SEATUNNEL_HOME}/lib/`(Zeta)에 배치하십시오.

## 주요 옵션

| 옵션 | 타입 | 필수 | 설명 |
|------|------|------|------|
| topic | String | Yes | 읽을 토픽 이름 (쉼표로 여러 개 지정 가능) |
| table_list | Map | No | 토픽/테이블 매핑. `topic`과 동시 사용 불가 |
| bootstrap.servers | String | Yes | `host:port` 목록 |
| pattern | Boolean | No | true일 경우 정규식으로 토픽 구독 |
| consumer.group | String | No | 컨슈머 그룹 이름(기본 `seatunnel_group`) |
| format | String | No | 메시지 포맷(JSON, Canal, Debezium 등) |
| start_mode | String | No | `latest`(기본), `earliest`, `specific-offset`, `timestamp`, `consumer-group` |
| start_offsets / start_timestamp | String/Long | No | 특정 오프셋/타임스탬프로 시작할 때 사용 |
| kafka.config | Map | No | 추가 Kafka 컨슈머 설정 (`enable.auto.commit` 등) |
| partition.discovery.interval.ms | Long | No | 파티션 변경 감지 주기 |
| ignore_no_leader_partition | Boolean | No | 리더 없는 파티션 무시 |
| value_format / key_format | String | No | 메시지 값/키 포맷 |
| value_converter_schema_enabled | Boolean | No | 스키마 포함 여부 |
| fuzzy_fetch_schema_enabled | Boolean | No | 스키마 자동 추론 활성화 |
| common-options | - | No | [공통 옵션](../common-options.md) |

## 인증 예시

### SASL/SCRAM
```hocon
kafka.config = {
  security.protocol = SASL_SSL
  sasl.mechanism = SCRAM-SHA-512
  sasl.jaas.config = "org.apache.kafka.common.security.scram.ScramLoginModule required \nusername=\"user\"\npassword=\"pwd\";"
}
```

### AWS MSK IAM
```hocon
kafka.config = {
  security.protocol = SASL_SSL
  sasl.mechanism = AWS_MSK_IAM
  sasl.jaas.config = "software.amazon.msk.auth.iam.IAMLoginModule required;"
  sasl.client.callback.handler.class = "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
}
```

### Kerberos
```hocon
kafka.config = {
  security.protocol = SASL_PLAINTEXT
  sasl.mechanism = GSSAPI
  sasl.kerberos.service.name = kafka
  sasl.jaas.config = "com.sun.security.auth.module.Krb5LoginModule required ..."
}
```

## 다중 토픽 처리

`table_list` 또는 `tables_configs`로 여러 토픽의 스키마를 정의할 수 있습니다. 데이터 포맷에 맞춰 `format`을 지정하세요.

## 예제

```hocon
source {
  Kafka {
    topic = "seatunnel"
    bootstrap.servers = "localhost:9092"
    consumer.group = "seatunnel_group"
    format = json
    start_mode = earliest
    kafka.config = {
      enable.auto.commit = false
      session.timeout.ms = 45000
    }
  }
}
```

## 변경 이력

<ChangeLog />
