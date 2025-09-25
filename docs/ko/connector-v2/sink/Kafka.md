import ChangeLog from '../changelog/connector-kafka.md';

# Kafka

> Kafka 싱크 커넥터

## 지원 엔진

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

Exactly-Once 모드를 지원하며 기본적으로 2PC를 사용해 메시지를 Kafka에 한 번만 전송하도록 보장합니다.

## 설명

행 데이터를 지정한 Kafka 토픽으로 전송합니다.

## 의존성

`connector-kafka` 모듈 의존성이 필요합니다. install-plugin.sh 또는 Maven Central에서 다운로드할 수 있습니다.

## 주요 옵션

| Name | Type | Required | Default | 설명 |
|------|------|----------|---------|------|
| topic | String | Yes | - | 전송 대상 토픽 |
| bootstrap.servers | String | Yes | - | `host:port` 목록 |
| kafka.config | Map | No | - | Kafka 프로듀서 설정(`acks`, `compression.type` 등) |
| semantics | String | No | NON | `EXACTLY_ONCE` / `AT_LEAST_ONCE` / `NON` |
| partition_key_fields | Array | No | - | 메시지 키로 사용할 필드 목록 |
| partition | Int | No | - | 특정 파티션으로 전송 |
| assign_partitions | Array | No | - | 파티션-토픽 매핑 설정 |
| format | String | No | json | 메시지 포맷(JSON, Canal, Debezium 등) |
| key_format | String | No | - | 메시지 키 포맷 |
| value_format | String | No | - | 메시지 값 포맷 |
| kafka.request.timeout.ms | Int | No | - | 요청 타임아웃(ms) |
| transactional.id.prefix | String | No | - | EXACTLY_ONCE 모드 트랜잭션 ID 접두사 |
| common-options | - | No | - | [싱크 공통 옵션](../sink-common-options.md)

## Exactly-Once 설정

- `semantics = EXACTLY_ONCE`
- 트랜잭션 ID는 병렬도와 조합해 고유해야 합니다(`transactional.id.prefix`).
- `kafka.config`에 `acks=all`, `transaction.timeout.ms` 등을 조정할 수 있습니다.

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
`aws-msk-iam-auth` JAR을 `$SEATUNNEL_HOME/plugin/kafka/lib`에 배치하고 아래처럼 설정합니다.
```hocon
kafka.config = {
  security.protocol = SASL_SSL
  sasl.mechanism = AWS_MSK_IAM
  sasl.jaas.config = "software.amazon.msk.auth.iam.IAMLoginModule required;"
  sasl.client.callback.handler.class = "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
}
```

### Kerberos
`java.security.krb5.conf`를 설정하고 다음과 같이 SASL GSSAPI를 사용합니다.
```hocon
kafka.config = {
  security.protocol = SASL_PLAINTEXT
  sasl.mechanism = GSSAPI
  sasl.kerberos.service.name = kafka
  sasl.jaas.config = "com.sun.security.auth.module.Krb5LoginModule required ..."
}
```

## 예제

```hocon
sink {
  Kafka {
    topic = "test_topic"
    bootstrap.servers = "localhost:9092"
    format = json
    semantics = EXACTLY_ONCE
    kafka.config = {
      acks = "all"
      request.timeout.ms = 60000
    }
  }
}
```

## 변경 이력

<ChangeLog />
