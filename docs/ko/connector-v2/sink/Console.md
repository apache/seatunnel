import ChangeLog from '../changelog/connector-console.md';

# Console

> Console 싱크 커넥터

## 지원 버전 & 엔진

- 모든 커넥터 버전
- Spark / Flink / SeaTunnel Zeta

## 설명

데이터를 콘솔에 출력합니다. 배치와 스트리밍 모두 지원합니다.

## 주요 옵션

| Name | Type | Required | Default | 설명 |
|------|------|----------|---------|------|
| common-options | - | no | - | [싱크 공통 옵션](../sink-common-options.md) |
| log.print.data | boolean | no | true | 로그에 데이터를 출력할지 여부 |
| log.print.delay.ms | int | no | 0 | 각 레코드 출력 간 지연(ms) |

## 예제

### 기본 예제
```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  FakeSource {
    plugin_output = "fake"
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  Console {
    plugin_input = "fake"
  }
}
```

### 다중 소스 출력
```hocon
sink {
  Console {
    plugin_input = "fake1"
  }
  Console {
    plugin_input = "fake2"
  }
}
```

## 콘솔 출력 예시
```
2022-12-19 11:01:45,417 INFO ... ConsoleSinkWriter - output rowType: name<STRING>, age<INT>
2022-12-19 11:01:46,489 INFO ... ConsoleSinkWriter - subtaskIndex=0 rowIndex=1: ...
```

## 변경 이력

<ChangeLog />
