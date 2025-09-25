# 커넥터 검사 명령 사용법

## 명령 진입점

```shell
bin/seatunnel-connector.sh
```

## 옵션

```text
Usage: seatunnel-connector.sh [options]
  Options:
    -h, --help         사용법 출력
    -l, --list         지원되는 플러그인(source, sink, transform) 목록 표시 (기본값: false)
    -o, --option-rule  플러그인 식별자(커넥터/트랜스폼 이름)로 옵션 규칙 확인
    -pt, --plugin-type SeaTunnel 플러그인 유형. [source, sink, transform] 지원
```

## 예제

```shell
# 지원되는 모든 커넥터(source/sink)와 transform 목록 출력
bin/seatunnel-connector.sh -l
# 지원되는 sink만 출력
bin/seatunnel-connector.sh -l -pt sink
# 지정한 커넥터 또는 transform의 옵션 규칙 확인
bin/seatunnel-connector.sh -o Paimon
# Paimon sink의 옵션 규칙 확인
bin/seatunnel-connector.sh -o Paimon -pt sink
```
