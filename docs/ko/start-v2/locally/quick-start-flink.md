---
sidebar_position: 3
---

# Flink 빠른 시작

## 1단계: SeaTunnel과 커넥터 배포

작업을 시작하기 전에 [배포](deployment.md) 문서에 따라 SeaTunnel과 필요한 커넥터를 다운로드하고 배포했는지 확인하세요.

## 2단계: Flink 설치 및 구성

먼저 [Flink 다운로드](https://flink.apache.org/downloads.html) 페이지에서 **1.12.0 이상** 버전을 설치하세요. 자세한 배포 방법은 [Standalone 시작하기](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/deployment/resource-providers/standalone/overview/) 문서를 참고하면 됩니다.

**SeaTunnel 구성**: `${SEATUNNEL_HOME}/config/seatunnel-env.sh` 파일에서 `FLINK_HOME`을 Flink 설치 경로로 설정하세요.

## 3단계: 작업을 정의하는 설정 파일 추가

SeaTunnel이 시작된 뒤 데이터 입력·처리·출력 방식을 결정하는 파일 `config/v2.streaming.conf.template`를 수정합니다.
아래 예시는 앞서 소개한 샘플 애플리케이션과 동일한 구성입니다.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 16
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

transform {
  FieldMapper {
    plugin_input = "fake"
    plugin_output = "fake1"
    field_mapper = {
      age = age
      name = new_name
    }
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
}

```

구성 옵션에 대한 자세한 내용은 [구성 기본 개념](../../concept/config.md)을 참고하세요.

## 4단계: SeaTunnel 애플리케이션 실행

다음 명령으로 애플리케이션을 실행할 수 있습니다.

Flink 버전이 `1.12.x`~`1.14.x`인 경우

```shell
cd "apache-seatunnel-${version}"
./bin/start-seatunnel-flink-13-connector-v2.sh --config ./config/v2.streaming.conf.template
```

Flink 버전이 `1.15.x`~`1.18.x`인 경우

```shell
cd "apache-seatunnel-${version}"
./bin/start-seatunnel-flink-15-connector-v2.sh --config ./config/v2.streaming.conf.template
```

**출력 확인**: 명령을 실행하면 콘솔에 출력이 표시됩니다. 이 결과로 명령이 정상 실행됐는지 확인할 수 있습니다.

SeaTunnel 콘솔에는 다음과 같은 로그가 출력됩니다.

```shell
fields : name, age
types : STRING, INT
row=1 : elWaB, 1984352560
row=2 : uAtnp, 762961563
row=3 : TQEIB, 2042675010
row=4 : DcFjo, 593971283
row=5 : SenEb, 2099913608
row=6 : DHjkg, 1928005856
row=7 : eScCM, 526029657
row=8 : sgOeE, 600878991
row=9 : gwdvw, 1951126920
row=10 : nSiKE, 488708928
row=11 : xubpl, 1420202810
row=12 : rHZqb, 331185742
row=13 : rciGD, 1112878259
row=14 : qLhdI, 1457046294
row=15 : ZTkRx, 1240668386
row=16 : SGZCr, 94186144
```

## 더 알아보기

- 지금 바로 자신만의 설정 파일을 작성해 보세요. 사용할 [커넥터](../../connector-v2/source)를 고르고, 해당 커넥터 문서에 따라 매개변수를 구성하면 됩니다.
- Flink에서 SeaTunnel을 사용하는 방법은 [SeaTunnel With Flink](../../other-engine/flink.md) 문서를 참고하세요.
- SeaTunnel에는 기본 내장 엔진 `Zeta`가 있으며, SeaTunnel의 기본 실행 엔진입니다. [SeaTunnel Engine 빠른 시작](quick-start-seatunnel-engine.md)을 따라 데이터 동기화 작업을 구성하고 실행해 보세요.

