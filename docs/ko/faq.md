# 자주 묻는 질문(FAQ)

## SeaTunnel이 지원하는 데이터 소스와 목적지는 무엇인가요?
SeaTunnel은 다양한 데이터 소스와 목적지를 지원합니다. 자세한 목록은 아래 링크에서 확인할 수 있습니다.
- 지원 데이터 소스(Source): [Source 목록](https://seatunnel.apache.org/docs/connector-v2/source)
- 지원 데이터 목적지(Sink): [Sink 목록](https://seatunnel.apache.org/docs/connector-v2/sink)

## SeaTunnel은 배치와 스트리밍 처리를 모두 지원하나요?
SeaTunnel은 배치와 스트리밍 모드를 모두 지원합니다. 업무 시나리오와 요구사항에 따라 적합한 모드를 선택하면 됩니다. 배치는 주기적인 데이터 통합 작업에, 스트리밍은 실시간 통합과 CDC(Change Data Capture)에 적합합니다.

## SeaTunnel을 사용할 때 Spark나 Flink 같은 엔진을 꼭 설치해야 하나요?
필수는 아닙니다. SeaTunnel은 Zeta, Spark, Flink를 통합 엔진으로 지원하며, 필요에 따라 선택할 수 있습니다. 커뮤니티에서는 통합 시나리오에 맞춰 설계된 차세대 고성능 통합 엔진 Zeta(커뮤니티에서는 애칭으로 “울트라맨 Zeta”라 부릅니다)를 적극 추천합니다. Zeta가 가장 많은 기능을 제공하며 커뮤니티의 지원도 가장 활발합니다.

## SeaTunnel이 제공하는 데이터 변환 기능은 무엇인가요?
SeaTunnel은 필드 매핑, 데이터 필터링, 형식 변환 등 다양한 데이터 변환 기능을 지원합니다. 설정 파일의 `transform` 모듈을 통해 변환 로직을 구현할 수 있으며, 자세한 내용은 [Transform 문서](https://seatunnel.apache.org/docs/transform-v2)를 참고하세요.

## 사용자 정의 데이터 정제 규칙을 적용할 수 있나요?
가능합니다. `transform` 모듈에서 사용자 정의 규칙을 구성해 더러운 데이터를 정리하거나, 유효하지 않은 레코드를 제거하거나, 필드를 변환할 수 있습니다.

## SeaTunnel은 실시간 증분 통합을 지원하나요?
SeaTunnel은 증분 데이터 통합을 지원합니다. 예를 들어 CDC 커넥터는 데이터 변경을 실시간으로 캡처하므로, 실시간 통합이 필요한 시나리오에 적합합니다.

## 현재 SeaTunnel이 지원하는 CDC 데이터 소스는 무엇인가요?
SeaTunnel은 MongoDB CDC, MySQL CDC, OpenGauss CDC, Oracle CDC, PostgreSQL CDC, SQL Server CDC, TiDB CDC 등을 지원합니다. 자세한 내용은 [Source 목록](https://seatunnel.apache.org/docs/connector-v2/source)을 참고하세요.

## SeaTunnel CDC 통합에 필요한 권한은 어떻게 설정하나요?
각 커넥터의 CDC 기능에 필요한 권한 설정 방법은 SeaTunnel 공식 문서를 참고하세요.

## MySQL 복제본에서 CDC를 지원하나요? 로그는 어떻게 수집하나요?
지원합니다. MySQL 복제본의 binlog를 구독해 SeaTunnel 서버에서 로그를 파싱하는 방식으로 동작합니다.

## 기본 키가 없는 테이블에서도 CDC 통합이 가능한가요?
불가능합니다. 기본 키가 없으면 동일한 레코드가 두 개 있을 때 어느 레코드를 삭제·수정해야 하는지 판단할 수 없어 데이터 불일치가 발생할 수 있습니다. 데이터의 고유성을 보장하기 위해 기본 키가 반드시 필요합니다.

## 자동으로 테이블을 생성할 수 있나요?
통합 작업을 시작하기 전에 대상 테이블 구조를 어떻게 처리할지 `schema_save_mode` 매개변수로 지정할 수 있습니다. 옵션은 다음과 같습니다.
- **`RECREATE_SCHEMA`**: 테이블이 없으면 생성하고, 존재하면 삭제 후 다시 생성합니다.
- **`CREATE_SCHEMA_WHEN_NOT_EXIST`**: 테이블이 없으면 생성하고, 존재하면 건너뜁니다.
- **`ERROR_WHEN_SCHEMA_NOT_EXIST`**: 테이블이 없으면 오류를 발생시킵니다.
- **`IGNORE`**: 테이블 처리를 건너뜁니다.
  많은 커넥터가 자동 테이블 생성을 지원하므로, [Jdbc sink](https://seatunnel.apache.org/docs/connector-v2/sink/Jdbc/#schema_save_mode-enum)와 같은 커넥터 문서를 참고하세요.

## 통합 작업을 시작하기 전에 기존 데이터를 어떻게 처리하나요?
`data_save_mode` 매개변수로 대상 측의 기존 데이터를 어떻게 처리할지 지정할 수 있습니다. 옵션은 다음과 같습니다.
- **`DROP_DATA`**: 데이터베이스 구조는 유지하고 데이터를 삭제합니다.
- **`APPEND_DATA`**: 구조와 데이터를 모두 유지합니다.
- **`CUSTOM_PROCESSING`**: 사용자 정의 방식으로 처리합니다.
- **`ERROR_WHEN_DATA_EXISTS`**: 데이터가 이미 있으면 오류를 발생시킵니다.
  많은 커넥터가 기존 데이터 처리를 지원하므로, [Jdbc sink](https://seatunnel.apache.org/docs/connector-v2/sink/Jdbc#data_save_mode-enum)와 같은 커넥터 문서를 참고하세요.

## 정확히 한 번(Exactly-once) 처리를 지원하나요?
SeaTunnel은 MySQL, PostgreSQL 등 일부 데이터 소스에 대해 Exactly-once 일관성을 지원합니다. 다만 정확성 보장은 사용 중인 데이터베이스의 기능에 의존합니다.

## 정기 실행 작업을 구성할 수 있나요?
Linux `cron`을 이용해 주기적으로 작업을 실행하거나, Apache DolphinScheduler, Apache Airflow 같은 스케줄러를 활용해 복잡한 정기 작업을 관리할 수 있습니다.

## 해결하지 못한 문제가 있을 때는 어떻게 하나요?
다음 방법으로 도움을 받을 수 있습니다.
1. [이슈 목록](https://github.com/apache/seatunnel/issues)이나 [메일링 리스트](https://lists.apache.org/list.html?dev@seatunnel.apache.org)를 검색해 동일한 문제가 있었는지 확인합니다.
2. 해결책을 찾지 못했다면, [커뮤니티 연락처](https://github.com/apache/seatunnel#contact-us)를 통해 도움을 요청하세요.

## 변수를 선언하려면 어떻게 하나요?
SeaTunnel 설정에서 변수를 선언한 후 실행 시점에 동적으로 치환할 수 있습니다. 이는 배치·주기 작업에서 시간이나 날짜 등을 치환할 때 흔히 사용됩니다.

설정 파일에서 변수를 정의합니다. 예를 들어 SQL 변환에서(`key = value` 형태라면 어느 위치든 변수로 치환할 수 있습니다):

```plaintext
...
transform {
  Sql {
    query = "select * from dual where city ='${city}' and dt = '${date}'"
  }
}
...
```

Zeta Local 모드에서 변수를 사용해 SeaTunnel을 실행하려면 다음과 같이 합니다.

```bash
$SEATUNNEL_HOME/bin/seatunnel.sh \
-c $SEATUNNEL_HOME/config/your_app.conf \
-m local[2] \
-i city=Singapore \
-i date=20231110
```

`-i` 또는 `--variable` 옵션에 `key=value` 형식으로 값을 지정하면, `key`가 설정 파일의 변수 이름과 매칭됩니다. 자세한 내용은 [SeaTunnel 변수 구성](https://seatunnel.apache.org/docs/concept/config)을 참고하세요.

## 설정 파일에서 여러 줄 텍스트를 작성하려면 어떻게 하나요?
길이가 길어 여러 줄로 작성해야 한다면 삼중 따옴표로 시작과 끝을 감싸면 됩니다.

```plaintext
var = """
Apache SeaTunnel is a
next-generation high-performance,
distributed, massive data integration tool.
"""
```

## 여러 줄 텍스트에서 변수 치환은 어떻게 하나요?
삼중 따옴표 안에서는 변수를 직접 사용할 수 없기 때문에 아래와 같이 처리합니다.

```plaintext
var = """
your string 1
"""${your_var}""" your string 2"""
```

자세한 내용은 [lightbend/config#456](https://github.com/lightbend/config/issues/456)을 참고하세요.

## SeaTunnel 소스 코드를 학습하려면 어디서부터 시작해야 하나요?
SeaTunnel은 추상화 수준이 높고 구조가 잘 정리돼 있어 빅데이터 아키텍처를 학습하기에 좋은 프로젝트입니다. `seatunnel-examples` 모듈의 `SeaTunnelEngineLocalExample.java`부터 살펴보고 디버깅을 시작해 보세요. 자세한 내용은 [SeaTunnel 기여 가이드](https://seatunnel.apache.org/docs/contribution/setup)를 참고하세요.

## 자신만의 Source, Sink, Transform을 개발하려면 전체 소스 코드를 이해해야 하나요?
그럴 필요는 없습니다. Source, Sink, Transform 인터페이스에 집중하면 됩니다. SeaTunnel API용 커넥터(Connector V2)를 직접 개발하려면 **[커넥터 개발 가이드](https://github.com/apache/seatunnel/blob/dev/seatunnel-connectors-v2/README.md)**를 참고하세요.
