import ChangeLog from '../changelog/connector-jdbc.md';

# JDBC

> JDBC 싱크 커넥터

## 설명

JDBC를 통해 데이터를 쓰는 커넥터입니다. 배치/스트리밍 모두 지원하며, 동시성 쓰기와 XA 트랜잭션 기반의 Exactly-Once를 제공합니다.

## 의존성 준비

- Spark/Flink 엔진: JDBC 드라이버 JAR을 `${SEATUNNEL_HOME}/plugins/`에 배치
- SeaTunnel Zeta 엔진: JDBC 드라이버 JAR을 `${SEATUNNEL_HOME}/lib/`에 배치

## 주요 기능

- [x] Exactly-Once (XA 트랜잭션 사용, `is_exactly_once=true` 설정 시)
- [x] CDC 이벤트 처리 지원
- [x] 다중 테이블 쓰기 지원

## 옵션

| Name | Type | Required | Default | 설명 |
|------|------|----------|---------|------|
| url | String | Yes | - | JDBC 연결 URL |
| driver | String | Yes | - | JDBC 드라이버 클래스 이름 |
| username | String | No | - | DB 사용자 이름 |
| password | String | No | - | DB 비밀번호 |
| query | String | No | - | 사용자 정의 INSERT/UPDATE SQL |
| compatible_mode | String | No | - | DB 호환 모드 (예: `mysql`, `oracle`, `postgresLow` 등) |
| dialect | String | No | - | 사용할 dialect. 설정 시 URL보다 우선 |
| database | String | No | - | 자동 SQL 생성 시 대상 DB 이름 |
| table | String | No | - | 대상 테이블 이름 (변수 `${schema_name}`, `${table_name}` 지원) |
| primary_keys | Array | No | - | 기본 키 목록 (UPSERT/CDC에 필요) |
| connection_check_timeout_sec | Int | No | 30 | 연결 검사 타임아웃(초) |
| max_retries | Int | No | 0 | 배치 실행 실패 시 재시도 횟수 |
| batch_size | Int | No | 1000 | 배치 크기 |
| is_exactly_once | Boolean | No | false | Exactly-Once 활성화 여부 |
| generate_sink_sql | Boolean | No | false | DB/테이블 정보를 기반으로 SQL 자동 생성 |
| xa_data_source_class_name | String | No | - | XA 데이터소스 클래스 |
| max_commit_attempts | Int | No | 3 | 트랜잭션 커밋 실패 재시도 횟수 |
| transaction_timeout_sec | Int | No | -1 | 트랜잭션 타임아웃(초) |
| auto_commit | Boolean | No | true | 자동 커밋 사용 여부 |
| field_ide | String | No | - | 필드 이름 대/소문자 정책 (`ORIGINAL`, `UPPERCASE`, `LOWERCASE`) |
| properties | Map | No | - | 추가 커넥션 파라미터 |
| schema_save_mode | Enum | No | CREATE_SCHEMA_WHEN_NOT_EXIST | 테이블 스키마 처리 방식 |
| data_save_mode | Enum | No | APPEND_DATA | 기존 데이터 처리 방식 |
| custom_sql | String | No | - | 커스텀 SQL (CUSTOM_PROCESSING 사용 시) |
| enable_upsert | Boolean | No | true | 기본 키 기반 UPSERT 사용 여부 |
| use_copy_statement | Boolean | No | false | COPY 구문 사용(PostgreSQL 등) |
| create_index | Boolean | No | true | 테이블 자동 생성 시 인덱스 생성 여부 |
| common-options | - | No | - | [싱크 공통 옵션](../sink-common-options.md)

## 주요 옵션 설명

- **generate_sink_sql**: `database`, `table`, `primary_keys`를 기반으로 INSERT/UPSERT SQL을 자동 생성합니다. `query`와 동시에 사용할 수 없습니다.
- **schema_save_mode**:
  - `RECREATE_SCHEMA`: 테이블이 있으면 삭제 후 재생성
  - `CREATE_SCHEMA_WHEN_NOT_EXIST`: 테이블 없을 때만 생성(기본)
  - `ERROR_WHEN_SCHEMA_NOT_EXIST`: 테이블 없으면 에러
  - `IGNORE`: 테이블 생성/삭제 작업 없음
- **data_save_mode**:
  - `DROP_DATA`: 구조 유지, 데이터 삭제
  - `APPEND_DATA`: 구조/데이터 유지(기본)
  - `CUSTOM_PROCESSING`: `custom_sql` 실행
  - `ERROR_WHEN_DATA_EXISTS`: 데이터 존재 시 에러
- **is_exactly_once**: true 시 XA 트랜잭션을 사용. DB에서 XA 지원 필요 (PostgreSQL: `max_prepared_transactions` 설정, MySQL: 8.0.29+ 및 `XA_RECOVER_ADMIN` 권한)
- **use_copy_statement**: PostgreSQL 드라이버의 COPY API 사용. `MAP/ARRAY/ROW` 타입은 미지원

## 팁

- JDBC 드라이버 URL에 `rewriteBatchedStatements=true`(MySQL) 등을 추가하면 성능 향상
- XA 사용 시 DB 설정 필요: PostgreSQL `ALTER SYSTEM set max_prepared_transactions`, MySQL 권한 부여 등

## 예제

### 기본 설정
```hocon
sink {
  jdbc {
    url = "jdbc:mysql://localhost:3306"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "123456"
    generate_sink_sql = true
    database = "sink_database"
    table = "sink_table"
    primary_keys = ["id"]
  }
}
```

### PostgreSQL 9.5 이하 CDC 지원
```hocon
sink {
  jdbc {
    url = "jdbc:postgresql://localhost:5432"
    driver = "org.postgresql.Driver"
    user = "root"
    password = "123456"
    compatible_mode = "postgresLow"
    database = "sink_database"
    table = "sink_table"
    generate_sink_sql = true
    primary_keys = ["id"]
  }
}
```

### 다중 테이블 예시
```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Mysql-CDC {
    url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
    username = "root"
    password = "******"
    table-names = ["seatunnel.role", "seatunnel.user"]
  }
}

sink {
  jdbc {
    url = "jdbc:mysql://localhost:3306"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "123456"
    generate_sink_sql = true
    database = "${database_name}_test"
    table = "${table_name}_test"
    primary_keys = ["${primary_key}"]
  }
}
```

## 변경 이력

<ChangeLog />
