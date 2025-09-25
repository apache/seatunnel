import ChangeLog from '../changelog/connector-cdc-mysql.md';

# MySQL CDC

> MySQL CDC 소스 커넥터

## 지원 엔진

> SeaTunnel Zeta<br/>
> Flink<br/>

## 설명

MySQL CDC 커넥터는 MySQL 데이터베이스에서 스냅샷 데이터와 변경 로그를 읽어와 실시간 동기화를 제공합니다.

## 주요 기능

- [ ] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [user-defined split](../../concept/connector-v2-features.md)

## 지원 데이터 소스

| Datasource | Version | Driver | Url | Maven |
|------------|---------|--------|-----|-------|
| MySQL      | MySQL 5.5/5.6/5.7/8.0.x, RDS MySQL 5.6/5.7/8.0.x | com.mysql.cj.jdbc.Driver | jdbc:mysql://localhost:3306/test | [Download](https://mvnrepository.com/artifact/mysql/mysql-connector-java/8.0.28) |

## 의존성 준비

- Spark/Flink: JDBC 드라이버 JAR을 `${SEATUNNEL_HOME}/plugins/`에 배치
- SeaTunnel Zeta: JDBC 드라이버 JAR을 `${SEATUNNEL_HOME}/lib/`에 배치

## MySQL 사용자 생성 및 권한

```sql
CREATE USER 'user'@'localhost' IDENTIFIED BY 'password';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user' IDENTIFIED BY 'password';
FLUSH PRIVILEGES;
```

## Binlog 설정

- `log_bin`, `binlog_format=ROW`, `binlog_row_image=FULL` 등 필수 설정을 활성화하고 서버를 재시작하세요.
- GTID 사용 시 `gtid_mode=ON`, `enforce_gtid_consistency=ON` 설정이 필요합니다.

## 요구 조건 요약

- MySQL 버전 5.5 이상 (권장 5.7+)
- Binlog ROW 모드 및 FULL 이미지
- CDC 계정에 복제 권한 부여
- 전역 서버 ID 고유 설정

## 커넥터 옵션

| Name | Type | Required | Default | 설명 |
|------|------|----------|---------|------|
| hostname | String | Yes | - | MySQL 호스트 |
| port | Int | No | 3306 | 포트 |
| username | String | Yes | - | MySQL 사용자 |
| password | String | Yes | - | 비밀번호 |
| database-name | String/List | Yes | - | 캡처할 DB 이름(정규식 지원) |
| table-name | String/List | No | - | 캡처할 테이블 이름(정규식) |
| table-pattern | String | No | - | 정규식으로 테이블 지정 시 사용 |
| startup.mode | Enum | No | INITIAL | `initial`, `earliest`, `latest`, `specific`, `timestamp` |
| startup.specific-offset.file | String | No | - | `startup.mode=specific` 시 시작 파일 |
| startup.specific-offset.pos | Long | No | - | `startup.mode=specific` 시 위치 |
| startup.timestamp | Long | No | - | `startup.mode=timestamp` 시 타임스탬프 |
| stop.mode | Enum | No | NEVER | `never`, `latest`, `specific` |
| stop.specific-offset.file | String | No | - | 종료 지점 파일 |
| stop.specific-offset.pos | Long | No | - | 종료 지점 위치 |
| server-id | String | No | - | 복제 서버 ID 범위 (예: `5400-5404`) |
| server-time-zone | String | No | UTC | 서버 타임존 |
| debezium | Map | No | - | Debezium 커스텀 설정 |
| fetch.size | Int | No | 1024 | 한 번에 가져올 레코드 수 |
| snapshot.split.size | Int | No | 8096 | 스냅샷 분할 크기 |
| chunk.key.column | String | No | - | 분할 키 컬럼 |
| include.schema.changes | Boolean | No | false | 스키마 변경 이벤트 포함 |
| heartbeat.interval.ms | Long | No | 5000 | 하트비트 주기 |
| enable.dynamic.read | Boolean | No | false | 테이블 동적 추가 읽기 |
| use.max.connections | Boolean | No | false | 병렬 스냅샷 시 최대 연결 사용 |
| common-options | - | No | - | [공통 옵션](../common-options.md)

*옵션이 많으므로 실제 사용 시 필요한 항목만 설정하세요. `table-names-config`로 테이블별 키, 분할 컬럼을 지정할 수 있습니다.*

## 예제

### 기본 CDC → 콘솔
```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MySQL-CDC {
    hostname = "mysql"
    port = 3306
    username = "root"
    password = "123456"
    database-name = ["inventory"]
    table-name = ["inventory.products"]
  }
}

sink {
  Console {
  }
}
```

### CDC → JDBC 싱크
```hocon
source {
  MySQL-CDC {
    hostname = "mysql"
    username = "root"
    password = "123456"
    table-name = ["inventory.products"]
  }
}

sink {
  jdbc {
    url = "jdbc:mysql://localhost:3306"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "seatunnel"
    generate_sink_sql = true
    database = "inventory_sink"
    table = "products"
    primary_keys = ["id"]
  }
}
```

### 다중 테이블 캡처
```hocon
source {
  MySQL-CDC {
    hostname = "mysql"
    username = "root"
    password = "123456"
    database-name = ["inventory"]
    table-name = ["inventory.products", "inventory.orders"]
    tables_configs = [
      {
        table = "inventory.products"
        primaryKeys = ["id"]
      },
      {
        table = "inventory.orders"
        primaryKeys = ["_id"]
      }
    ]
  }
}
```

## 변경 이력

<ChangeLog />
