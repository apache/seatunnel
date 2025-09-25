import ChangeLog from '../changelog/connector-cdc-postgres.md';

# PostgreSQL CDC

> PostgreSQL CDC 소스 커넥터

## 지원 엔진

> SeaTunnel Zeta<br/>
> Flink<br/>

## 주요 기능

- [ ] 배치 모드
- [x] 스트리밍
- [x] Exactly-Once (XA 기반)
- [ ] 컬럼 프로젝션
- [x] 병렬 처리
- [x] 사용자 정의 Split

## 설명

PostgreSQL CDC 커넥터는 PostgreSQL에서 스냅샷 데이터와 변경 로그를 읽어오는 기능을 제공합니다.

## 지원 데이터 소스

| Datasource | 버전 | Driver | URL | Maven |
|------------|------|--------|-----|-------|
| PostgreSQL | 버전별 드라이버 다름 | org.postgresql.Driver | jdbc:postgresql://localhost:5432/test | [Download](https://mvnrepository.com/artifact/org.postgresql/postgresql) |
| PostgreSQL (Geometry) | PostGIS 필요 시 | org.postgresql.Driver | jdbc:postgresql://localhost:5432/test | [Download](https://mvnrepository.com/artifact/net.postgis/postgis-jdbc) |

## 의존성 준비

- Spark/Flink: JDBC 드라이버 JAR을 `${SEATUNNEL_HOME}/plugins/`에 배치
- SeaTunnel Zeta: JDBC 드라이버 JAR을 `${SEATUNNEL_HOME}/lib/`에 배치

## PostgreSQL CDC 활성화 절차

1. `wal_level = logical` 설정 (postgresql.conf 또는 `ALTER SYSTEM`)
2. 필요 시 `ALTER TABLE ... REPLICA IDENTITY FULL;`
3. 서버 재시작 후 `log_bin`, `binlog_format`, GTID 설정 상태 확인

## 데이터 타입 매핑

PostgreSQL 타입에 맞춰 Seatunnel 타입으로 매핑됩니다(BOOL→BOOLEAN, INT→INT, BIGINT, DECIMAL 등). 자세한 매핑은 원문 표를 참고하세요.

## 옵션 요약

주요 옵션:

- 연결 정보: `hostname`, `port`, `username`, `password`
- 대상 테이블: `database-names`, `schema-names`, `table-names`, `table-pattern`
- 시작 모드: `startup.mode` (`initial`, `earliest`, `latest`, `specific`, `timestamp`)
- 종료 모드: `stop.mode` (`never`, `latest`, `specific`)
- 병렬 스냅샷: `split.size`, `chunk.key.column`, `enable.dynamic.read`
- Debezium 커스텀 설정: `debezium` 맵 사용
- Exactly-Once 비활성화: `exactly_once = false`

## 예제

### 기본 CDC → Console
```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Postgres-CDC {
    plugin_output = "customers_pg_cdc"
    username = "postgres"
    password = "postgres"
    database-names = ["inventory"]
    schema-names = ["public"]
    table-names = ["inventory.users"]
    url = "jdbc:postgresql://localhost:5432/inventory"
  }
}

sink {
  Console {
    plugin_input = "customers_pg_cdc"
  }
}
```

### CDC → JDBC 싱크
```hocon
source {
  Postgres-CDC {
    plugin_output = "customers_pg_cdc"
    username = "postgres"
    password = "postgres"
    database-names = ["postgres_cdc"]
    schema-names = ["inventory"]
    table-names = ["postgres_cdc.inventory.postgres_cdc_table_1"]
    url = "jdbc:postgresql://host:5432/postgres_cdc"
  }
}

sink {
  jdbc {
    plugin_input = "customers_pg_cdc"
    url = "jdbc:postgresql://host:5432/postgres_cdc"
    driver = "org.postgresql.Driver"
    username = "postgres"
    password = "postgres"
    generate_sink_sql = true
    database = "postgres_cdc"
    schema = "inventory"
    tablePrefix = "sink_"
    primary_keys = ["id"]
  }
}
```

### 기본 키 없는 테이블에 커스텀 키 지정
```hocon
source {
  Postgres-CDC {
    table-names = ["db.schema.table"]
    table-names-config = [{
      table = "db.schema.table"
      primaryKeys = ["id"]
    }]
  }
}
```

## 변경 이력

<ChangeLog />
