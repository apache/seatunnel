# Metadata

> Metadata 변환 플러그인

## 설명

데이터에 메타데이터 필드를 추가합니다.

## 사용 가능한 메타데이터

| Key       | Type   | 설명 |
|-----------|--------|------|
| Database  | string | 행이 속한 데이터베이스 이름 |
| Table     | string | 행이 속한 테이블 이름 |
| RowKind   | string | 동작 유형 (INSERT/UPDATE/DELETE 등) |
| EventTime | long   | 커넥터가 이벤트를 처리한 시각(ms) |
| Delay     | long   | 데이터 추출 시각과 실제 변경 시각의 차이(ms) |
| Partition | string | 행이 속한 파티션 정보(여러 개일 때 `,`로 연결) |

> `Delay`, `EventTime`은 현재 CDC 계열 커넥터(TiDB-CDC 제외)에서만 지원됩니다.

## 옵션

| name            | type | required | default | 설명 |
|-----------------|------|----------|---------|------|
| metadata_fields | map  | yes      | -       | 메타데이터 키와 출력 필드 간 매핑 |

### metadata_fields 예시

```hocon
metadata_fields {
  Database = c_database
  Table = c_table
  RowKind = c_rowKind
  EventTime = c_ts_ms
  Delay = c_delay
}
```

## 예제

```
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MySQL-CDC {
    plugin_output = "customers_mysql_cdc"
    server-id = 5652
    username = "root"
    password = "******"
    table-names = ["source.user"]
    url = "jdbc:mysql://host:3306/source"
  }
}

transform {
  Metadata {
    metadata_fields {
      Database = database
      Table = table
      RowKind = rowKind
      EventTime = ts_ms
      Delay = delay
    }
    plugin_output = "trans_result"
  }
}

sink {
  Console {
    plugin_input = "trans_result"
  }
}
```
