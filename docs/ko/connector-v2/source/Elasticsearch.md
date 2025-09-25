import ChangeLog from '../changelog/connector-elasticsearch.md';

# Elasticsearch

> Elasticsearch 소스 커넥터

## 설명

Elasticsearch에서 데이터를 조회하는 커넥터입니다. 2.x 이상 8.x 이하 버전을 지원합니다.

## 주요 기능

- [x] 배치 모드
- [ ] 스트리밍
- [ ] Exactly-Once
- [x] 컬럼 프로젝션
- [ ] 병렬 처리
- [ ] 사용자 정의 Split

## 옵션 요약

| 옵션 | 타입 | 필수 | 기본값 | 설명 |
|------|------|------|--------|------|
| hosts | array | yes | - | `host:port` 목록 |
| auth_type | string | no | basic | 인증 방식(`basic`, `api_key`, `api_key_encoded`) |
| username/password | string | no | - | Basic 인증 사용자/비밀번호 |
| auth.api_key_id / auth.api_key | string | no | - | API Key 인증 시 사용 |
| auth.api_key_encoded | string | no | - | Base64 인코딩 API Key |
| index | string | no | - | 단일 인덱스 이름(없다면 필수) |
| index_list | array | no | - | 다중 인덱스 동기화 설정 |
| source | array | no | - | 조회할 필드 목록 (`_id` 지정 가능) |
| query | json | no | `{ "match_all": {} }` | DSL 쿼리 |
| search_type | enum | no | DSL | `DSL` 또는 `SQL` |
| search_api_type | enum | no | SCROLL | 페이지 API(`SCROLL`, `PIT`) |
| sql_query | json | no | - | `search_type = SQL`일 때 사용 |
| scroll_time | string | no | 1m | Scroll 유지 시간 |
| scroll_size | int | no | 100 | Scroll 페이지 크기 |
| tls_* | string/bool | no | - | TLS 인증서, 호스트 검증, 키스토어/트러스트스토어 등 |
| array_column | map | no | - | 배열 타입 필드 정의 (`{c_array = "array<tinyint>"}`) |
| pit_keep_alive | long | no | 60000 | PIT 유지 시간(ms) |
| pit_batch_size | int | no | 100 | PIT 배치 크기 |
| common-options | - | no | - | [공통 옵션](../common-options.md) |

## 인증

- 기본(Basic): `username`, `password`
- API Key: `auth_type = "api_key"`, `auth.api_key_id`, `auth.api_key`
- Base64 API Key: `auth_type = "api_key_encoded"`, `auth.api_key_encoded`

## `index_list` 사용

여러 인덱스를 각각 다른 쿼리/필드로 처리하려면 `index_list` 배열에 항목을 추가합니다. 각 항목은 단일 인덱스와 동일한 옵션(`index`, `query`, `source`, `scroll_size`, `scroll_time` 등)을 지정할 수 있습니다.

## 예제

### 1) Basic 인증 + 단일 인덱스
```hocon
source {
  Elasticsearch {
    hosts = ["https://localhost:9200"]
    auth_type = "basic"
    username = "elastic"
    password = "your_password"
    index = "my_index"
  }
}
```

### 2) API Key 인증
```hocon
source {
  Elasticsearch {
    hosts = ["https://localhost:9200"]
    auth_type = "api_key"
    auth.api_key_id = "your_api_key_id"
    auth.api_key = "your_api_key_secret"
    index = "my_index"
  }
}
```

### 3) 다중 인덱스 동기화
```hocon
source {
  Elasticsearch {
    hosts = ["http://es:9200"]
    index_list = [
      {
        index = "orders-*"
        query = { "range": { "order_date": { "gte": "now-7d" } } }
        source = ["order_id", "customer", "amount"]
        scroll_size = 500
      },
      {
        index = "customers"
        source = ["_id", "name", "email"]
      }
    ]
  }
}
```

## 변경 이력

<ChangeLog />
