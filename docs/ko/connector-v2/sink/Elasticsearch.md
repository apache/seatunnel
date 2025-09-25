import ChangeLog from '../changelog/connector-elasticsearch.md';

# Elasticsearch

> Elasticsearch 싱크 커넥터

## 설명

Elasticsearch로 데이터를 내보내는 커넥터입니다. 2.x 이상 8.x 이하 버전을 지원합니다.

## 주요 기능

- [ ] Exactly-Once
- [x] CDC 이벤트 전송 지원

## 옵션

| 이름 | 타입 | 필수 | 기본값 | 설명 |
|------|------|------|--------|------|
| hosts | array | 예 | - | `host:port` 형식의 Elasticsearch 노드 목록 |
| index | string | 예 | - | 대상 인덱스 이름. `${field}` 변수 사용 가능(스키마 저장 모드는 `IGNORE` 필요) |
| schema_save_mode | string | 예 | CREATE_SCHEMA_WHEN_NOT_EXIST | 인덱스 생성 전략 (`RECREATE_SCHEMA`, `ERROR_WHEN_SCHEMA_NOT_EXIST`, `IGNORE`) |
| data_save_mode | string | 예 | APPEND_DATA | 기존 데이터 처리 방식 (`DROP_DATA`, `APPEND_DATA`, `CUSTOM_PROCESSING`, `ERROR_WHEN_DATA_EXISTS`) |
| index_type | string | 아니오 | - | 인덱스 타입 (ES 6 이상에서는 지정 권장하지 않음) |
| primary_keys | list | 아니오 | - | `_id` 구성에 사용할 필드 목록 (CDC 필수) |
| key_delimiter | string | 아니오 | `_` | 복합 키 구분자 |
| auth_type | string | 아니오 | basic | 인증 방식 (`basic`, `api_key`, `api_key_encoded`) |
| username/password | string | 아니오 | - | Basic 인증 정보 |
| auth.api_key_id / auth.api_key / auth.api_key_encoded | string | 아니오 | - | API Key 인증 설정 |
| max_retry_count | int | 아니오 | 3 | 배치 요청 재시도 횟수 |
| max_batch_size | int | 아니오 | 10 | Bulk 요청당 문서 수 |
| tls_verify_certificate | boolean | 아니오 | true | HTTPS 인증서 검증 여부 |
| tls_verify_hostnames | boolean | 아니오 | true | HTTPS 호스트네임 검증 여부 |
| tls_keystore_path/password | string | 아니오 | - | TLS 키스토어 경로/비밀번호 |
| tls_truststore_path/password | string | 아니오 | - | TLS 트러스트스토어 경로/비밀번호 |
| vectorization_fields | array | 아니오 | - | 벡터화할 필드 목록 |
| vector_dimensions | int | 아니오 | - | 벡터 차원 |
| common-options | - | 아니오 | - | [싱크 공통 옵션](../sink-common-options.md) |

## 인증 예시

### Basic 인증
```hocon
sink {
  Elasticsearch {
    hosts = ["https://localhost:9200"]
    auth_type = "basic"
    username = "elastic"
    password = "your_password"
    index = "my_index"
  }
}
```

### API Key 인증 (ID/Key 분리)
```hocon
sink {
  Elasticsearch {
    hosts = ["https://localhost:9200"]
    auth_type = "api_key"
    auth.api_key_id = "api_key_id"
    auth.api_key = "api_key_secret"
    index = "my_index"
  }
}
```

### API Key 인코딩 값 사용
```hocon
sink {
  Elasticsearch {
    hosts = ["https://localhost:9200"]
    auth_type = "api_key_encoded"
    auth.api_key_encoded = "base64(id:key)"
    index = "my_index"
  }
}
```

## 기타 설정
- `max_retry_count`: Bulk 요청 실패 시 재시도 횟수
- `vectorization_fields` / `vector_dimensions`: 임베딩 벡터 필드 매핑
- `schema_save_mode`, `data_save_mode`: 인덱스/데이터 초기화 전략 선택

## 변경 이력

<ChangeLog />
