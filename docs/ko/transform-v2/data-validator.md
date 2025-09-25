# DataValidator

> 데이터 검증 변환 플러그인

## 설명

DataValidator는 필드 값이 지정한 규칙을 만족하는지 검사하고, 실패 시 설정한 오류 처리 전략에 따라 동작합니다. NULL 검사, 범위 검증, 길이 검증, 정규식 패턴 매칭 등 다양한 규칙을 지원하며, 사용자 정의 함수(UDF)를 이용한 검증도 가능합니다.

## 옵션

| name                              | type  | required | default | 설명 |
|-----------------------------------|-------|----------|---------|------|
| row_error_handle_way              | enum  | no       | FAIL    | 행 단위 오류 처리 전략 |
| row_error_handle_way.error_table  | string| no       | -       | `ROUTE_TO_TABLE` 사용 시 오류 데이터를 기록할 테이블 이름 |
| field_rules                       | array | yes      | -       | 필드별 검증 규칙 목록 |

### row_error_handle_way

- `FAIL`: 검증 실패 시 작업을 즉시 실패 처리
- `SKIP`: 실패한 행을 건너뛰고 나머지 데이터를 계속 처리
- `ROUTE_TO_TABLE`: 실패한 행을 지정한 오류 테이블로 전송

> `ROUTE_TO_TABLE`은 다중 테이블을 지원하는 Sink에서만 동작합니다.

### row_error_handle_way.error_table

`ROUTE_TO_TABLE` 모드에서 오류 데이터를 전송할 테이블 이름. 필수.

오류 테이블 스키마는 고정이며 다음 필드를 포함합니다.

| 필드 이름          | 타입     | 설명 |
|--------------------|----------|------|
| source_table_id    | STRING   | 원본 테이블 식별자 |
| source_table_path  | STRING   | 원본 테이블 전체 경로 |
| original_data      | STRING   | 검증 실패한 행의 원본 JSON |
| validation_errors  | STRING   | 실패한 필드와 메시지를 담은 JSON 배열 |
| create_time        | TIMESTAMP| 오류 레코드 생성 시각 |

### field_rules

필드별 검증 규칙 배열. 각 항목은 `field_name`과 `rules`(또는 단일 규칙 속성)으로 구성됩니다.

#### 지원 규칙 유형

- **NOT_NULL**: null 허용 안 됨
- **RANGE**: 숫자 범위 검증 (`min_value`, `max_value`, `min_inclusive`, `max_inclusive`)
- **LENGTH**: 문자열/배열 길이 검증 (`min_length`, `max_length`, `exact_length`)
- **REGEX**: 정규식 매칭 (`pattern`, `case_sensitive`)
- **UDF**: 사용자 정의 함수 (`function_name`)

각 규칙에는 `custom_message`를 지정해 오류 메시지를 커스터마이징할 수 있습니다.

#### 내장 UDF

- `EMAIL`: OWASP 권장 규칙을 기반으로 이메일 형식 검증

사용자 정의 UDF는 `DataValidatorUDF` 인터페이스를 구현하고 `@AutoService` 애너테이션을 통해 등록하면 됩니다.

### 공통 옵션

공통 파라미터는 [Transform 공통 옵션](common-options.md)을 참고하세요.

## 예제

### 1) FAIL 모드 사용
```hocon
transform {
  DataValidator {
    plugin_input = "source_table"
    plugin_output = "validated_table"
    row_error_handle_way = "FAIL"
    field_rules = [
      {
        field_name = "name"
        rule_type = "NOT_NULL"
      },
      {
        field_name = "age"
        rule_type = "RANGE"
        min_value = 0
        max_value = 150
      },
      {
        field_name = "email"
        rule_type = "REGEX"
        pattern = "^[\\w-\\.]+@([\\w-]+\\.)+[\\w-]{2,4}$"
      }
    ]
  }
}
```

### 2) SKIP 모드 사용
```hocon
transform {
  DataValidator {
    plugin_input = "source_table"
    plugin_output = "validated_table"
    row_error_handle_way = "SKIP"
    field_rules = [
      {
        field_name = "name"
        rule_type = "NOT_NULL"
      },
      {
        field_name = "name"
        rule_type = "LENGTH"
        min_length = 2
        max_length = 50
      }
    ]
  }
}
```

### 3) 오류 테이블로 라우팅
```hocon
transform {
  DataValidator {
    plugin_input = "source_table"
    plugin_output = "validated_table"
    row_error_handle_way = "ROUTE_TO_TABLE"
    row_error_handle_way.error_table = "error_data"
    field_rules = [
      {
        field_name = "name"
        rule_type = "NOT_NULL"
      },
      {
        field_name = "age"
        rule_type = "RANGE"
        min_value = 0
        max_value = 150
      }
    ]
  }
}
```

