# JsonPath

> JsonPath 변환 플러그인

## 설명

JsonPath를 사용해 JSON 구조에서 원하는 값을 추출하고 새 필드에 매핑합니다. 배열 추출, 컬럼별 오류 처리 등을 지원합니다.

## 옵션

| name                 | type  | required | default | 설명 |
|----------------------|-------|----------|---------|------|
| columns              | array | yes      | -       | 추출 규칙 목록 |
| row_error_handle_way | enum  | no       | FAIL    | 행 단위 오류 처리 전략 |

### 공통 옵션
공통 파라미터는 [Transform 공통 옵션](common-options.md)을 참고하세요.

### row_error_handle_way
- `FAIL`: 오류 발생 시 작업 실패
- `SKIP`: 오류 행을 건너뛰고 계속 처리

### columns 항목
각 항목은 다음 속성을 가집니다.

| key                     | type   | required | default | 설명 |
|-------------------------|--------|----------|---------|------|
| src_field               | string | yes      | -       | JSON 원본 필드 이름 |
| dest_field              | string/array | yes | - | 추출 결과를 저장할 필드 이름(배열 지원) |
| path                    | string/array | yes | - | JsonPath 표현식(캡처 다중 가능) |
| dest_type               | string/array | no | string | 결과 데이터 타입 |
| column_error_handle_way | enum   | no       | -       | 컬럼 단위 오류 처리 (`FAIL`/`SKIP`/`SKIP_ROW`)

## 예제

기본 추출:
```
transform {
  JsonPath {
    plugin_input = "fake"
    plugin_output = "fake1"
    columns = [
      {
        src_field = "data"
        path = "$.data.c_string"
        dest_field = "c1_string"
      }
    ]
  }
}
```

배열 형식으로 여러 필드를 한 번에 추출할 수도 있습니다. 이 경우 `dest_type`을 반드시 지정해야 합니다.

## SeatunnelRow 추출 예시
```
columns = [
  {
    src_field = "col"
    path = "$[0]"
    dest_field = "name"
    dest_type = "string"
  },
  {
    src_field = "col"
    path = "$[1]"
    dest_field = "age"
    dest_type = "int"
  }
]
```

## 오류 처리 예시
- 행 전체 건너뛰기: `row_error_handle_way = SKIP`
- 특정 컬럼만 무시: `column_error_handle_way = "SKIP"`
- 특정 컬럼 오류 시 행 전체 건너뛰기: `column_error_handle_way = "SKIP_ROW"`

