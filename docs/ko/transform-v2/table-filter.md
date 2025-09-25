# TableFilter

> TableFilter 변환 플러그인

## 설명

정규식을 사용해 데이터베이스/스키마/테이블을 필터링합니다.

## 옵션

| name             | type   | required | default | 설명 |
|------------------|--------|----------|---------|------|
| database_pattern | string | no       | -       | 데이터베이스 이름 필터(정규식) |
| schema_pattern   | string | no       | -       | 스키마 이름 필터(정규식) |
| table_pattern    | string | no       | -       | 테이블 이름 필터(정규식) |
| pattern_mode     | string | no       | INCLUDE | `INCLUDE`/`EXCLUDE` 선택 |

## 예제

### 포함 필터
```
transform {
  TableFilter {
    plugin_input = "source1"
    plugin_output = "transform_a_1"
    database_pattern = "test"
    table_pattern = "user_\\d+"
  }
}
```

### 제외 필터
```
transform {
  TableFilter {
    plugin_input = "source1"
    plugin_output = "transform_a_1"
    database_pattern = "test"
    table_pattern = "user_\\d+"
    pattern_mode = "EXCLUDE"
  }
}
```
