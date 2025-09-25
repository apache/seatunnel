# TableRename

> TableRename 변환 플러그인

## 설명

테이블 이름을 일괄 변경하거나 특정 패턴으로 치환합니다.

## 옵션

| name                   | type   | required | default | 설명 |
|------------------------|--------|----------|---------|------|
| convert_case           | string | no       | -       | 테이블 명 대/소문자 변환 (`UPPER`, `LOWER`) |
| prefix                 | string | no       | -       | 테이블 명 앞에 붙일 접두사 |
| suffix                 | string | no       | -       | 테이블 명 뒤에 붙일 접미사 |
| replacements_with_regex | array | no       | -       | 정규식 치환 규칙 (`replace_from`, `replace_to`) |

## 예제

### 대문자로 변환
```
transform {
  TableRename {
    plugin_input = "customers_mysql_cdc"
    plugin_output = "trans_result"
    convert_case = "UPPER"
    prefix = "CDC_"
    suffix = "_TABLE"
    replacements_with_regex = [{ replace_from = "user", replace_to = "U" }]
  }
}
```

### 소문자로 변환
```
transform {
  TableRename {
    plugin_input = "customers_oracle_cdc"
    plugin_output = "trans_result"
    convert_case = "LOWER"
    prefix = "cdc_"
    suffix = "_table"
    replacements_with_regex = [{ replace_from = "USER", replace_to = "u" }]
  }
}
```
