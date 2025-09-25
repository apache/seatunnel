# FieldRename

> FieldRename 변환 플러그인

## 설명

필드 이름을 변경하거나 대/소문자를 일괄 변환합니다.

## 옵션

| name                   | type   | required | default | 설명 |
|------------------------|--------|----------|---------|------|
| convert_case           | string | no       | -       | 대소문자 변환 유형 (`UPPER`, `LOWER`) |
| prefix                 | string | no       | -       | 필드 이름에 추가할 접두사 |
| suffix                 | string | no       | -       | 필드 이름에 추가할 접미사 |
| replacements_with_regex | array | no       | -       | 정규식 기반 치환 규칙 목록 (`replace_from`, `replace_to`) |

### 공통 옵션
공통 파라미터는 [Transform 공통 옵션](common-options.md)을 참고하세요.

## 예제

### 필드를 대문자로 변환
```
transform {
  FieldRename {
    plugin_input = "customers_mysql_cdc"
    plugin_output = "trans_result"
    convert_case = "UPPER"
    prefix = "F_"
    suffix = "_S"
    replacements_with_regex = [
      {
        replace_from = "create_time"
        replace_to = "SOURCE_CREATE_TIME"
      }
    ]
  }
}
```

### 필드를 소문자로 변환
```
transform {
  FieldRename {
    plugin_input = "customers_oracle_cdc"
    plugin_output = "trans_result"
    convert_case = "LOWER"
    prefix = "f_"
    suffix = "_s"
    replacements_with_regex = [
      {
        replace_from = "CREATE_TIME"
        replace_to = "source_create_time"
      }
    ]
  }
}
```
