# Split

> Split 변환 플러그인

## 설명

지정한 필드 값을 구분자로 분리해 여러 필드로 나눕니다.

## 옵션

| name          | type   | required | default |
|---------------|--------|----------|---------|
| separator     | string | yes      | -       |
| split_field   | string | yes      | -       |
| output_fields | array  | yes      | -       |

### separator
분리에 사용할 구분자 문자열입니다.

### split_field
분리 대상 필드 이름입니다.

### output_fields
분리 결과를 매핑할 필드 목록입니다.

### 공통 옵션
공통 파라미터는 [Transform 공통 옵션](common-options.md)을 참고하세요.

## 예제

소스 데이터

| name     | age | card |
|----------|-----|------|
| Joy Ding | 20  | 123  |
| May Ding | 20  | 123  |
| Kin Dom  | 20  | 123  |
| Joy Dom  | 20  | 123  |

`name` 필드를 공백 기준으로 `first_name`, `second_name`으로 나누려면 아래처럼 설정합니다.

```
transform {
  Split {
    plugin_input = "fake"
    plugin_output = "fake1"
    separator = " "
    split_field = "name"
    output_fields = [first_name, second_name]
  }
}
```

결과 데이터 `fake1`

| name     | age | card | first_name | last_name |
|----------|-----|------|------------|-----------|
| Joy Ding | 20  | 123  | Joy        | Ding      |
| May Ding | 20  | 123  | May        | Ding      |
| Kin Dom  | 20  | 123  | Kin        | Dom       |
| Joy Dom  | 20  | 123  | Joy        | Dom       |

## 변경 이력

- Split Transform 추가
