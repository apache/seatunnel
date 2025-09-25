# FieldMapper

> FieldMapper 변환 플러그인

## 설명

입력 스키마와 출력 스키마 간 필드 매핑을 정의합니다.

## 옵션

| name         | type   | required | default |
|--------------|--------|----------|---------|
| field_mapper | object | yes      | -       |

### field_mapper

입력 필드와 출력 필드의 매핑 관계를 지정합니다.

### 공통 옵션

공통 파라미터는 [Transform 공통 옵션](common-options.md)을 참고하세요.

## 예제

소스 데이터

| id | name     | age | card |
|----|----------|-----|------|
| 1  | Joy Ding | 20  | 123  |
| 2  | May Ding | 20  | 123  |
| 3  | Kin Dom  | 20  | 123  |
| 4  | Joy Dom  | 20  | 123  |

`age`를 제거하고, 필드 순서를 `id`, `card`, `name`으로 재배치하며 `name`을 `new_name`으로 변경하려면 다음과 같이 설정합니다.

```
transform {
  FieldMapper {
    plugin_input = "fake"
    plugin_output = "fake1"
    field_mapper = {
      id = id
      card = card
      name = new_name
    }
  }
}
```

결과 데이터 `fake1`

| id | card | new_name |
|----|------|----------|
| 1  | 123  | Joy Ding |
| 2  | 123  | May Ding |
| 3  | 123  | Kin Dom  |
| 4  | 123  | Joy Dom  |

## 변경 이력

- FieldMapper Transform 추가
