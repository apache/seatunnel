# Filter

> Filter 변환 플러그인

## 설명

필드를 선택적으로 유지하거나 제거합니다.

## 옵션

| name           | type  | required | default |
|----------------|-------|----------|---------|
| include_fields | array | no       | -       |
| exclude_fields | array | no       | -       |

`include_fields`와 `exclude_fields`는 둘 중 하나만 지정해야 합니다.

### include_fields

유지할 필드 목록입니다. 목록에 없는 필드는 삭제됩니다.

### exclude_fields

삭제할 필드 목록입니다. 목록에 없는 필드는 유지됩니다.

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

`name`, `card`만 유지하려면 아래처럼 구성합니다.

```
transform {
  Filter {
    plugin_input = "fake"
    plugin_output = "fake1"
    include_fields = [name, card]
  }
}
```

`age`만 제거하려면 `exclude_fields`를 사용합니다.

```
transform {
  Filter {
    plugin_input = "fake"
    plugin_output = "fake1"
    exclude_fields = [age]
  }
}
```

결과 데이터 `fake1`

| name     | card |
|----------|------|
| Joy Ding | 123  |
| May Ding | 123  |
| Kin Dom  | 123  |
| Joy Dom  | 123  |

## 변경 이력

- Filter Transform 커넥터 추가
