# FilterRowKind

> FilterRowKind 변환 플러그인

## 설명

RowKind 값(INSERT, UPDATE_BEFORE 등)에 따라 데이터를 필터링합니다.

## 옵션

| name          | type  | required | default |
|---------------|-------|----------|---------|
| include_kinds | array | yes      | -       |
| exclude_kinds | array | yes      | -       |

`include_kinds`와 `exclude_kinds` 중 하나만 설정할 수 있습니다.

### 공통 옵션
공통 파라미터는 [Transform 공통 옵션](common-options.md)을 참고하세요.

## 예제

FakeSource가 생성하는 기본 RowKind는 `INSERT`입니다. 아래 예시는 `INSERT`를 제외해 싱크에 아무 데이터도 쓰지 않습니다.

```hocon
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 100
    schema = {
      fields {
        id = "int"
        name = "string"
        age = "int"
      }
    }
  }
}

transform {
  FilterRowKind {
    plugin_input = "fake"
    plugin_output = "fake1"
    exclude_kinds = ["INSERT"]
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
}
```
