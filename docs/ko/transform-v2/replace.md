# Replace

> Replace 변환 플러그인

## 설명

지정한 필드의 문자열에서 특정 문자열(또는 정규식)에 매칭되는 부분을 다른 문자열로 치환합니다.

## 옵션

| name          | type    | required | default |
|---------------|---------|----------|---------|
| replace_field | string  | yes      | -       |
| pattern       | string  | yes      | -       |
| replacement   | string  | yes      | -       |
| is_regex      | boolean | no       | false   |
| replace_first | boolean | no       | false   |

### replace_field
치환할 대상 필드 이름입니다.

### pattern
치환할 문자열(또는 정규식)입니다.

### replacement
새로 대체할 문자열입니다.

### is_regex
정규식을 사용할지 여부입니다.

### replace_first
`is_regex = true`일 때 첫 번째 매칭만 치환할지 여부입니다.

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

`name` 필드에서 공백을 `_`로 바꾸려면 아래처럼 설정합니다.

```
transform {
  Replace {
    plugin_input = "fake"
    plugin_output = "fake1"
    replace_field = "name"
    pattern = " "
    replacement = "_"
    is_regex = true
  }
}
```

결과 데이터 `fake1`

| name     | age | card |
|----------|-----|------|
| Joy_Ding | 20  | 123  |
| May_Ding | 20  | 123  |
| Kin_Dom  | 20  | 123  |
| Joy_Dom  | 20  | 123  |

## 잡 구성 예시

```
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
      }
    }
  }
}

transform {
  Replace {
    plugin_input = "fake"
    plugin_output = "fake1"
    replace_field = "name"
    pattern = ".+"
    replacement = "b"
    is_regex = true
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
}
```

## 변경 이력

- Replace Transform 추가
