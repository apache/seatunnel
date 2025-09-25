# RegexExtract

> RegexExtract 변환 플러그인

## 설명

정규식을 이용해 지정한 필드에서 값을 추출하고, 캡처 그룹에 해당하는 결과를 새로운 필드에 매핑합니다. 패턴이 일치하지 않을 경우 기본값을 설정할 수도 있습니다.

## 옵션

| name           | type   | required | default |
|----------------|--------|----------|---------|
| source_field   | string | yes      | -       |
| regex_pattern  | string | yes      | -       |
| output_fields  | array  | yes      | -       |
| default_values | array  | no       | -       |

- **source_field**: 정규식을 적용할 원본 필드 이름
- **regex_pattern**: 캡처 그룹을 포함한 정규식 패턴. 캡처 그룹 수는 `output_fields` 수와 일치해야 함
- **output_fields**: 추출 결과를 담을 새 필드 이름 목록
- **default_values**: 패턴 불일치 또는 null일 때 사용할 기본값(옵션)

### 공통 옵션
공통 파라미터는 [Transform 공통 옵션](common-options.md)을 참고하세요.

## 예제

이메일에서 사용자명과 도메인, 최상위 도메인을 추출하는 예시입니다.

```
transform {
  RegexExtract {
    plugin_input = "fake"
    plugin_output = "regex_result"
    source_field = "email"
    regex_pattern = "([^@]+)@([^.]+)\\.(.+)"
    output_fields = ["username", "domain", "tld"]
    default_values = ["unknown", "unknown", "unknown"]
  }
}
```

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
        email = "string"
        log_entry = "string"
      }
    }
    rows = [
      { kind = INSERT, fields = [1, "user1@example.com", "2023-12-01 10:30:45 INFO User login successful"] },
      { kind = INSERT, fields = [2, "admin@test.org", "2023-12-01 11:15:22 ERROR Database connection failed"] },
      { kind = INSERT, fields = [3, "guest@domain.net", "2023-12-01 12:00:00 WARN Memory usage high"] }
    ]
  }
}

transform {
  RegexExtract {
    plugin_input = "fake"
    plugin_output = "regex_result"
    source_field = "email"
    regex_pattern = "([^@]+)@([^.]+)\\.(.+)"
    output_fields = ["username", "domain", "tld"]
    default_values = ["unknown", "unknown", "unknown"]
  }
}

sink {
  Console {
    plugin_input = "regex_result"
  }
}
```

## 변경 이력

(업데이트 예정)
