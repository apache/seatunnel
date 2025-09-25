---
sidebar_position: 1
---

# Transform 공통 옵션

> Transform는 Source와 Sink 사이에서 데이터를 가공하는 중간 단계입니다. SQL을 사용하면 변환을 간단히 수행할 수 있습니다.

:::caution warn
기존 `source_table_name`/`result_table_name` 설정은 더 이상 사용되지 않으므로 `plugin_input`/`plugin_output`으로 마이그레이션하세요.
:::

| Name          | Type   | Required | Default | Description |
|---------------|--------|----------|---------|-------------|
| plugin_output | String | No       | -       | `plugin_input`을 지정하지 않으면, 이전 플러그인이 출력한 데이터셋을 그대로 처리합니다.<br/>`plugin_input`을 지정하면, 해당 데이터셋을 읽어 처리합니다. |
| plugin_input  | String | No       | -       | `plugin_output`을 지정하지 않으면, 처리 결과가 다른 플러그인이 접근할 수 있는 데이터셋으로 등록되지 않습니다.<br/>`plugin_output`을 지정하면, 처리 결과를 다른 플러그인이 접근 가능한 데이터셋(임시 테이블)으로 등록합니다. |

## 작업 예시

### 간단 예시

다음 예시는 데이터를 읽어 SQL 변환 후 두 개의 Sink에 출력합니다.

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
        age = "int"
        c_timestamp = "timestamp"
        c_date = "date"
        c_map = "map<string, string>"
        c_array = "array<int>"
        c_decimal = "decimal(30, 8)"
        c_row = {
          c_row = {
            c_int = int
          }
        }
      }
    }
  }
}

transform {
  Sql {
    plugin_input = "fake"
    plugin_output = "fake1"
    query = "select id, regexp_replace(name, '.+', 'b') as name, age+1 as age, pi() as pi, c_timestamp, c_date, c_map, c_array, c_decimal, c_row from dual"
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
  Console {
    plugin_input = "fake"
  }
}
```
