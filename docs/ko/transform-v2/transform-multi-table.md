---
sidebar_position: 2
---

# SeaTunnel에서 다중 테이블 변환하기

SeaTunnel은 하나의 Transform 설정에서 여러 테이블을 처리할 수 있는 Multi-table Transform을 지원합니다. 상위 커넥터가 여러 테이블을 동시에 출력하는 경우(예: `JDBCSource`, `MySQL-CDC`)에 유용하며, 테이블별로 다른 변환을 구성하면서도 하나의 Transform 블록 안에 관리할 수 있습니다.

:::tip
멀티 테이블 변환은 변환 능력을 제한하지 않습니다. 모든 Transform 플러그인은 동일한 방식으로 멀티 테이블 환경에서 사용할 수 있으며, 목적은 여러 테이블을 개별적으로 처리하면서도 하나의 설정으로 관리하기 위함입니다.
:::

## 속성

| 이름                 | 타입  | 필수 | 기본값 | 설명 |
|----------------------|-------|------|--------|------|
| table_match_regex    | 문자열 | 아니오 | `.*` | 변환이 필요한 테이블을 매칭하는 정규식. 기본값은 모든 테이블 매칭. (실제 테이블 이름 기준) |
| table_transform      | 리스트 | 아니오 | - | 특정 테이블에만 적용할 별도 규칙 목록. 지정된 테이블은 외부 규칙보다 우선합니다. |
| table_transform.table_path | 문자열 | 아니오 | - | `database[.schema].table` 형식의 테이블 경로를 명시해야 합니다. |

## 매칭 로직 예시

상위에서 `test.abc`, `test.abcd`, `test.xyz`, `test.xyzxyz`, `test.www` 다섯 개 테이블이 들어오고 각각 `id`, `name`, `age` 필드를 갖는다고 가정합니다.

요구 사항:
- `test.abc`, `test.abcd`: `name` → `name1`
- `test.xyz`: `name` → `name2`
- `test.xyzxyz`: `name` → `name3`
- `test.www`: 변환 없음

설정 예시:

```hocon
transform {
  Copy {
    plugin_input = "fake"
    plugin_output = "fake1"

    table_match_regex = "test.a.*"
    src_field = "name"
    dest_field = "name1"

    table_transform = [
      {
        table_path = "test.xyz"
        src_field = "name"
        dest_field = "name2"
      },
      {
        table_path = "test.xyzxyz"
        src_field = "name"
        dest_field = "name3"
      }
    ]
  }
}
```

우선순위: `table_transform` > `table_match_regex`. 어떤 규칙에도 매칭되지 않으면 변환이 적용되지 않습니다.

이 방식은 Copy 외 다른 Transform에도 동일하게 적용할 수 있습니다.
