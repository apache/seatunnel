# Copy

> Copy 변환 플러그인

## 설명

필드를 복사해 새로운 필드를 생성합니다.

## 옵션

| name   | type   | required | default |
|--------|--------|----------|---------|
| fields | object | yes      | -       |

`fields`에 입력과 출력의 매핑 관계를 지정합니다.

### 공통 옵션
공통 파라미터는 [Transform 공통 옵션](common-options.md)을 참고하세요.

## 예제

`name`, `age`를 각각 `name1`, `name2`, `age1`으로 복사하려면 아래와 같이 설정합니다.

```
transform {
  Copy {
    plugin_input = "fake"
    plugin_output = "fake1"
    fields {
      name1 = name
      name2 = name
      age1 = age
    }
  }
}
```

결과 데이터 `fake1`

| name     | age | card | name1    | name2    | age1 |
|----------|-----|------|----------|----------|------|
| Joy Ding | 20  | 123  | Joy Ding | Joy Ding | 20   |
| May Ding | 20  | 123  | May Ding | May Ding | 20   |
| Kin Dom  | 20  | 123  | Kin Dom  | Kin Dom  | 20   |
| Joy Dom  | 20  | 123  | Joy Dom  | Joy Dom  | 20   |

## 변경 이력

- Copy Transform 추가
- 필드 복사 기능 지원
