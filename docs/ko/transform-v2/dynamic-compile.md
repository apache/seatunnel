# DynamicCompile

> DynamicCompile 변환 플러그인

## 설명

:::tip
서비스 보안을 반드시 확인하고, 악성 코드 업로드를 차단하세요.
:::

행 단위로 커스텀 코드를 실행해 원하는 로직을 구현할 수 있습니다. RPC 호출, 외부 데이터 조회 등 복잡한 처리도 가능하지만, 복잡도가 높아지면 성능에 영향을 줄 수 있습니다.

## 옵션

| name            | type   | required | default | 설명 |
|-----------------|--------|----------|---------|------|
| compile_language | Enum   | yes      | -       | 컴파일 언어(`GROOVY`, `JAVA`, `SCALA`(Zeta 전용)) |
| compile_pattern  | Enum   | no       | SOURCE_CODE | 코드 로딩 방식(`SOURCE_CODE`, `ABSOLUTE_PATH`) |
| source_code      | string | no       | -       | 소스 코드(패턴이 `SOURCE_CODE`일 때 필요) |
| absolute_path    | string | no       | -       | 코드 파일 절대 경로(패턴이 `ABSOLUTE_PATH`일 때 필요) |

### 구현 방법

소스 코드에는 다음 메서드를 구현해야 합니다.

- `Column[] getInlineOutputColumns(CatalogTable inputCatalogTable)`
- `Object[] getInlineOutputFieldValues(SeaTunnelRowAccessor inputRow)`

`getInlineOutputColumns`는 출력 컬럼 정의를 반환하며, 기존 스키마를 덮어쓰거나 새 컬럼을 추가할 수 있습니다. `getInlineOutputFieldValues`는 입력 행을 가공해 출력 값을 반환합니다.

외부 의존성이 필요하면 `${SEATUNNEL_HOME}/lib`(Spark/Flink는 각 엔진 libs) 경로에 JAR을 배치하고 서버를 재시작하세요.
