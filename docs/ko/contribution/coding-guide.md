# 코딩 가이드

이 문서는 Apache SeaTunnel의 주요 모듈 개요와, 높은 품질의 Pull Request(PR)를 제출하기 위한 모범 사례를 정리했습니다.

## 모듈 개요

| 모듈 이름                               | 소개                                                                                                                                      |
|------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------| 
| seatunnel-api                            | SeaTunnel Connector V2 API 모듈                                                                                                         |
| seatunnel-common                         | SeaTunnel 공통 모듈                                                                                                                      |
| seatunnel-connectors-v2                  | SeaTunnel Connector V2 모듈 (현재 V2 커넥터 개발에 커뮤니티 역량을 집중)                                                                 |
| seatunnel-core/seatunnel-spark-starter   | Spark 엔진에서 Connector V2를 실행하기 위한 코어 스타터 모듈                                                                            |
| seatunnel-core/seatunnel-flink-starter   | Flink 엔진에서 Connector V2를 실행하기 위한 코어 스타터 모듈                                                                            |
| seatunnel-core/seatunnel-starter         | SeaTunnel 엔진에서 Connector V2를 실행하기 위한 코어 스타터 모듈                                                                         |
| seatunnel-e2e                            | SeaTunnel End-to-End 테스트 모듈                                                                                                         |
| seatunnel-examples                       | SeaTunnel 로컬 예제 모듈 (개발자가 단위 테스트 및 통합 테스트에 활용)                                                                    |
| seatunnel-engine                         | SeaTunnel 엔진 모듈 (SeaTunnel 커뮤니티가 데이터 동기화에 특화해 개발한 신규 계산 엔진)                                                    |
| seatunnel-formats                        | 데이터 포맷 기능을 제공하는 모듈                                                                                                        |
| seatunnel-plugin-discovery               | 클래스패스에서 SPI 플러그인을 로딩하는 기능을 제공하는 모듈                                                                             |
| seatunnel-transforms-v2                  | SeaTunnel Transform V2 모듈 (현재 개발 중이며 커뮤니티가 집중하는 영역)                                                                     |
| seatunnel-translation                    | Connector V2를 Spark·Flink 등 다른 계산 엔진과 연동하기 위한 어댑터 모듈                                                                |

## 높은 품질의 Pull Request를 만드는 방법

1. `lombok` 플러그인의 애너테이션(`@Data`, `@Getter`, `@Setter`, `@NonNull` 등)을 적극 활용해 보일러플레이트 코드를 줄이세요.
2. 클래스에서 로그를 출력할 때는 `@Slf4j` 애너테이션 사용을 권장합니다.
3. SeaTunnel은 Issue로 버그/개선 사항을 추적하고, GitHub PR로 코드 리뷰와 병합을 처리합니다. 명확한 Issue/PR 제목을 사용하면 의도를 전달하기 쉬워집니다.
   > `[목적] [모듈명] [서브모듈명] 설명`
   - PR 목적: `Hotfix`, `Feature`, `Improve`, `Docs`, `WIP` (WIP라면 Draft PR 사용)
   - Issue 목적: `Feature`, `Bug`, `Docs`, `Discuss`
   - 모듈명 예시: `Core`, `Connector-V2`, `Connector-V1`
   - 서브모듈 예시: `File`, `Redis`, `Hbase`
   - 설명: 핵심 목표를 한눈에 이해할 수 있도록 간결하고 명확하게 작성

   추가 정보는 [Issue 가이드](https://seatunnel.apache.org/community/contribution_guide/contribute#issue), [Pull Request 가이드](https://seatunnel.apache.org/community/contribution_guide/contribute#pull-request)를 참고하세요.

4. 동일 코드가 반복된다면 공용 메서드나 유틸 클래스로 추출하세요.
5. 예외를 던질 때는 의미 있는 메시지를 포함하고, 가능한 한 좁은 범위의 예외를 사용하세요. 예를 들어:
   ```java
   try {
       // read logic
   } catch (IOException e) {
       throw SeaTunnelORCFormatException("This orc file is corrupted, please check it", e);
   }
   ```
6. 모든 신규 파일에는 Apache 라이선스 헤더가 포함되어야 합니다.
7. 코드 스타일/포맷 검사는 `Spotless`로 관리합니다. 필요 시 아래 명령으로 자동 정리하세요.
   ```shell
   ./mvnw spotless:apply
   ```
8. PR 제출 전에 프로젝트가 정상 빌드되는지 확인하세요.
   ```shell
   # 멀티 스레드 빌드
   ./mvnw -T 1C clean package

   # 싱글 스레드 빌드
   ./mvnw clean package
   ```
9. 로컬에서 단위 테스트와 통합 테스트를 수행해 기능을 검증하세요. `seatunnel-examples` 모듈을 활용하면 다중 엔진 테스트를 쉽게 진행할 수 있습니다.
10. 기능 변경 시 문서도 함께 업데이트해야 합니다.
11. 커넥터 관련 PR은 e2e 테스트를 작성해 안정성을 검증하세요. 데이터 타입을 폭넓게 다루고, 가능하면 하나의 Docker 이미지로 sink/source 케이스를 함께 커버하며, 비동기 기능을 활용해 테스트 안정성을 확보하세요. 예시는 [MongodbIT.java](https://github.com/apache/seatunnel/blob/dev/seatunnel-e2e/seatunnel-connector-v2-e2e/connector-mongodb-e2e/src/test/java/org/apache/seatunnel/e2e/connector/v2/mongodb/MongodbIT.java)를 참고하세요.
12. 클래스 필드 접근 제어자는 기본적으로 `private`, 변경 가능성은 `final`을 우선합니다. 특별한 경우에만 예외적으로 조정하세요.
13. 클래스 필드와 메서드 파라미터는 기본 타입(int, boolean, double, float 등)을 우선 사용하고, 필요할 때만 래퍼 타입을 사용하세요.
14. Sink 커넥터 개발 시 객체가 직렬화될 수 있음을 염두에 두고, 직렬화가 어려운 속성은 별도 클래스로 감싼 뒤 싱글턴 패턴 등을 활용하세요.
15. 조건 분기에서는 `if-else-if` 체인을 최소화하고, 가능한 단순한 여러 `if` 블록으로 흐름을 구성하세요.
16. PR은 단일 책임 원칙을 지켜야 합니다. 관련 없는 변경이 섞여 있으면 커뮤니티가 PR을 닫을 수 있습니다.
17. 신규 기능 추가나 기존 기능 변경 시에는 적절한 테스트(단위 또는 e2e)를 함께 제출해 기능과 안정성을 증명하세요.
18. 커뮤니티 코드(특히 `core`, `api`)에서 개선이 필요하다고 판단되면, 먼저 `discuss` Issue나 메일로 의견을 공유한 뒤 합의가 이뤄져야 PR을 제출할 수 있습니다. 사전 논의 없이 제출된 PR은 폐기될 수 있습니다.
