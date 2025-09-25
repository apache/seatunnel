import ChangeLog from '../changelog/connector-mongodb.md';

# MongoDB

> MongoDB 소스 커넥터

## 지원 엔진

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 주요 기능

- [x] 배치 모드
- [ ] 스트리밍
- [x] Exactly-Once
- [x] 컬럼 프로젝션
- [x] 병렬 처리
- [x] 사용자 정의 Split

## 설명

MongoDB에서 데이터를 읽고 쓰는 커넥터입니다. 여기서는 읽기 설정 방법을 설명합니다.

## 의존성

`connector-mongodb` 의존성을 install-plugin.sh 또는 Maven Central에서 다운로드해 `${SEATUNNEL_HOME}/plugins/`(Spark/Flink) 또는 `${SEATUNNEL_HOME}/lib/`(Zeta)에 배치하십시오.

## 데이터 타입 매핑

MongoDB BSON 타입은 Seatunnel 타입으로 다음과 같이 매핑됩니다: ObjectId→STRING, Boolean→BOOLEAN, Binary→BINARY, Int32→INTEGER, Int64→BIGINT, Double→DOUBLE, Decimal128→DECIMAL, Date→DATE, Timestamp→TIMESTAMP, Object→ROW, Array→ARRAY. 특수 타입(Symbol 등)은 확장 JSON 형태로 STRING에 매핑됩니다.

## 주요 옵션

| 옵션 | 타입 | 필수 | 기본값 | 설명 |
|------|------|------|--------|------|
| uri | String | Yes | - | MongoDB 표준 연결 URI (`mongodb://user:password@host:27017/db`) |
| database | String | Yes | - | 대상 DB 이름 |
| collection | String | Yes | - | 대상 컬렉션 이름 |
| schema | String | Yes | - | BSON→Seatunnel 스키마 매핑 |
| partition_strategy | String | No | sample | 파티션 전략 (`default`, `sample` 등) |
| partition_size_mb | Int | No | 64 | 파티션당 데이터 크기 |
| partition_key | String | No | `_id` | 파티션 키 |
| match.query | String | No | - | MongoDB Match 쿼리 필터 |
| project.fields | String | No | - | Projection 설정 |
| fetch.size | Int | No | 1000 | Cursor fetch 크기 |
| common-options | - | No | - | [공통 옵션](../common-options.md) |

## Match Query 사용

성능 향상을 위해 `match.query`로 읽을 데이터 범위를 축소하세요.

```hocon
source {
  MongoDB {
    uri = "mongodb://user:password@127.0.0.1:27017"
    database = "test_db"
    collection = "orders"
    match.query = "{status: \"A\"}"
    schema = {
      table = "orders"
      fields {
        "_id" = string
        "status" = string
        "amount" = double
      }
    }
  }
}
```

## URI 예시

- 단일 노드: `mongodb://host:27017/mydb`
- 복제 세트: `mongodb://host:27017/mydb?replicaSet=rs0`
- 인증: `mongodb://user:pwd@host:27017/mydb?authSource=admin`
- 다중 노드: `mongodb://host1:27017,host2:27017/mydb?replicaSet=rs0`

사용자명/비밀번호는 URL 인코딩 후 사용하세요.

## 변경 이력

<ChangeLog />
