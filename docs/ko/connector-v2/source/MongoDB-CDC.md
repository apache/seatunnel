import ChangeLog from '../changelog/connector-cdc-mongodb.md';

# MongoDB CDC

> MongoDB CDC 소스 커넥터

## 지원 엔진

> SeaTunnel Zeta<br/>
> Flink<br/>

## 주요 기능

- [ ] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [user-defined split](../../concept/connector-v2-features.md)

## 설명

MongoDB CDC 커넥터는 MongoDB에서 스냅샷 데이터와 증분 변경 데이터를 모두 읽어올 수 있습니다.

## 지원 데이터 소스 정보

커넥터를 사용하려면 다음 의존성이 필요합니다. `install-plugin.sh`로 설치하거나 Maven Central에서 직접 받을 수 있습니다.

| Datasource | Version | Dependency |
|------------|---------|------------|
| MongoDB    | universal | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-cdc-mongodb) |

## 사용 조건

1. MongoDB 버전 4.0 이상<br/>
2. 복제 세트(replica set) 또는 샤딩 클러스터<br/>
3. WiredTiger 스토리지 엔진<br/>
4. 권한: changeStream, read

```shell
use admin;
db.createRole({
  role: "strole",
  privileges: [{
    resource: { db: "", collection: "" },
    actions: ["splitVector", "listDatabases", "listCollections", "collStats", "find", "changeStream"]
  }],
  roles: [{ role: 'read', db: 'config' }]
});

db.createUser({
  user: 'stuser',
  pwd: 'stpw',
  roles: [{ role: 'strole', db: 'admin' }]
});
```

## 데이터 타입 매핑

MongoDB BSON 타입과 SeaTunnel 타입 매핑은 아래와 같습니다.

| MongoDB BSON Type | SeaTunnel Type |
|-------------------|----------------|
| ObjectId          | STRING         |
| String            | STRING         |
| Boolean           | BOOLEAN        |
| Binary            | BINARY         |
| Int32             | INTEGER        |
| Int64             | BIGINT         |
| Double            | DOUBLE         |
| Decimal128        | DECIMAL        |
| Date              | DATE           |
| Timestamp         | TIMESTAMP      |
| Object            | ROW            |
| Array             | ARRAY          |

특수 타입은 Extended JSON 포맷으로 STRING에 매핑합니다.

| BSON Type         | SeaTunnel STRING 예시 |
|-------------------|------------------------|
| Symbol            | {"_value": {"$symbol": "12"}} |
| RegularExpression | {"_value": {"$regularExpression": {"pattern": "^9$", "options": "i"}}} |
| JavaScript        | {"_value": {"$code": "function() { return 10; }"}} |
| DbPointer         | {"_value": {"$dbPointer": {"$ref": "db.coll", "$id": {"$oid": "63932a00da01604af329e33c"}}}} |

**팁**<br/>
1. SeaTunnel DECIMAL 사용 시 최대 34자리까지 지원하므로 `decimal(34, 18)` 범위를 넘지 않도록 주의하세요.

## 소스 옵션

| Name | Type | Required | Description |
|------|------|----------|-------------|
| hosts | String | Yes | `host:port` 목록. 예) `localhost:27017,localhost:27018` |
| username | String | No | MongoDB 사용자 이름 |
| password | String | No | MongoDB 사용자 비밀번호 |
| database | List | No | 수집할 DB 목록. 예) `["inventory"]` |
| collection | List | No | 수집할 컬렉션 목록. 예) `["inventory.products"]` |
| connection.options | String | No | 추가 연결 옵션. 예) `tls=true&connectTimeoutMS=300000` |
| initial.sync.mode | String | No | 초기 스냅샷 전략(`initial`/`recovery`/`none`) |
| batch.size | Integer | No | 스냅샷 커서 fetch 크기 |
| poll.max.batch.size | Integer | No | 변경 이벤트를 한 번에 가져오는 최대 수 |
| poll.await.time.ms | Integer | No | 변경 이벤트 대기 시간(ms) |
| heartbeat.interval.ms | Integer | No | 하트비트 전송 주기(ms) |
| schema | Config | No | 단일 테이블 매핑 스키마 |
| tables_configs | List | No | 다중 테이블 설정. `schema`와 상호 배타적 |
| fetch.lob.data | Boolean | No | LOB(Binary) 데이터 포함 여부 |
| split.mode | String | No | 스냅샷 분할 방식(`partition`/`collection`) |
| close.id.lead | Integer | No | 분할 기준으로 사용할 ObjectId 리드 |
| stream.offset | String | No | 스트림 재시작 시 resumeToken |
| debezium | Map | No | Debezium 커스텀 설정 |

> 1. 변경이 빈번하지 않은 컬렉션은 `heartbeat.interval.ms`를 0보다 크게 설정해 resumeToken 만료를 방지하세요.<br/>
> 2. MongoDB는 문서 크기 16MB 제한이 있으므로 Change Stream 문서도 이 크기를 넘지 않도록 주의하세요.<br/>
> 3. 불변 샤드 키 사용을 권장합니다. 샤드 키 변경은 성능 저하 및 CDC 불일치를 유발할 수 있습니다.<br/>
> 4. `schema`와 `tables_configs`는 동시에 설정할 수 없습니다.

## Change Streams

[Change Stream](https://www.mongodb.com/docs/v5.0/changeStreams/)은 복제 세트/샤딩 클러스터에서 실시간 변경 이벤트를 제공하는 기능입니다. Update 시 최신 전체 문서를 조회하려면 Lookup 옵션을 사용하면 됩니다.

삭제 이벤트 포맷 예시:
```
{
  "_id": {...},
  "operationType": "delete",
  "clusterTime": <Timestamp>,
  "ns": {"db": "engineering", "coll": "users"},
  "documentKey": {"_id": ObjectId("...")}
}
```

## MongoDB CDC 작업 예시

### 1) CDC 데이터 콘솔 출력

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MongoDB-CDC {
    hosts = "mongo0:27017"
    database = ["inventory"]
    collection = ["inventory.products"]
    username = stuser
    password = stpw
    schema {
      table = "inventory.products"
      fields {
        "_id" : string
        "name" : string
        "description" : string
        "weight" : string
      }
    }
  }
}

sink {
  Console {
    parallelism = 1
  }
}
```

### 2) CDC 데이터 MySQL 저장

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MongoDB-CDC {
    hosts = "mongo0:27017"
    database = ["inventory"]
    collection = ["inventory.products"]
    username = stuser
    password = stpw
    schema {
      table = "inventory.products"
      fields {
        "_id" : string
        "name" : string
        "description" : string
        "weight" : string
      }
    }
  }
}

sink {
  jdbc {
    url = "jdbc:mysql://mysql_cdc_e2e:3306"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "st_user"
    password = "seatunnel"
    generate_sink_sql = true
    database = mongodb_cdc
    table = products
    primary_keys = ["_id"]
  }
}
```

### 3) 다중 테이블 동기화

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MongoDB-CDC {
    hosts = "mongo0:27017"
    database = ["inventory"]
    collection = ["inventory.products", "inventory.orders"]
    username = superuser
    password = superpw
    tables_configs = [
      {
        schema {
          table = "inventory.products"
          fields {
            "_id" : string
            "name" : string
            "description" : string
            "weight" : string
          }
        }
      },
      {
        schema {
          table = "inventory.orders"
          fields {
            "_id" : string
            "order_number" : int
            "order_date" : string
            "quantity" : int
            "product_id" : string
          }
        }
      }
    ]
  }
}

sink {
  Console {}
}
```

## 실시간 스트림 데이터 포맷

```json
{
  "_id": { ... },
  "operationType": "insert/delete/update",
  "fullDocument": { ... },
  "ns": { "db": "<database>", "coll": "<collection>" },
  "to": { "db": "<database>", "coll": "<collection>" },
  "source": {
    "ts_ms": "<timestamp>",
    "table": "<collection>",
    "db": "<database>",
    "snapshot": "false"
  },
  "documentKey": { "_id": <value> },
  "updateDescription": {
    "updatedFields": { ... },
    "removedFields": [ ... ]
  },
  "clusterTime": <Timestamp>,
  "txnNumber": <NumberLong>,
  "lsid": { "id": <UUID>, "uid": <BinData> }
}
```

## 변경 이력

<ChangeLog />
