# SQL

> SQL 변환 플러그인

## 설명

SQL을 사용해 입력 데이터를 변환합니다. 메모리 기반 SQL 엔진을 활용해 함수와 조건을 적용할 수 있습니다.

## 옵션

| name          | type   | required | default |
|---------------|--------|----------|---------|
| plugin_input  | string | yes      | -       |
| plugin_output | string | yes      | -       |
| query         | string | yes      | -       |

### plugin_input

쿼리 대상 테이블 이름입니다. SQL 구문에서 사용하는 테이블 이름과 일치해야 합니다.

### query

실행할 SQL. 기본 함수나 조건 필터는 지원하지만 복잡한 다중 테이블 조인, 집계 등은 아직 지원하지 않습니다. `select [table_name.]column_a`처럼 테이블 접두사를 붙여 특정 컬럼을 조회하거나, `select c_row.c_inner_row.column_b`처럼 중첩 구조 내 컬럼을 조회할 수 있습니다. 단, 중첩 구조를 조회할 때는 테이블 이름을 붙일 수 없습니다.

## 예제

소스 데이터

| id | name      | age |
|----|-----------|-----|
| 1  | Joy Ding  | 20  |
| 2  | May Ding  | 21  |
| 3  | Kin Dom   | 24  |
| 4  | Joy Dom   | 22  |

SQL 변환 구성

```
transform {
  Sql {
    plugin_input = "fake"
    plugin_output = "fake1"
    query = "select id, concat(name, '_') as name, age+1 as age from dual where id>0"
  }
}
```

결과 데이터

| id | name       | age |
|----|------------|-----|
| 1  | Joy Ding_  | 21  |
| 2  | May Ding_  | 22  |
| 3  | Kin Dom_   | 25  |
| 4  | Joy Dom_   | 23  |

### Struct 조회

중첩 구조를 가진 데이터에서도 아래와 같은 SQL이 허용됩니다.

```sql
select
name,
c_date,
c_row,
c_row.c_inner_row,
c_row.c_string,
c_row.c_inner_row.c_inner_int,
c_row.c_inner_row.c_inner_string,
c_row.c_inner_row.c_inner_timestamp,
c_row.c_inner_row.c_map_1,
c_row.c_inner_row.c_map_1.some_key
```

단, 맵 안의 맵처럼 더 깊은 중첩은 지원되지 않습니다.

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
        age = "int"
      }
    }
  }
}

transform {
  Sql {
    plugin_input = "fake"
    plugin_output = "fake1"
    query = "select id, concat(name, '_') as name, age+1 as age from dual where id>0"
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
}
```

## 변경 이력

- Struct 조회 지원
- SQL Transform 커넥터 추가
