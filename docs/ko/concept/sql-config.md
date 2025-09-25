# SQL 구성 파일

SQL 구성 파일을 작성하기 전에 파일 이름이 반드시 `.sql`로 끝나는지 확인하세요.

## SQL 구성 파일 구조

`SQL` 구성 파일은 아래와 같은 형태를 가집니다.

### SQL

```sql
/* config
env {
  parallelism = 1
  job.mode = "BATCH"
}
*/

CREATE TABLE source_table WITH (
  'connector'='jdbc',
  'type'='source',
  'url' = 'jdbc:mysql://localhost:3306/seatunnel',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'user' = 'root',
  'password' = '123456',
  'query' = 'select * from source',
  'properties'= '{
    useSSL = false,
    rewriteBatchedStatements = true
  }'
);

CREATE TABLE sink_table WITH (
  'connector'='jdbc',
  'type'='sink',
  'url' = 'jdbc:mysql://localhost:3306/seatunnel',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'user' = 'root',
  'password' = '123456',
  'generate_sink_sql' = 'true',
  'database' = 'seatunnel',
  'table' = 'sink'
);

INSERT INTO sink_table SELECT id, name, age, email FROM source_table;
```

## `SQL` 구성 파일 설명

### SQL 파일의 공통 설정

```sql
/* config
env {
  parallelism = 1
  job.mode = "BATCH"
}
*/
```

`SQL` 파일에서는 `/* config */` 주석 블록을 사용해 공통 설정을 정의합니다. 이 블록 내부에서는 `env`처럼 공통 설정을 `HOCON` 형식으로 작성할 수 있습니다.

### SOURCE SQL 문법

```sql
CREATE TABLE source_table WITH (
  'connector'='jdbc',
  'type'='source',
  'url' = 'jdbc:mysql://localhost:3306/seatunnel',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'user' = 'root',
  'password' = '123456',
  'query' = 'select * from source',
  'properties' = '{
    useSSL = false,
    rewriteBatchedStatements = true
  }'
);
```

* `CREATE TABLE ... WITH (...)` 구문은 소스 테이블과 커넥터 설정을 매핑합니다. `TABLE` 이름이 소스 테이블을 대표하며, `WITH` 안에 소스 관련 파라미터를 정의합니다.
* `WITH` 절에는 고정 파라미터 두 가지가 있습니다. `connector`는 커넥터 플러그인 이름(`jdbc`, `FakeSource` 등), `type`은 소스 유형(항상 `source`)을 나타냅니다.
* 나머지 파라미터는 해당 커넥터의 설정 키를 참고하되, `'key' = 'value',` 형식으로 작성해야 합니다.
* 값이 서브 구성이라면 `HOCON` 문자열을 그대로 사용할 수 있습니다. 이때 내부 속성은 `,`로 구분해야 합니다.

```sql
'properties' = '{
  useSSL = false,
  rewriteBatchedStatements = true
}'
```

* 값 안에 `'` 문자가 필요하면 `''`처럼 두 번 연속 입력해 이스케이프합니다.

```sql
'query' = 'select * from source where name = ''Joy Ding'''
```

### SINK SQL 문법

```sql
CREATE TABLE sink_table WITH (
  'connector'='jdbc',
  'type'='sink',
  'url' = 'jdbc:mysql://localhost:3306/seatunnel',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'user' = 'root',
  'password' = '123456',
  'generate_sink_sql' = 'true',
  'database' = 'seatunnel',
  'table' = 'sink'
);
```

* `CREATE TABLE ... WITH (...)` 구문은 싱크 테이블 매핑을 만듭니다. 테이블 이름은 타깃 테이블을 나타내며, `WITH` 절 안에 싱크 관련 파라미터를 정의합니다.
* `WITH`에는 `connector`와 `type` 두 가지 고정 파라미터가 있고, 각각 커넥터 이름(`jdbc`, `console` 등)과 싱크 유형(항상 `sink`)을 의미합니다.
* 그 외 파라미터는 해당 커넥터의 설정 항목을 `'key' = 'value',` 형식으로 작성하세요.

### INSERT INTO SELECT 문법

```sql
INSERT INTO sink_table SELECT id, name, age, email FROM source_table;
```

* `SELECT ... FROM` 부분은 소스 테이블 이름입니다. 선택하는 필드가 SQL 키워드인 경우([참고](https://github.com/JSQLParser/JSqlParser/blob/master/src/main/jjtree/net/sf/jsqlparser/parser/JSqlParserCC.jjt)) 백틱(`\``)으로 감싸야 합니다.

```sql
INSERT INTO sink_table SELECT id, name, age, email,`output` FROM source_table;
```

* `INSERT INTO` 뒤에는 싱크 테이블 이름을 적습니다.
* 주의: `INSERT INTO sink_table (id, name, age, email) ...`처럼 필드를 지정하는 형태는 지원하지 않습니다.

### INSERT INTO SELECT TABLE 문법

```sql
INSERT INTO sink_table SELECT source_table;
```

* `SELECT` 뒤에 소스 테이블 이름을 그대로 사용하면, 소스 테이블 전체 데이터를 싱크 테이블로 삽입합니다.
* 이 구문은 `transform` 구성을 생성하지 않습니다. 주로 다중 테이블 동기화에 사용합니다.

```sql
CREATE TABLE source_table WITH (
  'connector'='jdbc',
  'type' = 'source',
  'url' = 'jdbc:mysql://127.0.0.1:3306/seatunnel',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'user' = 'root',
  'password' = '123456',
  'table_list' = '[
      {
        table_path = "source.table1"
      },
      {
        table_path = "source.table2",
        query = "select * from source.table2"
      }
    ]'
);

CREATE TABLE sink_table WITH (
  'connector'='jdbc',
  'type' = 'sink',
  'url' = 'jdbc:mysql://127.0.0.1:3306/seatunnel',
  'driver' = 'com.mysql.cj.jdbc.Driver',
  'user' = 'root',
  'password' = '123456',
  'generate_sink_sql' = 'true',
  'database' = 'sink'
);

INSERT INTO sink_table SELECT source_table;
```

### CREATE TABLE AS 문법

```sql
CREATE TABLE temp1 AS SELECT id, name, age, email FROM source_table;
```

* `SELECT` 결과를 임시 테이블로 생성한 뒤 `INSERT INTO`에 활용할 수 있습니다.
* `SELECT` 문법은 [SQL Transform](../transform-v2/sql.md)의 `query` 항목과 동일합니다.

```sql
CREATE TABLE temp1 AS SELECT id, name, age, email FROM source_table;

INSERT INTO sink_table SELECT * FROM temp1;
```

## SQL 구성 파일 실행 예시

```bash
./bin/seatunnel.sh --config ./config/sample.sql
```
