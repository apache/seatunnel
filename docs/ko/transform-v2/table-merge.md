# TableMerge

> TableMerge 변환 플러그인

## 설명

샤딩된 여러 테이블을 하나의 논리 테이블로 병합할 때 사용합니다.

## 옵션

| name     | type   | required | default | 설명 |
|----------|--------|----------|---------|------|
| database | string | no       | -       | 병합 후 사용할 데이터베이스 이름 |
| schema   | string | no       | -       | 병합 후 사용할 스키마 이름 |
| table    | string | yes      | -       | 병합 후 사용할 테이블 이름 |

## 예제

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  MySQL-CDC {
    plugin_output = "customers_mysql_cdc"
    username = "root"
    password = "123456"
    table-names = ["source.user_1", "source.user_2", "source.shop"]
    url = "jdbc:mysql://localhost:3306/source"
  }
}

transform {
  TableMerge {
    plugin_input = "customers_mysql_cdc"
    plugin_output = "trans_result"
    table_match_regex = "source.user_.*"
    database = "user_db"
    table = "user_all"
  }
}

sink {
  jdbc {
    plugin_input = "trans_result"
    driver = "com.mysql.cj.jdbc.Driver"
    url = "jdbc:mysql://localhost:3306/sink"
    user = "myuser"
    password = "mypwd"
    generate_sink_sql = true
    database = "${database_name}"
    table = "${table_name}"
    primary_keys = ["${primary_key}"]
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```
