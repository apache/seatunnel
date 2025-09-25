---
sidebar_position: 2
---

# SeaTunnel Engine 빠른 시작

## 1단계: SeaTunnel과 커넥터 배포

작업을 시작하기 전에 [배포](deployment.md) 문서에 따라 SeaTunnel과 필요한 커넥터를 다운로드하고 배포했는지 확인하세요.

## 2단계: 작업을 정의하는 설정 파일 추가

SeaTunnel이 시작된 뒤 데이터 입력·처리·출력 방식을 결정하는 파일 `config/v2.batch.config.template`를 수정합니다.
아래 예시는 앞서 소개한 샘플 애플리케이션과 동일한 구성입니다.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 16
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

transform {
  FieldMapper {
    plugin_input = "fake"
    plugin_output = "fake1"
    field_mapper = {
      age = age
      name = new_name
    }
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
}

```

구성 옵션에 대한 자세한 내용은 [구성 기본 개념](../../concept/config.md)을 참고하세요.

## 3단계: SeaTunnel 애플리케이션 실행

다음 명령으로 애플리케이션을 실행할 수 있습니다.

:::tip

2.3.1 버전부터 `seatunnel.sh`의 `-e` 옵션은 더 이상 사용되지 않으니, 대신 `-m` 옵션을 사용하세요.

:::

```shell
cd "apache-seatunnel-${version}"
./bin/seatunnel.sh --config ./config/v2.batch.config.template -m local

```

**출력 확인**: 명령을 실행하면 콘솔에 출력이 표시됩니다. 이 결과로 명령이 정상 실행됐는지 확인할 수 있습니다.

SeaTunnel 콘솔에는 다음과 같은 로그가 출력됩니다.

```shell
2022-12-19 11:01:45,417 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - output rowType: name<STRING>, age<INT>
2022-12-19 11:01:46,489 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=1:  SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: CpiOd, 8520946
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=2: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: eQqTs, 1256802974
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=3: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: UsRgO, 2053193072
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=4: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: jDQJj, 1993016602
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=5: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: rqdKp, 1392682764
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=6: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: wCoWN, 986999925
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=7: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: qomTU, 72775247
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=8: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: jcqXR, 1074529204
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=9: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: AkWIO, 1961723427
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=10: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: hBoib, 929089763
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=11: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: GSvzm, 827085798
2022-12-19 11:01:46,491 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=12: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: NNAYI, 94307133
2022-12-19 11:01:46,491 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=13: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: EexFl, 1823689599
2022-12-19 11:01:46,491 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=14: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: CBXUb, 869582787
2022-12-19 11:01:46,491 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=15: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: Wbxtm, 1469371353
2022-12-19 11:01:46,491 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=16: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: mIJDt, 995616438
```

## 확장 예시: MySQL에서 Doris로 배치 모드 전송

### 1단계: 커넥터 다운로드

먼저 `${SEATUNNEL_HOME}/config/plugin_config` 파일에 사용할 커넥터 이름을 추가한 뒤, 설치 명령을 실행합니다. (또는 [Apache Maven 저장소](https://repo.maven.apache.org/maven2/org/apache/seatunnel/)에서 커넥터를 직접 내려받아 `connectors/` 디렉터리에 배치해도 됩니다.) 마지막으로 `${SEATUNNEL_HOME}/connectors/` 디렉터리에 `connector-jdbc`와 `connector-doris`가 존재하는지 확인하세요.

```bash
# 커넥터 이름 구성
--seatunnel-connectors--
connector-jdbc
connector-doris
--end--
```

```bash
# 커넥터 설치
sh bin/install-plugin.sh
```

### 2단계: MySQL 드라이버 배치

[JDBC 드라이버 JAR](https://mvnrepository.com/artifact/mysql/mysql-connector-java)을 다운로드해 `${SEATUNNEL_HOME}/lib/` 디렉터리에 넣습니다.

### 3단계: 작업 정의용 설정 파일 추가

```bash
cd seatunnel/job/

vim st.conf

env {
  parallelism = 2
  job.mode = "BATCH"
}
source {
    Jdbc {
        url = "jdbc:mysql://localhost:3306/test"
        driver = "com.mysql.cj.jdbc.Driver"
        connection_check_timeout_sec = 100
        user = "user"
        password = "pwd"
        table_path = "test.table_name"
        query = "select  * from test.table_name"
    }
}

sink {
   Doris {
          fenodes = "doris_ip:8030"
          username = "user"
          password = "pwd"
          database = "test_db"
          table = "table_name"
          sink.enable-2pc = "true"
          sink.label-prefix = "test-cdc"
          doris.config = {
            format = "json"
            read_json_by_line="true"
          }
      }
}
```

구성 항목에 대한 자세한 설명은 [구성 기본 개념](../../concept/config.md)을 참고하세요.

### 4단계: SeaTunnel 애플리케이션 실행

다음 명령으로 애플리케이션을 실행합니다.

```shell
cd seatunnel/
./bin/seatunnel.sh --config ./job/st.conf -m local

```

**출력 확인**: 명령 실행 후 콘솔에 표시되는 결과로 성공 여부를 확인할 수 있습니다.

SeaTunnel 콘솔에는 다음과 같은 로그가 출력됩니다.

```shell
***********************************************
           Job Statistic Information
***********************************************
Start Time                : 2024-08-13 10:21:49
End Time                  : 2024-08-13 10:21:53
Total Time(s)             :                   4
Total Read Count          :                1000
Total Write Count         :                1000
Total Failed Count        :                   0
***********************************************
```

:::tip

작업을 최적화하고 싶다면 [Source-MySQL](../../connector-v2/source/Mysql.md)과 [Sink-Doris](../../connector-v2/sink/Doris.md) 커넥터 문서를 참고하세요.

:::

## 더 알아보기

- 지금 바로 자신만의 설정 파일을 작성해 보세요. 사용할 [커넥터](../../connector-v2/source)를 고르고, 해당 커넥터 문서에 따라 매개변수를 구성하면 됩니다.
- SeaTunnel Engine에 대해 더 알고 싶다면 [SeaTunnel Engine(Zeta)](../../seatunnel-engine/about.md) 문서를 확인하세요. 클러스터 모드 배포와 활용 방법을 배울 수 있습니다.

