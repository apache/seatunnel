# 구성 파일 소개

SeaTunnel에서 가장 중요한 요소는 구성(config) 파일입니다. 이 파일을 통해 사용자는 데이터 동기화 요구사항을 자유롭게 정의하여 SeaTunnel의 잠재력을 최대한 끌어낼 수 있습니다. 아래에서는 구성 파일을 어떻게 작성하고 설정하는지 설명합니다.

구성 파일의 기본 형식은 `hocon`입니다. 자세한 내용은 [HOCON 안내서](https://github.com/lightbend/config/blob/main/HOCON.md)를 참고하세요. 참고로 `json` 형식도 지원하지만, 이 경우 파일 이름이 반드시 `.json`으로 끝나야 합니다.

또한 `SQL` 형식의 설정도 지원합니다. 자세한 내용은 [SQL 구성](sql-config.md)을 확인하세요.

## 예제

본격적인 설명에 앞서, 바이너리 패키지의 config 디렉터리에서 제공하는 예제 구성 파일을 먼저 살펴보면 도움이 됩니다. 예제는 [여기](https://github.com/apache/seatunnel/tree/dev/seatunnel-e2e/seatunnel-connector-v2-e2e/connector-jdbc-e2e/connector-jdbc-e2e-part-1/src/test/resources)에서 확인할 수 있습니다.

## 구성 파일 구조

구성 파일은 아래 예제와 유사한 구조를 가집니다.

:::caution warn
이전 구성 키인 `source_table_name`/`result_table_name`은 더 이상 사용하지 않습니다. 가능한 빨리 새 이름인 `plugin_input`/`plugin_output`으로 마이그레이션하세요.
:::

### hocon 예시

```hocon
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 100
    schema = {
      fields {
        name = "string"
        age = "int"
        card = "int"
      }
    }
  }
}

transform {
  Filter {
    plugin_input = "fake"
    plugin_output = "fake1"
    fields = [name, card]
  }
}

sink {
  Clickhouse {
    host = "clickhouse:8123"
    database = "default"
    table = "seatunnel_console"
    fields = ["name", "card"]
    username = "default"
    password = ""
    plugin_input = "fake1"
  }
}
```

구성 파일은 기본적으로 `env`, `source`, `transform`, `sink` 섹션으로 이루어집니다. 각 모듈은 서로 다른 역할을 담당하며, 이 구조를 이해하면 SeaTunnel이 어떤 방식으로 동작하는지 파악할 수 있습니다.

### env

`env` 섹션은 엔진별 선택 매개변수를 설정하는 곳입니다. 어떤 엔진(Zeta, Spark, Flink)을 사용하더라도, 공통적으로 필요한 옵션은 이곳에 정의합니다. 엔진별 매개변수는 분리되어 있으며, 공통 매개변수는 기존과 동일한 방식으로 설정할 수 있습니다. Flink/Spark 엔진의 구체적인 규칙은 [JobEnvConfig](./JobEnvConfig.md)를 참고하세요.

### source

`source`는 SeaTunnel이 데이터를 읽어오는 위치와 방식을 정의합니다. 동시에 여러 개의 소스를 선언할 수도 있습니다. 지원되는 소스 목록은 [Source of SeaTunnel](../connector-v2/source)에서 확인할 수 있습니다. 각 소스는 고유한 설정값을 가지고 있으며, SeaTunnel은 자주 사용하는 공통 옵션도 제공합니다. 예를 들어 `plugin_output`은 해당 소스가 생성한 데이터의 이름을 지정하여 다른 모듈에서 참조할 때 사용합니다.

### transform

소스에서 데이터를 가져온 뒤 추가 가공이 필요하다면 `transform` 모듈을 사용합니다. 물론 가공이 필요 없으면 `transform`을 생략하고 `source`에서 바로 `sink`로 연결해도 됩니다. 아래 예시는 `transform`을 사용하지 않는 형태입니다.

```hocon
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 100
    schema = {
      fields {
        name = "string"
        age = "int"
        card = "int"
      }
    }
  }
}

sink {
  Clickhouse {
    host = "clickhouse:8123"
    database = "default"
    table = "seatunnel_console"
    fields = ["name", "age", "card"]
    username = "default"
    password = ""
    plugin_input = "fake"
  }
}
```

소스와 마찬가지로 `transform` 모듈도 각 플러그인마다 고유의 매개변수가 있습니다. 지원되는 변환 목록은 [Transform V2](../transform-v2)에서 확인하세요.

### sink

SeaTunnel의 목적은 데이터를 한 곳에서 다른 곳으로 동기화하는 것입니다. 따라서 데이터를 어디에, 어떻게 쓸지 결정하는 `sink` 정의가 매우 중요합니다. SeaTunnel이 제공하는 `sink` 모듈을 이용하면 이 작업을 빠르게 수행할 수 있습니다. 구조는 `source`와 유사하지만, 읽기 대신 쓰기를 담당한다는 점이 다릅니다. 지원되는 싱크 목록은 [Supported Sinks](../connector-v2/sink)를 확인하세요.

### 추가 정보

여러 개의 소스와 싱크를 정의했을 때 각 싱크가 어떤 데이터를 읽어오는지, 각 변환이 어떤 데이터를 처리하는지 궁금할 수 있습니다. 이를 위해 `plugin_output`과 `plugin_input`이라는 핵심 설정이 존재합니다. 모든 소스는 `plugin_output`으로 자신이 생성한 데이터의 이름을 정의하고, 다른 `transform` 및 `sink`는 `plugin_input`으로 원하는 데이터를 지정합니다. 변환 모듈은 중간 처리 역할을 하므로 두 옵션을 모두 동시에 사용할 수도 있습니다.

예시 구성에서 모든 모듈이 이 두 옵션을 명시적으로 설정하지 않은 이유는 SeaTunnel이 "이전 단계의 마지막 모듈이 생성한 데이터"를 기본값으로 사용하는 규칙을 갖고 있기 때문입니다. 소스가 하나뿐일 때 특히 편리합니다.

## 여러 줄 문자열 지원

`hocon`에서는 삼중 따옴표(**`"""`**)를 사용해 여러 줄 문자열을 쉽게 작성할 수 있습니다. 줄바꿈이나 특수문자 처리를 걱정하지 않고 긴 텍스트를 그대로 넣을 수 있습니다.

```
var = """
Apache SeaTunnel is a
next-generation high-performance,
distributed, massive data integration tool.
"""
sql = """ select * from "table" """
```

## JSON 형식 지원

구성 파일 이름이 `.json`으로 끝나는지 반드시 확인하세요.

```json
{
  "env": {
    "job.mode": "batch"
  },
  "source": [
    {
      "plugin_name": "FakeSource",
      "plugin_output": "fake",
      "row.num": 100,
      "schema": {
        "fields": {
          "name": "string",
          "age": "int",
          "card": "int"
        }
      }
    }
  ],
  "transform": [
    {
      "plugin_name": "Filter",
      "plugin_input": "fake",
      "plugin_output": "fake1",
      "fields": ["name", "card"]
    }
  ],
  "sink": [
    {
      "plugin_name": "Clickhouse",
      "host": "clickhouse:8123",
      "database": "default",
      "table": "seatunnel_console",
      "fields": ["name", "card"],
      "username": "default",
      "password": "",
      "plugin_input": "fake1"
    }
  ]
}
```

## 구성 변수 치환

구성 파일에서는 변수를 정의하고 실행 시점에 값을 치환할 수 있습니다. 단, 이 기능은 HOCON 형식에서만 지원됩니다.

### 변수 사용 방식
- `${varName}`: 변수가 전달되지 않으면 예외가 발생합니다.
- `${varName:default}`: 변수가 없으면 기본값을 사용합니다. 기본값을 지정할 때는 큰따옴표로 감싸세요.
- `${varName:}`: 변수가 없으면 빈 문자열을 사용합니다.

`-i` 옵션 대신 시스템 환경 변수로 값을 전달할 수도 있습니다. 예를 들어 쉘에서 아래와 같이 환경 변수를 지정하면:

```shell
export varName="value with space"
```

구성 파일에서 해당 변수를 사용할 수 있습니다.

기본값 없이 변수를 정의했는데 실행 시 값을 넘기지 않으면, 해당 변수는 그대로 남아도 시스템은 예외를 던지지 않습니다. 다만 다른 프로세스가 이 값을 이해할 수 있는지 확인해야 합니다. 예를 들어 ElasticSearch 인덱스는 `${xxx}` 형태의 동적 값을 지원해야 합니다. 지원하지 않는 경우 프로그램이 올바르게 동작하지 않을 수 있습니다.

### 예시
```hocon
env {
  job.mode = "BATCH"
  job.name = ${jobName}
  parallelism = 2
}

source {
  FakeSource {
    plugin_output = "${resName:fake_test}_table"
    row.num = "${rowNum:50}"
    string.template = ${strTemplate}
    int.template = [20, 21]
    schema = {
      fields {
        name = "${nameType:string}"
        age = ${ageType}
      }
    }
  }
}

transform {
    sql {
      plugin_input = "${resName:fake_test}_table"
      plugin_output = "sql"
      query = "select * from ${resName:fake_test}_table where name = '${nameVal}' "
    }

}

sink {
  Console {
     plugin_input = "sql"
     username = ${username}
     password = ${password}
  }
}
```

위 구성에서는 `${rowNum}`, `${resName}`와 같은 변수를 선언했습니다. 실행 시 아래와 같이 치환할 수 있습니다.

```shell
./bin/seatunnel.sh -c <config 파일 경로> \
-i jobName='this_is_a_job_name' \
-i strTemplate=['abc','d~f','hi'] \
-i ageType=int \
-i nameVal=abc \
-i username=seatunnel=2.3.1 \
-i password='$a^b%c.d~e0*9(' \
-m local
```

위 명령에서 `resName`, `rowNum`, `nameType`은 값을 전달하지 않았으므로 기본값이 사용됩니다.

최종적으로 제출되는 구성은 다음과 같습니다.

```hocon
env {
  job.mode = "BATCH"
  job.name = "this_is_a_job_name"
  parallelism = 2
}

source {
  FakeSource {
    plugin_output = "fake_test_table"
    row.num = 50
    string.template = ['abc','d~f','hi']
    int.template = [20, 21]
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

transform {
    sql {
      plugin_input = "fake_test_table"
      plugin_output = "sql"
      query = "select * from dual where name = 'abc' "
    }

}

sink {
  Console {
     plugin_input = "sql"
     username = "seatunnel=2.3.1"
     password = "$a^b%c.d~e0*9("
    }
}
```

### 주의 사항
- 값에 `(` 같은 특수 문자가 포함되면 작은따옴표(`'`)로 감싸세요.
- 치환 변수 안에 작은따옴표나 큰따옴표가 포함되어 있으면 그대로 값에 포함시켜야 합니다.
- 값에 공백은 사용할 수 없습니다. 예를 들어 `-i jobName='this is a job name'`은 `job.name = "this"`로 잘못 치환됩니다. 공백이 필요하면 환경 변수를 사용하세요.
- 동적 파라미터는 `-i date=$(date +"%Y%m%d")`처럼 사용할 수 있습니다.
- 시스템이 예약한 특정 키워드는 `-i`로 치환할 수 없습니다. 예: `${database_name}`, `${schema_name}`, `${table_name}`, `${schema_full_name}`, `${table_full_name}`, `${primary_key}`, `${unique_key}`, `${field_names}`. 자세한 내용은 [Sink 옵션 플레이스홀더](sink-options-placeholders.md)를 참고하세요.

## 더 알아보기

- 원하는 [커넥터](../connector-v2/source)를 선택해 직접 구성 파일을 작성해보세요. 각 커넥터 문서에 따라 파라미터를 설정하면 됩니다.
- 구성 형식에 대해 더 알고 싶다면 [HOCON](https://github.com/lightbend/config/blob/main/HOCON.md) 문서를 확인하세요.
