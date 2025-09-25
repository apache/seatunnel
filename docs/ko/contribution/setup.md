# 개발 환경 설정

이 문서는 SeaTunnel 개발 환경을 구성하고 JetBrains IntelliJ IDEA에서 간단한 예제를 실행하는 방법을 안내합니다.

> 선호하는 어떤 개발 환경에서도 SeaTunnel을 개발하거나 테스트할 수 있습니다. 여기서는 [JetBrains IDEA](https://www.jetbrains.com/idea/)를 예시로 단계별 과정을 설명합니다.

## 준비 사항

환경 설정을 시작하기 전에 아래 소프트웨어가 설치되어 있는지 확인하세요.

* [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
* [Java](https://www.java.com/en/download/) (현재 JDK 8/11 지원) 및 `JAVA_HOME` 설정
* [Scala](https://www.scala-lang.org/download/2.11.12.html) (현재 2.11.12만 지원)
* [JetBrains IDEA](https://www.jetbrains.com/idea/)

## 설정 절차

### 소스 코드 가져오기

먼저 [GitHub](https://github.com/apache/seatunnel)에서 SeaTunnel 소스 코드를 클론합니다.

```shell
git clone git@github.com:apache/seatunnel.git
```

### 서브프로젝트 로컬 설치

소스 코드를 받았다면 JetBrains IDEA에서 올바르게 실행할 수 있도록 `./mvnw` 명령으로 서브프로젝트를 로컬 Maven 저장소에 설치해야 합니다.

```shell
./mvnw clean install -DskipTests
```

### 소스 코드에서 SeaTunnel 빌드

Maven이 준비되었다면 다음 명령으로 컴파일 및 패키징할 수 있습니다.

```shell
mvn clean package -pl seatunnel-dist -am -Dmaven.test.skip=true
```

### 서브 모듈 개별 빌드

특정 서브모듈만 빌드하려면 아래와 같이 실행합니다.

```shell
# 예: Redis 커넥터를 별도로 빌드
mvn clean package -pl seatunnel-connectors-v2/connector-redis -am -DskipTests -T 1C
```

### JetBrains IDEA Scala 플러그인 설치

JetBrains IDEA에서 Scala 코드를 빌드하려면 [Scala 플러그인](https://plugins.jetbrains.com/plugin/1347-scala)을 설치해야 합니다. 설치 방법은 [Install Plugins For IDEA](https://www.jetbrains.com/help/idea/managing-plugins.html#install-plugins)를 참고하세요.

### JetBrains IDEA Lombok 플러그인 설치

아래 예제를 실행하기 전에 [Lombok 플러그인](https://plugins.jetbrains.com/plugin/6317-lombok)도 설치해야 합니다. 설치 방법은 위와 동일한 안내서를 참고하세요.

### 코드 스타일

Apache SeaTunnel은 코드 스타일과 포맷 검사를 위해 `Spotless`를 사용합니다. 아래 명령을 실행하면 `Spotless`가 자동으로 스타일/포맷 오류를 수정합니다.

```shell
./mvnw spotless:apply
```

또한 `/tools/spotless_check/pre-commit.sh` 파일을 `.git/hooks/` 디렉터리로 복사해 두면, `git commit` 실행 시마다 `Spotless`가 자동으로 코드 스타일을 정리해 줍니다.

## 간단한 예제 실행

위 단계를 마치면 환경 설정이 완료되며, 제공되는 예제를 바로 실행할 수 있습니다. 모든 예제는 `seatunnel-examples` 모듈에 있으며, 관심 있는 예제를 선택해 [IDEA에서 실행 또는 디버그](https://www.jetbrains.com/help/idea/run-debug-configuration.html)할 수 있습니다.

예를 들어 `seatunnel-examples/seatunnel-engine-examples/src/main/java/org/apache/seatunnel/example/engine/SeaTunnelEngineLocalExample.java`를 실행하면 아래와 같은 출력이 표시됩니다.

```log
2024-08-10 11:45:32,839 INFO  org.apache.seatunnel.core.starter.seatunnel.command.ClientExecuteCommand - 
***********************************************
           Job Statistic Information
***********************************************
Start Time                : 2024-08-10 11:45:30
End Time                  : 2024-08-10 11:45:32
Total Time(s)             :                   2
Total Read Count          :                   5
Total Write Count         :                   5
Total Failed Count        :                   0
***********************************************
```

## 더 알아보기

SeaTunnel 예제는 의존성을 최소화하고 실행을 쉽게 하기 위해 단순한 소스와 싱크를 사용합니다. 필요에 따라 `resources/examples`에서 설정 파일을 수정하세요. 예를 들어 PostgreSQL을 소스로 사용하고 콘솔로 출력하려면 아래와 같이 구성할 수 있습니다. 단, FakeSource와 Console 이외의 커넥터를 사용할 때는 `seatunnel-examples`의 해당 서브모듈 `pom.xml`에 의존성을 추가해야 합니다.

```conf
env {
  parallelism = 1
  job.mode = "BATCH"
}
source {
    Jdbc {
        driver = org.postgresql.Driver
        url = "jdbc:postgresql://host:port/database"
        username = postgres
        password = "123456"
        query = "select * from test"
        table_path = "database.test"
    }
}

sink {
  Console {}
}
```
