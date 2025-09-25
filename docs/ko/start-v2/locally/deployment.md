---
sidebar_position: 2
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 배포

## 준비

SeaTunnel을 다운로드하기 전에 다음 필수 소프트웨어가 설치되어 있는지 확인하세요.

* [Java](https://www.java.com/en/download/)를 설치하고 `JAVA_HOME` 환경 변수를 설정합니다. (Java 8 또는 11 권장, 8 이상 버전은 이론적으로 사용 가능)

## SeaTunnel 릴리스 패키지 다운로드

### 바이너리 패키지 다운로드

[SeaTunnel 다운로드 페이지](https://seatunnel.apache.org/download)에서 최신 바이너리 패키지 `seatunnel-<version>-bin.tar.gz`를 내려받습니다.

터미널에서 직접 다운로드할 수도 있습니다.

```shell
export version="2.3.13"
wget "https://archive.apache.org/dist/seatunnel/${version}/apache-seatunnel-${version}-bin.tar.gz"
tar -xzvf "apache-seatunnel-${version}-bin.tar.gz"
```

### 커넥터 플러그인 다운로드

2.2.0-beta 버전부터 바이너리 패키지에는 커넥터 의존성이 기본 포함되지 않습니다. 처음 사용할 때는 아래 명령으로 커넥터를 설치하세요. (또는 [Apache Maven 저장소](https://repo.maven.apache.org/maven2/org/apache/seatunnel/)에서 커넥터를 직접 내려받아 `connectors/` 디렉터리에 배치해도 됩니다. 2.3.5 이전 버전은 `connectors/seatunnel` 디렉터리에 넣어야 합니다.)

```bash
sh bin/install-plugin.sh
```

특정 버전의 커넥터가 필요하다면, 예를 들어 2.3.13 버전을 설치하려면 다음 명령을 실행합니다.

```bash
sh bin/install-plugin.sh 2.3.13
```

모든 커넥터 플러그인이 필요한 것은 아닙니다. `config/plugin_config` 파일에서 필요한 플러그인만 지정할 수 있습니다. 예시 애플리케이션을 정상 실행하려면 `connector-console`과 `connector-fake` 플러그인이 필요하므로, 구성 파일을 다음과 같이 수정합니다.

```plugin_config
--seatunnel-connectors--
connector-fake
connector-console
--end--
```

지원되는 커넥터와 `plugin_config`에 사용할 이름은 `${SEATUNNEL_HOME}/connectors/plugins-mapping.properties` 파일에서 확인할 수 있습니다.

:::tip 팁

커넥터를 직접 내려받아 설치하려면 관련 플러그인만 다운로드해 `${SEATUNNEL_HOME}/connectors/` 디렉터리에 배치하면 됩니다.

:::

## 소스 코드에서 SeaTunnel 빌드하기

### 소스 코드 다운로드

소스 코드에서 빌드할 수도 있습니다. 소스 코드 다운로드 방법은 바이너리 패키지를 받을 때와 동일합니다.
[다운로드 페이지](https://seatunnel.apache.org/download/)에서 압축 파일을 받거나 [GitHub 저장소](https://github.com/apache/seatunnel/releases)를 클론하세요.

### 소스 코드 빌드

```shell
cd seatunnel
sh ./mvnw clean install -DskipTests -Dskip.spotless=true
# 생성된 바이너리 패키지 복사
cp seatunnel-dist/target/apache-seatunnel-2.3.13-bin.tar.gz /The-Path-You-Want-To-Copy

cd /The-Path-You-Want-To-Copy
tar -xzvf "apache-seatunnel-${version}-bin.tar.gz"
```

소스 코드로 빌드하면 모든 커넥터 플러그인과 필요한 의존성(예: MySQL 드라이버)이 바이너리 패키지에 포함됩니다. 별도 설치 없이 바로 커넥터 플러그인을 사용할 수 있습니다.

# SeaTunnel 실행

SeaTunnel 바이너리 패키지와 커넥터 플러그인을 모두 준비했다면, 이제 원하는 엔진을 선택해 동기화 작업을 실행할 수 있습니다.

Flink로 동기화 작업을 실행한다면 SeaTunnel Engine 서비스 클러스터를 따로 배포할 필요가 없습니다. [Flink 빠른 시작](quick-start-flink.md)을 참고하세요.

Spark로 실행하는 경우에도 SeaTunnel Engine 서비스 클러스터는 필요하지 않습니다. [Spark 빠른 시작](quick-start-spark.md)을 참고하세요.

내장 SeaTunnel Engine(Zeta)을 사용하려면 먼저 SeaTunnel Engine 서비스를 배포해야 합니다. [SeaTunnel Engine 빠른 시작](quick-start-seatunnel-engine.md)을 살펴보세요.
