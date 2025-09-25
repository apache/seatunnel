# SeaTunnel 소개

<img src="https://seatunnel.apache.org/image/logo.png" alt="seatunnel logo" width="200px" height="200px" align="right" />

[![Slack](https://img.shields.io/badge/slack-%23seatunnel-4f8eba?logo=slack)](https://s.apache.org/seatunnel-slack)
[![Twitter Follow](https://img.shields.io/twitter/follow/ASFSeaTunnel.svg?label=Follow&logo=twitter)](https://twitter.com/ASFSeaTunnel)

> ⚠️ 이 문서는 SeaTunnel 공식 문서를 한국어로 번역한 버전입니다. 용어는 기존 문서와 일관되게 사용해 주세요.

SeaTunnel은 사용하기 쉬운 다중 모드 초고성능 분산 데이터 통합 플랫폼으로, 방대한 데이터를 실시간으로 동기화할 수 있습니다.
하루에도 수백억 건의 데이터를 안정적이고 효율적으로 처리하며, 현재 약 100개의 기업에서 프로덕션 용도로 활용되고 있습니다.

## SeaTunnel이 필요한 이유

SeaTunnel은 데이터 통합과 데이터 동기화에 집중하며, 데이터 통합 분야에서 흔히 겪는 문제를 해결하도록 설계되었습니다.

* **다양한 데이터 소스**: 서로 호환되지 않는 버전을 가진 데이터 소스가 수백 개에 달합니다. 새로운 기술이 등장할수록 더 많은 데이터 소스가 생겨나기 때문에, 이러한 소스를 빠르게 지원하는 도구를 찾기 어렵습니다.
* **멀티모달 데이터 통합**: 구조화된 데이터뿐 아니라 동영상, 이미지, 바이너리 파일, 구조화·비구조화 텍스트 데이터를 함께 다뤄야 합니다. 하지만 기존 데이터 통합 도구는 주로 구조화 데이터에 초점이 맞춰져 있습니다.
* **복잡한 동기화 시나리오**: 오프라인 전체 동기화, 오프라인 증분 동기화, CDC, 실시간 동기화, 전체 데이터베이스 동기화 등 다양한 시나리오를 지원해야 합니다.
* **높은 자원 요구량**: 기존 도구는 대량의 작은 테이블을 실시간으로 동기화하려면 방대한 컴퓨팅 자원이나 JDBC 연결 자원이 필요해 기업 부담이 커집니다.
* **품질 및 모니터링 부족**: 동기화 과정에서 데이터 유실이나 중복이 발생하기 쉽고, 모니터링이 충분하지 않아 작업 중 데이터를 직관적으로 파악하기 어렵습니다.
* **복잡한 기술 스택**: 기업마다 사용하는 기술 컴포넌트가 달라, 각 컴포넌트에 맞춘 동기화 프로그램을 별도로 개발해야 합니다.
* **관리 및 유지보수 난이도**: Flink·Spark와 같이 서로 다른 기반 기술에 따라 오프라인과 실시간 동기화를 따로 개발·관리해야 해 운영 복잡도가 증가합니다.

## SeaTunnel의 주요 특징

* **풍부하고 확장 가능한 커넥터**: 특정 실행 엔진에 의존하지 않는 커넥터 API를 제공합니다. 이 API로 개발된 Source, Transform, Sink 커넥터는 SeaTunnel Engine(Zeta), Flink, Spark 등 다양한 엔진에서 실행할 수 있습니다.
* **커넥터 플러그인**: 플러그인 구조로 되어 있어 사용자가 손쉽게 커넥터를 개발하고 SeaTunnel 프로젝트에 통합할 수 있습니다. 현재 100개 이상의 커넥터를 지원하며 계속 증가하고 있습니다.
* **배치-스트림 통합**: SeaTunnel 커넥터 API로 개발된 커넥터는 오프라인·실시간·전체·증분 등 다양한 동기화 시나리오를 모두 지원하여 데이터 통합 작업의 관리 부담을 줄여 줍니다.
* **분산 스냅샷**: 분산 스냅샷 알고리즘을 지원하여 데이터 일관성을 보장합니다.
* **멀티 엔진 지원**: 기본적으로 SeaTunnel Engine(Zeta)을 사용하지만, Flink 또는 Spark를 실행 엔진으로 선택해 기존 환경과 쉽게 통합할 수 있습니다. 여러 버전의 Spark와 Flink도 지원합니다.
* **JDBC 멀티플렉싱 및 데이터베이스 로그 다중 테이블 파싱**: 다중 테이블·전체 데이터베이스 동기화를 지원해 JDBC 연결 과다 문제를 해결하며, CDC 다중 테이블 시나리오에서 로그를 반복 분석해야 하는 부담을 줄여 줍니다.
* **고성능·저지연**: 병렬 읽기/쓰기를 지원해 높은 처리량과 낮은 지연으로 안정적인 동기화 능력을 제공합니다.
* **정교한 실시간 모니터링**: 동기화 과정의 각 단계를 세밀하게 모니터링하여 데이터 건수, 데이터 크기, QPS 등 지표를 쉽게 확인할 수 있습니다.
* **두 가지 작업 개발 방식 지원**: 코드 기반 개발과 캔버스 기반 설계를 모두 지원합니다. SeaTunnel 웹 프로젝트(<https://github.com/apache/seatunnel-web>)를 통해 작업의 시각적 관리, 스케줄링, 실행, 모니터링을 제공합니다.

## SeaTunnel 동작 흐름도

![SeaTunnel Work Flowchart](../images/architecture_diagram.png)

위 그림은 SeaTunnel의 런타임 프로세스를 보여줍니다.

사용자는 작업 정보를 구성하고 실행 엔진을 선택한 뒤 작업을 제출합니다.

Source 커넥터는 데이터를 병렬로 읽어 하위 Transform 또는 Sink로 전달하고, Sink는 데이터를 대상 시스템에 기록합니다. Source·Transform·Sink는 모두 사용자가 손쉽게 확장하거나 직접 개발할 수 있습니다.

SeaTunnel은 EL(T) 데이터 통합 플랫폼입니다. 따라서 Transform은 대문자/소문자 변환, 컬럼명 변경, 컬럼 분할과 같은 간단한 변환 작업에 사용합니다.

SeaTunnel의 기본 실행 엔진은 [SeaTunnel Engine](seatunnel-engine/about.md)입니다. Flink 또는 Spark 엔진을 선택하면 SeaTunnel이 커넥터를 해당 엔진용 프로그램으로 패키징하여 실행합니다.

## 커넥터

- **Source Connector**: SeaTunnel은 관계형·그래프·NoSQL·문서·인메모리 데이터베이스, HDFS 같은 분산 파일 시스템, S3·OSS 등 다양한 클라우드 스토리지에서 데이터를 읽을 수 있습니다. 또한 여러 SaaS 서비스의 데이터 읽기를 지원합니다. 자세한 목록은 [여기](connector-v2/source)에서 확인할 수 있으며, 필요하다면 Source 커넥터를 직접 개발해 손쉽게 통합할 수 있습니다.
- **Transform Connector**: Source와 Sink의 스키마가 다를 경우 Transform 커넥터를 사용해 스키마를 변환하고 Sink 스키마와 일치시킬 수 있습니다.
- **Sink Connector**: SeaTunnel은 관계형·그래프·NoSQL·문서·인메모리 데이터베이스, HDFS 등 분산 파일 시스템, S3·OSS 같은 클라우드 스토리지에 데이터를 쓸 수 있습니다. 다양한 SaaS 서비스로의 데이터 쓰기도 지원하며, 필요 시 Sink 커넥터를 직접 개발해 통합할 수 있습니다.

## SeaTunnel을 사용하는 곳

SeaTunnel을 활용하는 조직은 매우 많습니다. 자세한 정보는 [사용자 페이지](https://seatunnel.apache.org/user)에서 확인할 수 있습니다.

## 생태계

<p align="center">
<br/><br/>
<img src="https://landscape.cncf.io/images/left-logo.svg" width="150" alt=""/>&nbsp;&nbsp;<img src="https://landscape.cncf.io/images/right-logo.svg" width="200" alt=""/>
<br/><br/>
SeaTunnel은 <a href="https://landscape.cncf.io/?item=app-definition-and-development--streaming-messaging--seatunnel">CNCF CLOUD NATIVE Landscape</a>를 더욱 풍성하게 만듭니다.
</p>

## 더 알아보기

다음 단계는 [빠른 시작](start-v2/locally/deployment.md) 문서를 참고하세요.
