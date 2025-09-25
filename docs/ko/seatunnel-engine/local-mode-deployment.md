---
sidebar_position: 4
---

# 로컬 모드에서 잡 실행하기

로컬 모드에서는 작업마다 별도의 프로세스를 시작하고, 작업이 끝나면 해당 프로세스가 종료됩니다. 이 모드에는 다음과 같은 제한이 있습니다.

1. 작업 일시 중지/재개를 지원하지 않습니다.
2. 작업 목록 조회를 지원하지 않습니다.
3. 명령으로 작업을 취소할 수 없고, 프로세스를 직접 종료해야 합니다.

각 작업이 독립 프로세스에서 실행되므로 상호 영향이 없다는 장점이 있어 안정성이 최우선인 환경에 적합합니다.

## 로컬 모드 배포

로컬 모드에서는 별도의 SeaTunnel Engine 클러스터를 준비할 필요가 없습니다. 아래 명령처럼 잡을 제출하면 제출한 프로세스 안에서 SeaTunnel Engine(Zeta) 서비스가 일시적으로 실행되고, 잡이 끝나면 프로세스가 종료됩니다.

이때 다운로드한 설치 패키지를 실행할 서버에 복사하기만 하면 됩니다. 실행 시 JVM 파라미터를 조정하려면 `$SEATUNNEL_HOME/config/jvm_client_options` 파일을 수정하세요.

## 잡 제출

```shell
$SEATUNNEL_HOME/bin/seatunnel.sh --config $SEATUNNEL_HOME/config/v2.batch.config.template -m local
```

### 로컬 모드 JVM 옵션 설정

로컬 모드에서는 JVM 옵션을 두 가지 방식으로 지정할 수 있습니다.

1. `$SEATUNNEL_HOME/config/jvm_client_options`에 JVM 옵션 추가
   - 해당 파일을 수정하면 `seatunnel.sh`로 제출하는 모든 작업(로컬/클러스터)을 동일하게 적용합니다.

2. 실행 시 JVM 옵션 전달
   - 예: `$SEATUNNEL_HOME/bin/seatunnel.sh --config $SEATUNNEL_HOME/config/v2.batch.config.template -m local -DJvmOption="-Xms2G -Xmx2G"`

## 잡 운영

로컬 모드에서 제출된 작업은 제출한 프로세스에서 실행되며, 작업이 완료되면 프로세스가 종료됩니다. 작업을 중단하려면 해당 프로세스를 종료하면 되고, 실행 로그는 제출 프로세스의 표준 출력에 기록됩니다.

기타 운영·유지보수 기능은 제공되지 않습니다.
