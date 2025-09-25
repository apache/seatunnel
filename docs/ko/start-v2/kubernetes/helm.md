---
sidebar_position: 4
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Helm으로 설정하기

이 문서는 Helm을 사용해 SeaTunnel을 빠르게 배포하는 방법을 안내합니다.

## 선행 조건

다음 도구가 로컬에 설치되어 있다고 가정합니다.

- [Docker](https://docs.docker.com/)
- [Kubernetes](https://kubernetes.io/)
- [Helm](https://helm.sh/docs/intro/quickstart/)

따라서 `kubectl`, `helm` 명령을 로컬에서 바로 사용할 수 있어야 합니다.

예시로 Kubernetes [minikube](https://minikube.sigs.k8s.io/docs/start/)를 사용할 수 있습니다. 아래 명령으로 클러스터를 시작하세요.

```bash
minikube start --kubernetes-version=v1.23.3
```

## 설치

기본 설정으로 설치합니다.
```bash
# 필요한 버전을 선택하세요
export VERSION=2.3.10
helm pull oci://registry-1.docker.io/apache/seatunnel-helm --version ${VERSION}
tar -xvf seatunnel-helm-${VERSION}.tgz
cd seatunnel-helm
helm install seatunnel .
```
다른 네임스페이스에 설치하려면 다음 명령을 사용합니다.
```bash
helm install seatunnel . -n <your namespace>
```

## 작업 제출

기본 설정에서는 Ingress가 비활성화되어 있으므로 마스터의 REST API를 포트 포워딩해야 합니다.
```bash
kubectl port-forward -n default svc/seatunnel-master 5801:5801
```
그런 다음 `http://127.0.0.1:5801/`에서 REST API를 사용할 수 있습니다.

Ingress를 사용하려면 `values.yaml`을 수정하세요.

예시:
```commandline
ingress:
  enabled: true
  host: "<your domain>"
```
수정 후에는 Helm 릴리스를 업그레이드하면 됩니다.

이제 `http://<your domain>`에서 REST API에 접근할 수 있습니다.

또는 마스터 Pod 안으로 들어가 로컬에서 curl을 실행할 수도 있습니다.
```commandline
# 마스터 Pod 중 하나의 이름을 가져옵니다.
MASTER_POD=$(kubectl get po -l  'app.kubernetes.io/name=seatunnel-master' | sed '1d' | awk '{print $1}' | head -n1)
# 마스터 Pod 컨테이너에 진입합니다.
kubectl -n default exec -it $MASTER_POD -- /bin/bash

curl http://127.0.0.1:5801/running-jobs
curl http://127.0.0.1:5801/system-monitoring-information
```

이후에는 [REST API v2](../../seatunnel-engine/rest-api-v2.md)를 통해 작업을 제출하면 됩니다.

## 더 알아보기

지금까지 Helm으로 SeaTunnel을 살펴보았습니다. SeaTunnel이 지원하는 모든 소스와 싱크는 [커넥터 목록](../../connector-v2/source)에서 확인할 수 있습니다.
다른 엔진 클러스터에 애플리케이션을 제출하려면 [배포 문서](../../seatunnel-engine/deployment.md)를 참고하세요.
