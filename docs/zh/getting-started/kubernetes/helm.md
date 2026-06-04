---
sidebar_position: 4
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 使用Helm部署

使用Helm快速部署Seatunnel集群。

## 准备

我们假设您的本地已经安装如下软件:

- [docker](https://docs.docker.com/)
- [kubernetes](https://kubernetes.io/)
- [helm](https://helm.sh/docs/intro/quickstart/)

在您的本地环境中能够正常执行`kubectl`和`helm`命令。
 
以 [minikube](https://minikube.sigs.k8s.io/docs/start/) 为例, 您可以使用如下命令启动一个集群:

```bash
minikube start --kubernetes-version=v1.23.3
```

## 安装

使用默认配置安装
```bash
# Choose the corresponding version yourself
export VERSION=2.3.10
helm pull oci://registry-1.docker.io/apache/seatunnel-helm --version ${VERSION}
tar -xvf seatunnel-helm-${VERSION}.tgz
cd seatunnel-helm
helm install seatunnel .
```

如果您需要使用其他命名空间进行安装。
```
helm install seatunnel . -n <your namespace>
```

对于托管 Kubernetes 服务，建议将云厂商相关的差异化配置单独维护在一个 values 文件中，并通过 `-f` 传入，例如：

```bash
helm install seatunnel . -n <your namespace> -f values-eks.yaml
```

在托管集群上常见需要复核的 values 包括：

- 用于 ECR、Artifact Registry、ACR、ACK 容器镜像服务、TCR、SWR、Volcengine Container Registry 或 OpenShift 内置镜像仓库的镜像仓库地址与 `imagePullSecrets`
- 目标命名空间对应的 ServiceAccount、RBAC，以及 OpenShift 上的 SecurityContextConstraint 要求
- 云厂商负载均衡、子网、证书与内外网暴露模式相关的 Service 或 Ingress annotation
- 用于 checkpoint 与 state 路径的对象存储或 PersistentVolume 配置
- 用于运行 SeaTunnel Pod 的节点池所需的 resource request/limit、node selector、toleration 与 affinity
- 接入云厂商监控栈的日志与指标采集配置

## 提交任务

当前默认的配置没有启用ingress，所以需要使用转发命令将master的restapi端口转发出来。
```bash
kubectl port-forward -n default svc/seatunnel-master 5801:5801
```
然后可以通过地址 "http://127.0.0.1/5801/" 访问master的restapi。

如果想要使用ingress, 需要更新 `value.yaml`

例如:
```commandline
ingress:
  enabled: true
  host: "<your domain>"
```
然后更新seatunnel。

就可以使用域名`http://<your domain>`进行访问了。

或者您可以直接进入master的POD执行curl命令。.
```commandline
# 获取其中一个master pod
MASTER_POD=$(kubectl get po -l  'app.kubernetes.io/name=seatunnel-master' | sed '1d' | awk '{print $1}' | head -n1)
# 进入master pod
kubectl -n default exec -it $MASTER_POD -- /bin/bash
# 执行 restapi
curl http://127.0.0.1:5801/running-jobs
curl http://127.0.0.1:5801/system-monitoring-information
```

后面就可以使用[rest-api-v2](../../engines/zeta/rest-api-v2.md)提交任务了。

## 下一步
到现在为止，您已经安装好Seatunnel集群了，你可以查看Seatunnel有哪些[连接器](../../connectors).
或者选择其他方式 [部署](../../engines/zeta/deployment.md).
