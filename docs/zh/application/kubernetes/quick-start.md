---
sidebar_position: 3
title: 快速开始
---

# Kubernetes 快速开始

## 1. 准备发行包和镜像

### 使用官方发行包

从 [SeaTunnel 下载页](https://seatunnel.apache.org/download/)下载包含 Application Mode 的二进制发行包。正式版本不需要重新执行 Maven 构建。

### 从源码构建

需要验证尚未发布的功能或修改代码时，在仓库根目录执行：

```bash
./mvnw -Prelease,seatunnel \
  -pl seatunnel-dist -am -DskipTests -Dskip.ui=true package
```

### 创建镜像

使用下载或源码构建得到的标准发行包创建镜像：

```dockerfile
FROM eclipse-temurin:8-jdk
ADD apache-seatunnel-<version>-bin.tar.gz /opt/
RUN mv /opt/apache-seatunnel-<version> /opt/seatunnel
ENV SEATUNNEL_HOME=/opt/seatunnel
WORKDIR /opt/seatunnel
```

```bash
docker build -t registry.example.com/seatunnel:application .
docker push registry.example.com/seatunnel:application
```

镜像必须包含 `starter/`、`config/`、`lib/`、`connectors/` 和 `resource-managers/kubernetes/`。作业需要的 Connector、Format 和驱动必须提前放入镜像。

## 2. 创建 Namespace 和 RBAC

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: seatunnel-apps
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: seatunnel-application
  namespace: seatunnel-apps
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: seatunnel-application
  namespace: seatunnel-apps
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["create", "get", "list", "delete", "deletecollection"]
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: seatunnel-application
  namespace: seatunnel-apps
subjects:
  - kind: ServiceAccount
    name: seatunnel-application
    namespace: seatunnel-apps
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: seatunnel-application
```

```bash
kubectl apply -f application-rbac.yaml
```

提交用户还需要在该 Namespace 中管理 Job、ConfigMap、Service 和 Pod 的权限。

## 3. 创建作业和部署配置

修改 Kubernetes 部署参数前，先复制发行包中的模板：

```bash
cp config/v1.kubernetes.conf.template kubernetes.conf
```

`job.conf`：

```hocon
env { job.mode = "BATCH", parallelism = 2 }
source {
  FakeSource {
    row.num = 10
    split.num = 2
    schema { fields { id = int, name = string } }
  }
}
sink { Console {} }
```

`kubernetes.conf`：

```hocon
application.name = "example"
application.worker-count = 2
application.worker.memory-mb = 1024
application.worker.cpu-cores = 1
application.worker.slots = 2
application.master.memory-mb = 1024
application.master.cpu-cores = 1
application.startup-timeout-millis = 180000

kubernetes.namespace = "seatunnel-apps"
kubernetes.image = "registry.example.com/seatunnel:application"
kubernetes.service-account = "seatunnel-application"
kubernetes.image-pull-policy = "IfNotPresent"
```

## 4. 提交并等待结果

```bash
bin/seatunnel-application.sh submit --target kubernetes \
  --config job.conf --deployment-config kubernetes.conf --wait
```

命令打印 Kubernetes application ID（生成的 Job 名称）和原生 Zeta job ID。去掉 `--wait` 可 detached 提交，关闭客户端不会取消远端 Job。

## 5. 查询或取消

```bash
bin/seatunnel-application.sh status --target kubernetes \
  --id example-01234567-abc --deployment-config kubernetes.conf

bin/seatunnel-application.sh cancel --target kubernetes \
  --id example-01234567-abc --deployment-config kubernetes.conf
```

继续阅读[配置参考](configuration.md)、[Checkpoint 恢复](checkpoint-recovery.md)和 [FAQ](faq.md)。
