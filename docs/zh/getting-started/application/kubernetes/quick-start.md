---
sidebar_position: 3
title: 快速开始
---

# Kubernetes 快速开始

Application Mode 为每个作业运行一套独立的 Zeta 集群。提交端创建挂起的 Kubernetes Job 及其依赖，
启动后立即返回；Master Pod 启动固定数量的 Worker，只运行一个作业，结束后释放 Worker 并退出。

## 1. 准备镜像

应用镜像就是标准 SeaTunnel 发行版加上 `resource-managers/kubernetes/` 里的 Kubernetes 资源管理器。
可以直接用包含 Application Mode 的官方镜像，也可以自行构建（用于未发布改动或额外 Connector）。

### 官方镜像

```bash
docker pull apache/seatunnel:<version>
docker tag apache/seatunnel:<version> registry.example.com/seatunnel:application
```

### 自定义构建

从源码构建发行版：

```bash
./mvnw -Prelease,seatunnel -pl seatunnel-dist -am -DskipTests -Dskip.ui=true package
```

用产物构建镜像：

```dockerfile
FROM eclipse-temurin:11-jdk
ADD apache-seatunnel-<version>-bin.tar.gz /opt/
RUN mv /opt/apache-seatunnel-<version> /opt/seatunnel
ENV SEATUNNEL_HOME=/opt/seatunnel
WORKDIR /opt/seatunnel
```

把 `seatunnel-dist/target/` 下的 `apache-seatunnel-<version>-bin.tar.gz` 放到 Dockerfile 旁边
（`ADD` 路径相对构建上下文），然后构建镜像：

```bash
cp seatunnel-dist/target/apache-seatunnel-<version>-bin.tar.gz .
docker build -t registry.example.com/seatunnel:application .
```

无论哪种方式，`kubernetes.image` 都必须指向集群可拉取的镜像：推送到镜像仓库，或加载到本地集群
（`minikube image load`、`kind load docker-image`）。

:::note
镜像必须包含 `starter/`、`config/`、`lib/`、`connectors/`、`resource-managers/kubernetes/`，以及
作业所需的全部 Connector、Format 和驱动 jar。
:::

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
    verbs: ["create", "list", "watch", "delete", "deletecollection"]
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
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: seatunnel-application-submitter
  namespace: seatunnel-apps
rules:
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["create", "get", "patch", "delete"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["list", "deletecollection"]
  - apiGroups: [""]
    resources: ["services"]
    verbs: ["create", "delete"]
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["create", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: seatunnel-application-submitter
  namespace: seatunnel-apps
subjects:
  - kind: ServiceAccount
    name: seatunnel-submitter
    namespace: seatunnel-apps
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: seatunnel-application-submitter
```

```bash
kubectl apply -f application-rbac.yaml
```

请把提交身份 RoleBinding 的 subjects 换成运行 `bin/seatunnel-application.sh` 的身份。

## 3. 准备部署配置

从 `config/v1.kubernetes.conf.template` 复制（模板里已带一个可直接运行的示例作业），修改平台选项：

```bash
cp config/v1.kubernetes.conf.template kubernetes.conf
```

```hocon
application {
  name = "example"
  worker-count = 2
}

kubernetes {
  namespace = "seatunnel-apps"
  image = "registry.example.com/seatunnel:application"
  service-account = "seatunnel-application"
  # 示例作业由本 application 自建的 ConfigMap 挂载进来，首次提交无需 -c 文件。
  job-config-file = job.conf
  configmap.job-conf {
    data.job.conf = """
      env {
        job.mode = "BATCH"
        parallelism = 2
      }
      source {
        FakeSource {
          row.num = 10
          split.num = 2
          schema { fields { id = int, name = string } }
        }
      }
      sink {
        Console {}
      }
    """
    mount.job.conf = "/opt/seatunnel/config/job.conf"
  }
}
```

:::note
每个挂载的 key 必须写完整容器路径，两个条目不能挂到同一路径。创建型 ConfigMap 命名为
`<application-id>-job-conf`，随 application 一起删除；如果希望作业放在独立文件里，改用
`-c job.conf` 提交并去掉 `job-config-file`。
:::

## 4. 提交与查看

```bash
bin/seatunnel-application.sh submit --target kubernetes -a kubernetes.conf
bin/seatunnel-application.sh status --target kubernetes --id APPLICATION_ID -a kubernetes.conf --wait
bin/seatunnel-application.sh cancel --target kubernetes --id APPLICATION_ID -a kubernetes.conf

kubectl -n seatunnel-apps get jobs,pods,services
kubectl -n seatunnel-apps describe job APPLICATION_ID
kubectl -n seatunnel-apps logs job/APPLICATION_ID
```

:::note
作业已写在部署文件里，因此无需 `-c`。`submit` 在 API 接受并启动 Job 后立即返回，镜像拉取慢
或调度慢不会让提交失败；需要终态时用 `status --wait`（或 `submit --wait`）。
:::

完整参数、退出码约定和 `kubectl` 排查方式见 [命令参考](commands.md)。
