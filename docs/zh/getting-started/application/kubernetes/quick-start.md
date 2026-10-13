---
sidebar_position: 3
title: 快速开始
---

# Kubernetes 快速开始

Application Mode 为每个作业运行一套独立的 Zeta 集群。提交端创建挂起的 Kubernetes Job 及其依赖，
启动后立即返回；Master Pod 启动固定数量的 Worker，只运行一个作业，结束后释放 Worker 并退出。

## 1. 准备镜像

使用包含 Application Mode 的发行版，或从源码构建：

```bash
./mvnw -Prelease,seatunnel -pl seatunnel-dist -am -DskipTests -Dskip.ui=true package
```

用标准发行版构建应用镜像：

```dockerfile
FROM eclipse-temurin:11-jdk
ADD apache-seatunnel-<version>-bin.tar.gz /opt/
RUN mv /opt/apache-seatunnel-<version> /opt/seatunnel
ENV SEATUNNEL_HOME=/opt/seatunnel
WORKDIR /opt/seatunnel
```

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

上面的 ServiceAccount 供 **Master Pod** 创建、观测、删除 Worker Pod 以及读取 owner Job 使用。**提交身份**
另行需要：

- 必需：Job 的 create/get/delete、Service 的 create/delete、ConfigMap 的 get（提交前校验引用）；
- 仅使用 `kubernetes.secret` 时：Secret 的 create/delete；
- 仅使用创建型 ConfigMap（`kubernetes.configmap.<entry>.data`）时：ConfigMap 的 create/delete；
- 仅使用 `kubernetes.secret-ref` 时：Secret 的 get（无权限时校验降级为告警）。

本地化配置通过 Master 容器环境传递，默认不会创建 application Secret。

## 3. 创建运行时 ConfigMap

Master 和 Worker Pod 需要发行版里的 `seatunnel.yaml` 与 `log4j2_client.properties`。创建一个
ConfigMap，按键逐文件挂载：

```bash
kubectl -n seatunnel-apps create configmap seatunnel-configuration \
  --from-file=seatunnel.yaml=config/seatunnel.yaml \
  --from-file=log4j2_client.properties=config/log4j2_client.properties
```

挂载使用 `subPath`，只替换声明的文件，镜像里配置目录的其他文件不受影响。

## 4. 准备部署配置

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
  configmap.runtime {
    ref = "seatunnel-configuration"
    mount.seatunnel.yaml = "/opt/seatunnel/config/seatunnel.yaml"
    mount.log4j2_client.properties = "/opt/seatunnel/config/log4j2_client.properties"
  }
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

需要传凭据时用平台管理的 Secret 并在作业文件里引用：

```hocon
kubernetes {
  # 引用平台已有 Secret，或让 SeaTunnel 创建：
  secret-ref = "platform-job-secrets"
  # secret { name = "example-job-secrets", data.MYSQL_PASSWORD = ${MYSQL_PASSWORD} }
  env.MYSQL_PASSWORD = "secret:platform-job-secrets/password"
}
```

:::tip
在 `job.conf` 里使用 `${MYSQL_PASSWORD}`；占位符在 Pod 内解析，不在提交机替换。
:::

## 5. 提交与查看

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
