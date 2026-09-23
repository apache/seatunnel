---
sidebar_position: 3
title: Quick Start
---

# Kubernetes quick start

## 1. Prepare the distribution and image

### Use a released distribution

Download a binary distribution that includes Application Mode from the [SeaTunnel download page](https://seatunnel.apache.org/download/). A released distribution does not require a Maven build.

### Build from source

To test unreleased functionality or local code changes, run from the repository root:

```bash
./mvnw -Prelease,seatunnel,kubernetes \
  -pl seatunnel-dist -am -DskipTests -Dskip.ui=true package
```

### Build the image

Build an image from the downloaded or locally built standard distribution:

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

The image must contain `starter/`, `config/`, `lib/`, `connectors/`, and `resource-managers/kubernetes/`. Include every connector, format, and driver required by the job.

## 2. Create namespace and RBAC

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

The submitting user also needs permission to manage Jobs, ConfigMaps, Services, and Pods in this namespace.

## 3. Create job and deployment configuration

`job.conf`:

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

`kubernetes.conf`:

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

## 4. Submit and wait

```bash
bin/seatunnel-application.sh submit --target kubernetes \
  --config job.conf --deployment-config kubernetes.conf --wait
```

The command prints the Kubernetes application ID, which is the generated Job name, and the native Zeta job ID. Remove `--wait` for detached submission. Closing the client does not cancel the remote Job.

## 5. Inspect or cancel

```bash
bin/seatunnel-application.sh status --target kubernetes \
  --id example-01234567-abc --deployment-config kubernetes.conf

bin/seatunnel-application.sh cancel --target kubernetes \
  --id example-01234567-abc --deployment-config kubernetes.conf
```

Continue with [Configuration](configuration.md), [Checkpoint recovery](checkpoint-recovery.md), and [FAQ](faq.md).
