---
sidebar_position: 3
title: Quick Start
---

# Kubernetes quick start

Application Mode runs one isolated Zeta cluster per job. The submitter creates a suspended
Kubernetes Job plus its dependencies, starts it and returns immediately; the master Pod starts the
fixed workers, runs exactly one job, releases the workers and exits.

## 1. Prepare the image

The application image is the standard SeaTunnel distribution with the Kubernetes resource manager
in `resource-managers/kubernetes/`. Use a released image that includes Application Mode, or build
your own to pick up unreleased changes or extra connectors.

### Released image

```bash
docker pull apache/seatunnel:<version>
docker tag apache/seatunnel:<version> registry.example.com/seatunnel:application
```

### Custom image

Build the distribution from source:

```bash
./mvnw -Prelease,seatunnel -pl seatunnel-dist -am -DskipTests -Dskip.ui=true package
```

Build the image from the produced archive:

```dockerfile
FROM eclipse-temurin:11-jdk
ADD apache-seatunnel-<version>-bin.tar.gz /opt/
RUN mv /opt/apache-seatunnel-<version> /opt/seatunnel
ENV SEATUNNEL_HOME=/opt/seatunnel
WORKDIR /opt/seatunnel
```

Put `apache-seatunnel-<version>-bin.tar.gz` from `seatunnel-dist/target/` next to the Dockerfile
(the `ADD` path is relative to the build context) and build the image:

```bash
cp seatunnel-dist/target/apache-seatunnel-<version>-bin.tar.gz .
docker build -t registry.example.com/seatunnel:application .
```

Either way, `kubernetes.image` must name an image the cluster can pull: push it to a registry, or
load it into a local cluster (`minikube image load`, `kind load docker-image`).

:::note
The image must contain `starter/`, `config/`, `lib/`, `connectors/` and
`resource-managers/kubernetes/`, plus every connector, format and driver used by the job.
:::

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

The ServiceAccount above is used by the **master Pod** to create, observe and delete worker Pods
and to read the owner Job. The **submitting identity** that runs `bin/seatunnel-application.sh`
needs these permissions in the namespace:

| Resource | Verbs | Required when |
| --- | --- | --- |
| Jobs | `create`, `get`, `patch`, `delete` | always; `patch` unsuspends the Job after creation |
| Pods | `list`, `deletecollection` | always; `list` resolves the application status, `deletecollection` removes the Pods on `cancel` |
| Services | `create`, `delete` | always; the headless discovery Service |
| ConfigMaps | `get` | `kubernetes.configmap.<entry>.ref`; without it the validation is skipped with a warning |
| ConfigMaps | `create`, `delete` | only with a created entry (`kubernetes.configmap.<entry>.data`) |
| Secrets | `create`, `delete` | only with `kubernetes.secret` |
| Secrets | `get` | only with `kubernetes.secret-ref`; without it the validation is skipped with a warning |

When the kubeconfig used for submission lacks one of these verbs, create a Role with the matching
rules and bind it to that user, group or CI ServiceAccount.

The localized specification travels in the master Pod environment; no application Secret is
created by default.

## 3. Create the runtime ConfigMap

The master and worker Pods need `seatunnel.yaml` and `log4j2_client.properties` from the
distribution. Create one ConfigMap and mount the keys per file:

```bash
kubectl -n seatunnel-apps create configmap seatunnel-configuration \
  --from-file=seatunnel.yaml=config/seatunnel.yaml \
  --from-file=log4j2_client.properties=config/log4j2_client.properties
```

Mounts use `subPath`, so only the declared files are replaced and the rest of the image
configuration directory stays untouched.

## 4. Create the deployment configuration

Start from `config/v1.kubernetes.conf.template`, which already carries a demo job, and set the
platform options:

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
  # The demo job is mounted from a ConfigMap SeaTunnel creates for this application, so the first
  # submission needs no -c file.
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
Every mounted key needs its full container path; two entries cannot share one path. The created
ConfigMap is named `<application-id>-job-conf` and is deleted with the application. To keep the job
in its own file instead, pass it with `-c job.conf` and remove `job-config-file`.
:::

To pass credentials, inject platform-managed values and reference them from the job file:

```hocon
kubernetes {
  # Reference a Secret managed by your platform, or let SeaTunnel create one:
  secret-ref = "platform-job-secrets"
  # secret { name = "example-job-secrets", data.MYSQL_PASSWORD = ${MYSQL_PASSWORD} }
  env.MYSQL_PASSWORD = "secret:platform-job-secrets/password"
}
```

:::tip
Use `${MYSQL_PASSWORD}` in `job.conf`; placeholders are resolved inside the Pod, never on the
submitting host.
:::

## 5. Submit and inspect

```bash
bin/seatunnel-application.sh submit --target kubernetes -a kubernetes.conf
bin/seatunnel-application.sh status --target kubernetes --id APPLICATION_ID -a kubernetes.conf --wait
bin/seatunnel-application.sh cancel --target kubernetes --id APPLICATION_ID -a kubernetes.conf

kubectl -n seatunnel-apps get jobs,pods,services
kubectl -n seatunnel-apps describe job APPLICATION_ID
kubectl -n seatunnel-apps logs job/APPLICATION_ID
```

:::note
No `-c` file is needed because the deployment file carries the job. `submit` returns as soon as the
API accepted the started Job, so slow image pulls or scheduling never fail the command. Use
`status --wait` (or `submit --wait`) when you need the terminal result.
:::

See [Commands](commands.md) for every argument, the exit-code contract, and the `kubectl`
inspection patterns.
