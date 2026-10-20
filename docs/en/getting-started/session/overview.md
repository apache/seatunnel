---
sidebar_position: 5
title: Session Cluster
---

# Session Cluster

A Session Cluster is a long-running Zeta cluster shared by multiple jobs. Two masters form a
Hazelcast active/standby pair; the active master creates workers, and jobs submit through REST or
the native client.

[Application Mode](../application/overview.md) is different: it creates a dedicated master and
worker set for one job.

## Topology

```text
kubectl apply -k deploy/kubernetes/session
  -> 2 Master Pods (StatefulSet)
  -> active master creates min-workers Worker Pods
  -> workers register slots
  -> submit jobs through REST or the native client
```

- Masters: StatefulSet, 2 replicas, `backup-count: 1`.
- Workers: bare Pods owned by the root ConfigMap. There is no worker Deployment or StatefulSet.
- Slots: fixed mode only; `session.worker.slots` must equal worker `slot-service.slot-num`.
- Scale-up: `ceil(missingSlots / slotsPerWorker)`, capped by `max-workers`.
- Scale-down: not supported.

## Deploy

Read [Kubernetes Deployment](../kubernetes/kubernetes.mdx) and
[Kubernetes Configuration](../kubernetes/configuration.md) first. The Session image must contain
`seatunnel-session-cluster.sh` and the Kubernetes resource-manager provider.

```bash
kubectl apply -k deploy/kubernetes/session
```

The sample uses `conf/` for all master and worker configuration files. Kustomize generates one
`<session-name>-config` ConfigMap; the master mounts it whole and workers mount their required keys.

To run another Session in the same namespace, copy the directory and rename every session-scoped
resource, label, `session.yaml` value, `configMapGenerator` name, and Hazelcast/client DNS entry:

```bash
cp -R deploy/kubernetes/session /tmp/payments-session
# Edit /tmp/payments-session as described above.
kubectl apply -k /tmp/payments-session
```

## Configure

`conf/session.yaml` is the Session-specific configuration. Unknown keys are ignored; invalid values
fail master startup.

```yaml
session:
  # Required DNS-1123 label, max 40 chars; unique per namespace.
  name: seatunnel-session-cluster

  # Default: seatunnel-session-<name>.
  cluster-name: seatunnel-session-cluster

  # Default: seatunnel-session-<name>-meta. Must already exist and have a UID.
  root-config-map: seatunnel-session-cluster-meta

  master:
    # Default: 5801. Keep Services and client config aligned.
    port: 5801

  worker:
    # Required. The master image is configured in the StatefulSet.
    image: apache/seatunnel:3.0.1

    # Defaults: min-workers=2, max-workers=32.
    min-workers: 2
    max-workers: 32

    # Fixed slots; must equal seatunnel.yaml slot-service.slot-num.
    slots: 8

    # Defaults: cpu-cores=1, memory-mb=1024.
    cpu-cores: 2
    memory-mb: 4096

    # Default: 3/4 of memory-mb; must be < memory-mb.
    heap-mb: 3072

    # Default: -Xms<heap>m -Xmx<heap>m.
    jvm-options: "-Xms3g -Xmx3g"

    # Defaults: IfNotPresent, default, /opt/seatunnel.
    image-pull-policy: IfNotPresent
    service-account: default
    seatunnel-home: /opt/seatunnel

    # Must contain seatunnel.yaml, hazelcast-worker.yaml, log4j2.properties,
    # and jvm_worker_options.
    config-map: seatunnel-session-cluster-config

    # Optional Pod metadata.
    # labels: {}
    # annotations: {}

    # Optional. Values are literals or secret:name/key and configmap:name/key.
    # env:
    #   AWS_REGION: us-east-1
    # env-from:
    #   secret: s3-credentials
    #   configmap: shared-settings

    # Optional existing Secret mount.
    # secret-ref: session-secret
    # secret-directory: /etc/seatunnel-secrets/session

  dynamic-worker:
    # Default: false. Creates workers from pending-job demand when true.
    enabled: true
    # Defaults: 120000 and 3000; both must be > 0.
    startup-timeout-millis: 120000
    retry-interval-millis: 3000

kubernetes:
  # Default: default. Must contain all Session resources.
  namespace: seatunnel
```

The worker `seatunnel.yaml` must set `slot-service.dynamic-slot: false`. Use the master/worker
engine and Hazelcast configuration from [Separated Cluster Mode](../kubernetes/separated-cluster-mode.md);
the sample `file:///tmp/` storage is for local validation only.

## Start

```bash
kubectl apply -k deploy/kubernetes/session
kubectl -n seatunnel get pods -w
```

For status, logs, and REST access, see [Kubernetes Operations](../kubernetes/operations.md).

## Submit jobs

REST:

```bash
kubectl -n seatunnel port-forward svc/seatunnel-session-cluster-rest 8080:8080
curl -X POST http://127.0.0.1:8080/submit-job \
  -H 'Content-Type: application/json' \
  -d @job.json
```

Native client:

```bash
/opt/seatunnel/bin/seatunnel.sh --config job.conf -m cluster
```

The client configuration points at
`seatunnel-session-cluster-client.seatunnel.svc.cluster.local:5801`.

## Scaling

The active master creates `min-workers` at startup. When a pending job cannot get enough slots, it
creates `ceil(missingSlots / slotsPerWorker)` workers up to `max-workers`, waits for registration,
and retries allocation. `REJECT` fails after the startup timeout; `WAIT` keeps the job pending.

## Stop

Delete the master StatefulSet first, then the Session-scoped resources. Do not use
`kubectl delete -k` if the namespace hosts multiple Sessions.

```bash
SESSION_NAME=seatunnel-session-cluster

kubectl -n seatunnel delete statefulset "${SESSION_NAME}-master"
kubectl -n seatunnel delete configmap "${SESSION_NAME}-meta" "${SESSION_NAME}-config"
kubectl -n seatunnel delete service \
  "${SESSION_NAME}-headless" "${SESSION_NAME}-rest" "${SESSION_NAME}-client"
kubectl -n seatunnel delete pdb "${SESSION_NAME}-master"
```

User-owned checkpoints, PVCs, and referenced ConfigMaps/Secrets are not deleted.

## Limitations

- Worker template changes are not supported in place; create a new Session.
- No automatic scale-down or manual scale/spec API.
- No HA beyond the Hazelcast active/standby pair and StatefulSet restart.
- No multi-tenant quotas, Kerberos, or CPU-aware slot scheduling.
