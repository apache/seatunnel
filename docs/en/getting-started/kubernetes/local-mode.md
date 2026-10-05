---
sidebar_position: 2
---

# Local Mode

Local Mode runs one SeaTunnel job in Kubernetes with a single Pod or Job. It does not provide cluster high availability and is not suitable as a long-running production cluster.

## Create Job Configuration

Create `seatunnel.streaming.conf`:

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 2000
}

source {
  FakeSource {
    parallelism = 2
    plugin_output = "fake"
    row.num = 16
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  Console {
  }
}
```

Create a ConfigMap:

```bash
kubectl create configmap seatunnel-job-config \
  --from-file=seatunnel.streaming.conf=seatunnel.streaming.conf
```

## Create Pod

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: seatunnel-local
  labels:
    app: seatunnel
    mode: local
spec:
  restartPolicy: Never
  containers:
    - name: seatunnel
      image: seatunnel:3.0.0
      imagePullPolicy: IfNotPresent
      command:
        - /bin/sh
        - -c
        - /opt/seatunnel/bin/seatunnel.sh --config /data/seatunnel.streaming.conf -e local
      env:
        - name: SEATUNNEL_HOME
          value: /opt/seatunnel
      resources:
        requests:
          cpu: "1"
          memory: 2Gi
        limits:
          cpu: "1"
          memory: 4Gi
      volumeMounts:
        - name: job-config
          mountPath: /data/seatunnel.streaming.conf
          subPath: seatunnel.streaming.conf
  volumes:
    - name: job-config
      configMap:
        name: seatunnel-job-config
```

Apply it:

```bash
kubectl apply -f seatunnel-local.yaml
```

View logs:

```bash
kubectl logs -f seatunnel-local
```

Delete the Pod:

```bash
kubectl delete -f seatunnel-local.yaml
```

## Recommendation

Use Local Mode to verify images, connectors, and job configuration. If you need long-running service, REST API submission, cluster failover, or independent scaling, use [Separated Cluster Mode](separated-cluster-mode.md).
