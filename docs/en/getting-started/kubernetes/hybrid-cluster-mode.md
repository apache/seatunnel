---
sidebar_position: 3
---

# Hybrid Cluster Mode

In Hybrid Cluster Mode, SeaTunnel Engine Master and Worker run in the same process. Every node can participate in Master election and execute tasks.

This mode is simple to deploy and suitable for small clusters.

:::tip Tip
For production environments, [Separated Cluster Mode](separated-cluster-mode.md) is recommended because it isolates scheduling resources from execution resources.
:::

## Deployment Principles

- Deploy hybrid cluster nodes with StatefulSet.
- Use a Headless Service for Hazelcast Kubernetes discovery.
- Use the same `hazelcast.yaml`, `hazelcast-client.yaml`, and `seatunnel.yaml` for all nodes.
- Each node handles both scheduling and execution, so resource settings must cover both Master and Worker load.

## Create ConfigMaps

:::caution Warning
Store sensitive values in Secret for production environments. The following example only shows non-sensitive configuration.
:::

Split ConfigMaps by responsibility to avoid an overly long YAML file.

### Hazelcast ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: seatunnel-hazelcast-config
data:
  hazelcast.yaml: |
    hazelcast:
      cluster-name: seatunnel-cluster
      network:
        rest-api:
          enabled: true
          endpoint-groups:
            CLUSTER_WRITE:
              enabled: true
            DATA:
              enabled: true
        port:
          auto-increment: false
          port: 5801
        join:
          kubernetes:
            enabled: true
            namespace: default
            service-name: seatunnel-cluster
            service-port: 5801
      properties:
        hazelcast.shutdownhook.policy: GRACEFUL
        hazelcast.invocation.max.retry.count: 20
        hazelcast.tcp.join.port.try.count: 30
        hazelcast.logging.type: log4j2
        hazelcast.operation.generic.thread.count: 50
        hazelcast.heartbeat.failuredetector.type: phi-accrual
        hazelcast.heartbeat.interval.seconds: 2
        hazelcast.max.no.heartbeat.seconds: 180
        hazelcast.heartbeat.phiaccrual.failuredetector.threshold: 10
        hazelcast.heartbeat.phiaccrual.failuredetector.sample.size: 200
        hazelcast.heartbeat.phiaccrual.failuredetector.min.std.dev.millis: 100
```

This example uses Hazelcast Kubernetes API discovery. If you prefer DNS discovery and do not want Hazelcast to call the Kubernetes API, replace the `join.kubernetes` block with:

```yaml
join:
  kubernetes:
    enabled: true
    service-dns: seatunnel-cluster.default.svc.cluster.local
    service-dns-timeout: 10
```

:::info Note
When DNS discovery is used, the RBAC section below is not required for member discovery. If you skip the RBAC manifest, also remove `serviceAccountName: seatunnel` from the StatefulSet or create that ServiceAccount separately.
:::

### Hazelcast Client ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: seatunnel-client-config
data:
  hazelcast-client.yaml: |
    hazelcast-client:
      cluster-name: seatunnel-cluster
      properties:
        hazelcast.logging.type: log4j2
      connection-strategy:
        connection-retry:
          cluster-connect-timeout-millis: 7000
      network:
        cluster-members:
          # Replace `default` with your namespace if SeaTunnel is deployed elsewhere.
          - seatunnel-cluster.default.svc.cluster.local:5801
```

### SeaTunnel Engine ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: seatunnel-engine-config
data:
  seatunnel.yaml: |
    seatunnel:
      engine:
        backup-count: 1
        history-job-expire-minutes: 1440
        print-execution-info-interval: 300
        classloader-cache-mode: true
        slot-service:
          dynamic-slot: false
          slot-num: 8
        job-schedule-strategy: WAIT
        checkpoint:
          interval: 180000
          timeout: 30000
          storage:
            type: hdfs
            max-retained: 3
            plugin-config:
              namespace: /seatunnel/checkpoint/
              storage.type: hdfs
              fs.defaultFS: hdfs://namenode:8020
        http:
          enable-http: true
          port: 8080
```

## Create RBAC for API Discovery

The default `namespace`, `service-name`, and `service-port` settings in `hazelcast.yaml` use Hazelcast Kubernetes API discovery. In RBAC-enabled clusters, create a ServiceAccount, Role, and RoleBinding before starting the StatefulSet. Skip this section if you use `service-dns` discovery instead:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: seatunnel
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: seatunnel-hazelcast-discovery
rules:
  - apiGroups: [""]
    resources: ["pods", "services", "endpoints"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: seatunnel-hazelcast-discovery
subjects:
  - kind: ServiceAccount
    name: seatunnel
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: seatunnel-hazelcast-discovery
```

## Create Services

```yaml
apiVersion: v1
kind: Service
metadata:
  name: seatunnel-cluster
  labels:
    app: seatunnel-cluster
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  ports:
    - name: hazelcast
      port: 5801
      targetPort: 5801
  selector:
    app: seatunnel
    component: hybrid
---
apiVersion: v1
kind: Service
metadata:
  name: seatunnel
  labels:
    app: seatunnel
    component: hybrid
spec:
  type: ClusterIP
  ports:
    - name: rest-api
      port: 8080
      targetPort: 8080
    - name: hazelcast
      port: 5801
      targetPort: 5801
  selector:
    app: seatunnel
    component: hybrid
```

## Create StatefulSet

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: seatunnel
  labels:
    app: seatunnel
    component: hybrid
spec:
  serviceName: seatunnel-cluster
  replicas: 3
  selector:
    matchLabels:
      app: seatunnel
      component: hybrid
  template:
    metadata:
      labels:
        app: seatunnel
        component: hybrid
    spec:
      serviceAccountName: seatunnel
      containers:
        - name: app
          image: seatunnel:3.0.0
          imagePullPolicy: IfNotPresent
          command:
            - /opt/seatunnel/bin/seatunnel-cluster.sh
          env:
            - name: SEATUNNEL_HOME
              value: /opt/seatunnel
            - name: HAZELCAST_CLUSTER_NAME
              value: seatunnel-cluster
          ports:
            - containerPort: 8080
              name: rest-api
            - containerPort: 5801
              name: hazelcast
          resources:
            requests:
              cpu: "1"
              memory: 2Gi
            limits:
              cpu: "2"
              memory: 4Gi
          volumeMounts:
            - name: hazelcast-config
              mountPath: /opt/seatunnel/config/hazelcast.yaml
              subPath: hazelcast.yaml
            - name: client-config
              mountPath: /opt/seatunnel/config/hazelcast-client.yaml
              subPath: hazelcast-client.yaml
            - name: engine-config
              mountPath: /opt/seatunnel/config/seatunnel.yaml
              subPath: seatunnel.yaml
      terminationGracePeriodSeconds: 120
      volumes:
        - name: hazelcast-config
          configMap:
            name: seatunnel-hazelcast-config
        - name: client-config
          configMap:
            name: seatunnel-client-config
        - name: engine-config
          configMap:
            name: seatunnel-engine-config
```

## Health Check and Graceful Shutdown

For cluster mode, add `startupProbe`, health checks, and `preStop` to each SeaTunnel container to avoid killing slow starts and to reduce the impact of rolling updates or node eviction.

```yaml
startupProbe:
  tcpSocket:
    port: 5801
  periodSeconds: 10
  failureThreshold: 30
readinessProbe:
  tcpSocket:
    port: 5801
  initialDelaySeconds: 30
  periodSeconds: 30
  timeoutSeconds: 5
  failureThreshold: 3
livenessProbe:
  tcpSocket:
    port: 5801
  initialDelaySeconds: 30
  periodSeconds: 30
  timeoutSeconds: 5
  failureThreshold: 3
lifecycle:
  preStop:
    exec:
      command:
        - /bin/sh
        - -c
        - |
          /opt/seatunnel/bin/stop-seatunnel-cluster.sh
          while kill -0 $(ps -ef | grep SeaTunnelServer | grep -v grep | awk '{print $2}') 2>/dev/null; do
            sleep 1
          done
```

Apply the manifests:

:::info Note
Apply `seatunnel-rbac.yaml` only when API discovery is used, or when the StatefulSet keeps `serviceAccountName: seatunnel`.
:::

```bash
kubectl apply -f seatunnel-hazelcast-config.yaml
kubectl apply -f seatunnel-client-config.yaml
kubectl apply -f seatunnel-engine-config.yaml
kubectl apply -f seatunnel-rbac.yaml
kubectl apply -f seatunnel-services.yaml
kubectl apply -f seatunnel-hybrid.yaml
```

## Access REST API

```bash
kubectl port-forward svc/seatunnel 8080:8080
curl http://127.0.0.1:8080/system-monitoring-information
```

## Recommendation

:::tip Tip
In Hybrid Cluster Mode, Master nodes also run tasks. When task load is high, execution load may affect Master election, scheduling, and REST API stability. For a more stable production deployment, use [Separated Cluster Mode](separated-cluster-mode.md).
:::
