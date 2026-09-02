---
sidebar_position: 4
---

# Separated Cluster Mode

In Separated Cluster Mode, SeaTunnel Engine Master and Worker run in separate processes. Master handles job scheduling, REST API, job submission, and IMap state storage. Worker executes tasks, does not participate in Master election, and does not store IMap data.

This is the recommended deployment mode for Kubernetes production environments. Deploy both Master and Worker with StatefulSet to avoid election instability or sudden slot shortage when rolling updates, node eviction, or rescheduling terminate too many nodes at once.

## Recommended Topology

| Component | Recommended workload | Minimum replicas | Production recommendation |
| --- | --- | --- | --- |
| Master | StatefulSet | 1 | At least 2 for HA and IMap backup |
| Worker | StatefulSet | 1 | Scale according to job parallelism and slot planning |
| Hazelcast discovery | Headless Service | 1 | `publishNotReadyAddresses: true` |
| REST API | ClusterIP Service | 1 | Expose with Ingress or LoadBalancer as needed |

:::tip Tip
A single Master can start the cluster, but it does not provide high availability. If `backup-count: 1` is used, deploy at least 2 Masters. Otherwise, the cluster cannot rely on backup replicas to recover IMap state after a Master failure.
:::

## Create ConfigMaps

Split configuration by role to avoid an overly long YAML file and to make Master, Worker, and client configuration easier to update independently. Access keys, passwords, and tokens must be managed through Secret in production and should not be written directly into ConfigMap.

### Master Hazelcast ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: seatunnel-master-hazelcast-config
data:
  hazelcast-master.yaml: |
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
      map:
        engine*:
          map-store:
            enabled: true
            initial-mode: EAGER
            factory-class-name: org.apache.seatunnel.engine.server.persistence.FileMapStoreFactory
            properties:
              type: hdfs
              namespace: /seatunnel/imap
              clusterName: seatunnel-cluster
              storage.type: hdfs
              fs.defaultFS: hdfs://namenode:8020
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

### Worker Hazelcast ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: seatunnel-worker-hazelcast-config
data:
  hazelcast-worker.yaml: |
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
      member-attributes:
        rule:
          type: string
          value: worker
```

These examples use Hazelcast Kubernetes API discovery. If you prefer DNS discovery and do not want Hazelcast to call the Kubernetes API, replace the `join.kubernetes` block in both `hazelcast-master.yaml` and `hazelcast-worker.yaml` with:

```yaml
join:
  kubernetes:
    enabled: true
    service-dns: seatunnel-cluster.default.svc.cluster.local
    service-dns-timeout: 10
```

:::info Note
When DNS discovery is used, the RBAC section below is not required for member discovery. If you skip the RBAC manifest, also remove `serviceAccountName: seatunnel` from both StatefulSets or create that ServiceAccount separately.
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
        telemetry:
          metric:
            enabled: false
          logs:
            scheduled-deletion-enable: true
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
              storage.type: hdfs
              namespace: /seatunnel/checkpoint/
              fs.defaultFS: hdfs://namenode:8020
        http:
          enable-http: true
          port: 8080
```

:::caution Warning
If S3, OSS, COS, OBS, TOS, or another object store is used for checkpoint or MapStore, every Pod must have network access and credentials. Do not put `secretKeyRef` inside the YAML content stored in ConfigMap. Render the final configuration before deployment, or use credential mechanisms supported by the underlying filesystem, such as Hadoop credential provider, cloud-provider credential chains, or mounted credential files.
:::

## Create RBAC for API Discovery

The default `namespace`, `service-name`, and `service-port` settings in `hazelcast-master.yaml` and `hazelcast-worker.yaml` use Hazelcast Kubernetes API discovery. In RBAC-enabled clusters, create a ServiceAccount, Role, and RoleBinding before starting the StatefulSets. Skip this section if you use `service-dns` discovery instead:

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

`seatunnel-cluster` is the Headless Service for Hazelcast member discovery. Both Master and Worker use it to join the same cluster.

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
---
apiVersion: v1
kind: Service
metadata:
  name: seatunnel-master
  labels:
    app: seatunnel-master
    component: master
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
    component: master
---
apiVersion: v1
kind: Service
metadata:
  name: seatunnel-worker
  labels:
    app: seatunnel-worker
    component: worker
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
    component: worker
```

## Create Master StatefulSet

The StatefulSet examples below use a 16 GB JVM heap, request 4 CPUs and 20 GiB of memory, and limit the container to 8 CPUs and 24 GiB of memory. The memory headroom is reserved for metaspace, direct memory, thread stacks, and other native memory, while the CPU allocation provides capacity for task execution, garbage collection, and engine coordination. For large-scale data processing, a 32 GB JVM heap is recommended; increase the CPU request and limit to 8 and 16, and the memory request and limit to 36 GiB and 40 GiB as a starting point. Adjust these values based on connector characteristics, job parallelism, and observed CPU, GC, and memory utilization.

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: seatunnel-master
  labels:
    app: seatunnel
    component: master
spec:
  serviceName: seatunnel-cluster
  replicas: 2
  selector:
    matchLabels:
      app: seatunnel
      component: master
  template:
    metadata:
      labels:
        app: seatunnel
        component: master
    spec:
      serviceAccountName: seatunnel
      containers:
        - name: app
          image: seatunnel:3.0.0
          imagePullPolicy: IfNotPresent
          command:
            - /opt/seatunnel/bin/seatunnel-cluster.sh
            - -r
            - master
            - '-DJvmOption=-Xms16g -Xmx16g'
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
              cpu: "4"
              memory: 20Gi
            limits:
              cpu: "8"
              memory: 24Gi
          volumeMounts:
            - name: hazelcast-master-config
              mountPath: /opt/seatunnel/config/hazelcast-master.yaml
              subPath: hazelcast-master.yaml
            - name: client-config
              mountPath: /opt/seatunnel/config/hazelcast-client.yaml
              subPath: hazelcast-client.yaml
            - name: engine-config
              mountPath: /opt/seatunnel/config/seatunnel.yaml
              subPath: seatunnel.yaml
      terminationGracePeriodSeconds: 120
      volumes:
        - name: hazelcast-master-config
          configMap:
            name: seatunnel-master-hazelcast-config
        - name: client-config
          configMap:
            name: seatunnel-client-config
        - name: engine-config
          configMap:
            name: seatunnel-engine-config
```

## Create Worker StatefulSet

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: seatunnel-worker
  labels:
    app: seatunnel
    component: worker
spec:
  serviceName: seatunnel-cluster
  replicas: 2
  selector:
    matchLabels:
      app: seatunnel
      component: worker
  template:
    metadata:
      labels:
        app: seatunnel
        component: worker
    spec:
      serviceAccountName: seatunnel
      containers:
        - name: app
          image: seatunnel:3.0.0
          imagePullPolicy: IfNotPresent
          command:
            - /opt/seatunnel/bin/seatunnel-cluster.sh
            - -r
            - worker
            - '-DJvmOption=-Xms16g -Xmx16g'
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
              cpu: "4"
              memory: 20Gi
            limits:
              cpu: "8"
              memory: 24Gi
          volumeMounts:
            - name: hazelcast-worker-config
              mountPath: /opt/seatunnel/config/hazelcast-worker.yaml
              subPath: hazelcast-worker.yaml
            - name: client-config
              mountPath: /opt/seatunnel/config/hazelcast-client.yaml
              subPath: hazelcast-client.yaml
            - name: engine-config
              mountPath: /opt/seatunnel/config/seatunnel.yaml
              subPath: seatunnel.yaml
      terminationGracePeriodSeconds: 120
      volumes:
        - name: hazelcast-worker-config
          configMap:
            name: seatunnel-worker-hazelcast-config
        - name: client-config
          configMap:
            name: seatunnel-client-config
        - name: engine-config
          configMap:
            name: seatunnel-engine-config
```

Custom plugins are not included in the base StatefulSet. If extra plugins are required, see [Plugin Loading](configuration.md#plugin-loading) and manage them separately with a custom image, initContainer overlay, PersistentVolume, or object storage.

## Health Check and Graceful Shutdown

Add the following container fragment to both Master and Worker. `startupProbe` avoids killing slow starts. `preStop` calls the SeaTunnel stop script first, then waits for the `SeaTunnelServer` process to exit. Together with `terminationGracePeriodSeconds`, this reduces the impact of rolling updates and node eviction on running jobs.

```yaml
startupProbe:
  tcpSocket:
    port: 5801
  periodSeconds: 10
  failureThreshold: 30
readinessProbe:
  tcpSocket:
    port: 5801
  initialDelaySeconds: 31
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

## Apply Manifests

:::info Note
Apply `seatunnel-rbac.yaml` only when API discovery is used, or when the StatefulSets keep `serviceAccountName: seatunnel`.
:::

```bash
kubectl apply -f seatunnel-master-hazelcast-config.yaml
kubectl apply -f seatunnel-worker-hazelcast-config.yaml
kubectl apply -f seatunnel-client-config.yaml
kubectl apply -f seatunnel-engine-config.yaml
kubectl apply -f seatunnel-rbac.yaml
kubectl apply -f seatunnel-services.yaml
kubectl apply -f seatunnel-master.yaml
kubectl apply -f seatunnel-worker.yaml
```

Check Pod status:

```bash
kubectl get pods -l app=seatunnel
```

Access REST API:

```bash
kubectl port-forward svc/seatunnel-master 8080:8080
curl http://127.0.0.1:8080/system-monitoring-information
```

To submit jobs, see [REST API V2](../../engines/zeta/rest-api-v2.md).

## Scale Worker

Scaling Workers increases available slots:

```bash
kubectl scale statefulset seatunnel-worker --replicas=4
```

Before scaling down, confirm that no critical running job depends on the Worker being removed and that remaining Workers have enough slots for current jobs. Stop or migrate jobs before scaling down Workers.
