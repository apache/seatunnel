---
sidebar_position: 3
---

# 混合集群模式

混合集群模式中，SeaTunnel Engine 的 Master 与 Worker 运行在同一个进程中。所有节点都可以参与 Master 选举，也都可以执行任务。

该模式部署简单，适合小规模集群。生产环境更推荐使用 [分离集群模式](separated-cluster-mode.md)，将调度与执行资源隔离。

## 部署原则

- 使用 StatefulSet 部署混合集群节点。
- 使用 Headless Service 提供 Hazelcast Kubernetes discovery。
- 所有节点使用同一份 `hazelcast.yaml`、`hazelcast-client.yaml` 和 `seatunnel.yaml`。
- 节点既承担调度职责，也承担执行职责，资源配置需要同时考虑 Master 与 Worker 负载。

## 创建 ConfigMap

生产环境应将敏感信息放入 Secret。以下示例只展示非敏感配置。建议按配置职责拆分 ConfigMap，避免单个 YAML 过长。

### Hazelcast 配置

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
        hazelcast.invocation.max.retry.count: 50
        hazelcast.tcp.join.port.try.count: 10
        hazelcast.logging.type: log4j2
        hazelcast.phone.home.enabled: false
        hazelcast.operation.generic.thread.count: 32
        hazelcast.heartbeat.failuredetector.type: phi-accrual
        hazelcast.heartbeat.interval.seconds: 5
        hazelcast.max.no.heartbeat.seconds: 30
        hazelcast.heartbeat.phiaccrual.failuredetector.threshold: 10
        hazelcast.heartbeat.phiaccrual.failuredetector.sample.size: 200
        hazelcast.heartbeat.phiaccrual.failuredetector.min.std.dev.millis: 100
        hazelcast.shutdownhook.policy: GRACEFUL
        hazelcast.graceful.shutdown.max.wait: 120
```

### Hazelcast Client 配置

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
          # 如果 SeaTunnel 部署在其他命名空间，需要将 default 替换为实际命名空间。
          - seatunnel-cluster.default.svc.cluster.local:5801
```

### SeaTunnel Engine 配置

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

## 创建 Service

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

## 创建 StatefulSet

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

## 健康检查和优雅停止

集群模式建议为每个 SeaTunnel 容器添加 `startupProbe`、健康检查和 `preStop`，避免启动期误杀，并减少滚动更新或节点驱逐对集群的影响。

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

应用配置：

```bash
kubectl apply -f seatunnel-hazelcast-config.yaml
kubectl apply -f seatunnel-client-config.yaml
kubectl apply -f seatunnel-engine-config.yaml
kubectl apply -f seatunnel-services.yaml
kubectl apply -f seatunnel-hybrid.yaml
```

## 访问 REST API

```bash
kubectl port-forward svc/seatunnel 8080:8080
curl http://127.0.0.1:8080/system-monitoring-information
```

## 使用建议

混合集群模式中，Master 节点同时运行任务。当任务负载较高时，执行负载可能影响 Master 选举、调度和 REST API 稳定性。需要更稳定的生产部署时，请使用 [分离集群模式](separated-cluster-mode.md)。
