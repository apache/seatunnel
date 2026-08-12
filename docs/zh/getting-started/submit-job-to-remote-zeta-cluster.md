---
sidebar_position: 4
---

# 向远程 Zeta 集群提交作业

本手册介绍如何将 SeaTunnel 作业提交至**远程** Zeta 集群，而不仅仅是本地运行。内容涵盖
Docker 单节点、多节点集群、Kubernetes（通过 `kubectl port-forward` 或 NodePort/LoadBalancer）
以及 EKS/Helm 部署场景。

---

## 1. 前置条件

| 需求 | 说明 |
|---|---|
| SeaTunnel 客户端已安装 | 本地拥有 SeaTunnel 目录，可执行 `bin/seatunnel.sh` |
| 集群可达 | 提交机器能够访问 REST API 端口（默认 **8080**） |
| 作业配置文件就绪 | HOCON `.conf` 或 JSON 格式的作业配置文件 |

---

## 2. 从本地机器向远程集群提交

### 2.1 `--master` 参数（Zeta）

所有 SeaTunnel Zeta 客户端命令均支持 `--master` 参数，用于指定集群连接地址：

```bash
bin/seatunnel.sh \
  --config job.conf \
  --master seatunnel://192.168.1.100:5801
```

Zeta 集群内部默认端口为 **5801**，与 REST API 端口（8080）不同。`--master` 参数用于
通过 Hazelcast 成员协议直接连接集群。

### 2.2 使用 REST API（推荐用于自动化场景）

对于 CI/CD 流水线和脚本，推荐使用 REST API 提交：

```bash
curl -X POST http://192.168.1.100:8080/submit-job \
  -H "Content-Type: application/json" \
  -d @job.json
```

完整请求与响应说明请参见 [REST API v2](../engines/zeta/rest-api-v2.md)。

---

## 3. Docker：单节点提交

### 3.1 启动已启用 REST API 的 Zeta 容器

```bash
docker run -d --name seatunnel \
  -p 8080:8080 \
  -e ST_DOCKER_MEMBER_COUNT=1 \
  apache/seatunnel:<version>
```

### 3.2 从容器外部提交作业

```bash
curl -X POST http://localhost:8080/submit-job \
  -H "Content-Type: application/json" \
  -d '{
    "env": { "job.name": "test", "job.mode": "BATCH" },
    "source": [{ "plugin_name": "FakeSource", "plugin_output": "fake",
                 "row.num": 10,
                 "schema": { "fields": { "id": "int", "name": "string" } } }],
    "transform": [],
    "sink": [{ "plugin_name": "Console", "plugin_input": ["fake"] }]
  }'
```

### 3.3 在容器内执行本地 smoke 测试

```bash
docker run -d --name seatunnel \
  -p 8080:8080 \
  -v /path/to/your/jobs:/jobs \
  apache/seatunnel:<version>

# 在容器内执行
docker exec seatunnel \
  /opt/seatunnel/bin/seatunnel.sh --config /jobs/my-job.conf --master local
```

该命令仅适用于在容器内做快速本地 smoke 测试。它**不是**向远程 Zeta
集群提交作业，因为 `--master local` 会在当前容器进程内本地启动作业。

---

## 4. Docker：多节点集群

### 4.1 Docker Compose 示例

```yaml
version: "3.8"
services:
  master:
    image: apache/seatunnel:<version>
    container_name: seatunnel-master
    ports:
      - "8080:8080"
      - "5801:5801"
    environment:
      ST_DOCKER_MEMBER_COUNT: 2
    networks:
      - st-net

  worker:
    image: apache/seatunnel:<version>
    container_name: seatunnel-worker
    environment:
      ST_DOCKER_MEMBER_COUNT: 2
    networks:
      - st-net
    depends_on:
      - master

networks:
  st-net:
    driver: bridge
```

启动集群：

```bash
docker-compose up -d
```

向 master 提交作业：

```bash
curl -X POST http://localhost:8080/submit-job \
  -H "Content-Type: application/json" \
  -d @job.json
```

### 4.2 网络端口要求（Docker）

| 端口 | 协议 | 用途 |
|---|---|---|
| **5801** | TCP | Hazelcast 集群内部成员通信 |
| **8080** | TCP | REST API（作业提交 / 监控）|

需确保 Docker 网络中所有集群成员之间 5801 端口互通。REST API 仅需在 Master 节点上对外暴露。

---

## 5. Kubernetes：作业提交

### 5.1 使用 `kubectl port-forward`（开发 / 临时提交）

在 Kubernetes 上部署 SeaTunnel（参见 [Kubernetes 部署](kubernetes/kubernetes.mdx)）后，将 Master
Pod 的 REST 端口转发到本地：

```bash
# 查找 master pod
kubectl get pods -n seatunnel

# 转发 REST 端口
kubectl port-forward -n seatunnel \
  pod/seatunnel-master-0 8080:8080
```

在另一个终端提交作业：

```bash
curl -X POST http://localhost:8080/submit-job \
  -H "Content-Type: application/json" \
  -d @job.json
```

### 5.2 使用 NodePort 服务（测试 / 生产）

若集群通过 `NodePort` 服务暴露 Master：

```bash
# 获取 NodePort
kubectl get svc -n seatunnel seatunnel-master-rest

# 使用节点 IP 和节点端口提交
curl -X POST http://<node-ip>:<node-port>/submit-job \
  -H "Content-Type: application/json" \
  -d @job.json
```

### 5.3 使用 LoadBalancer 服务

```bash
LB_IP=$(kubectl get svc -n seatunnel seatunnel-master-rest \
  -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

curl -X POST http://${LB_IP}:8080/submit-job \
  -H "Content-Type: application/json" \
  -d @job.json
```

---

## 6. Kubernetes：通过 ConfigMap 管理作业配置

在集群内运行作业时，建议将作业配置文件以 ConfigMap 形式挂载，而非打包到镜像中：

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: seatunnel-job-config
  namespace: seatunnel
data:
  cdc-job.conf: |
    env {
      job.name = "cdc-prod"
      job.mode = STREAMING
      checkpoint.interval = 30000
    }
    source {
      MySQL-CDC {
        ...
      }
    }
    sink {
      ...
    }
```

在 Pod spec 中挂载：

```yaml
volumeMounts:
  - name: job-config
    mountPath: /opt/seatunnel/jobs
volumes:
  - name: job-config
    configMap:
      name: seatunnel-job-config
```

通过 `kubectl exec` 提交：

```bash
kubectl exec -n seatunnel seatunnel-master-0 -- \
  /opt/seatunnel/bin/seatunnel.sh \
  --config /opt/seatunnel/jobs/cdc-job.conf
```

---

## 7. Amazon EKS / Helm 部署

### 7.1 Helm 安装

```bash
helm repo add seatunnel https://apache.github.io/seatunnel-helm-charts
helm repo update

helm install seatunnel seatunnel/seatunnel \
  --namespace seatunnel \
  --create-namespace \
  --set master.replicaCount=2 \
  --set worker.replicaCount=4 \
  --set master.service.type=LoadBalancer
```

### 7.2 EKS 获取 Load Balancer 主机名

```bash
kubectl get svc -n seatunnel seatunnel-master \
  -o jsonpath='{.status.loadBalancer.ingress[0].hostname}'
```

以该主机名作为 API 端点：

```bash
export ST_HOST=$(kubectl get svc -n seatunnel seatunnel-master \
  -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')

curl -X POST http://${ST_HOST}:8080/submit-job \
  -H "Content-Type: application/json" \
  -d @job.json
```

### 7.3 通过 Helm values 自定义资源配置

```yaml
# values-prod.yaml
master:
  replicaCount: 2
  resources:
    requests:
      memory: "4Gi"
      cpu: "2"
    limits:
      memory: "8Gi"
      cpu: "4"

worker:
  replicaCount: 8
  resources:
    requests:
      memory: "8Gi"
      cpu: "4"
    limits:
      memory: "16Gi"
      cpu: "8"

seatunnel:
  config:
    engine:
      backup-count: 2
      queue-type: blockingqueue
      print-execution-info-interval: 60
      http:
        enable-http: true
        port: 8080
```

应用配置：

```bash
helm upgrade seatunnel seatunnel/seatunnel \
  --namespace seatunnel \
  -f values-prod.yaml
```

---

## 8. 网络与端口要求

| 端口 | 协议 | 使用方 | 注意事项 |
|---|---|---|---|
| **5801** | TCP | Hazelcast 集群 | 成员间通信；生产环境不要对外暴露 |
| **8080** | TCP | REST API | 生产环境建议通过认证网关暴露 |
| **8090** | TCP | Web UI | 可选，仅用于管理看板 |

在 Kubernetes 中，建议设置 NetworkPolicy 将 5801 端口限制在 SeaTunnel 命名空间内：

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: seatunnel-internal
  namespace: seatunnel
spec:
  podSelector:
    matchLabels:
      app: seatunnel
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: seatunnel
      ports:
        - port: 5801
```

---

## 9. 故障排查

| 现象 | 可能原因 | 修复方法 |
|---|---|---|
| 8080 端口 `Connection refused` | REST API 未启用或端口错误 | 设置 `enable-http: true`；检查 `port` 配置 |
| Worker 无法加入集群 | 防火墙阻断 5801 端口 | 开放所有集群节点间 TCP 5801 |
| `kubectl port-forward` 断开 | 空闲超时或 Pod 重启 | 重新执行 port-forward；考虑改用 NodePort |
| 作业已提交但状态始终为 `WAITING` | 无可用 Worker 槽位 | 扩容 Worker 副本数或检查资源配额 |
| EKS LoadBalancer 主机名无法解析 | DNS 传播延迟 | 等待 1–2 分钟；用 `nslookup` 验证 |
| Helm 安装卡在 `pending-install` | 上次安装失败残留 | 执行 `helm rollback` 或 `helm uninstall` 后重试 |

---

## 参考

- [Kubernetes 部署指南](kubernetes/kubernetes.mdx)
- [Helm Chart 参考](kubernetes/helm.md)
- [Docker 快速入门](docker/docker.md)
- [REST API v2](../engines/zeta/rest-api-v2.md)
- [Zeta 引擎配置](../engines/zeta/deployment.md)
