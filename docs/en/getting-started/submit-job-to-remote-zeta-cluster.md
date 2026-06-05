---
sidebar_position: 4
---

# Submitting Jobs to a Remote Zeta Cluster

This guide explains how to submit SeaTunnel jobs to a **remote** Zeta cluster rather than running
locally. It covers Docker single-node setups, multi-node clusters, Kubernetes (K8s) via `kubectl
port-forward` or direct NodePort/LoadBalancer access, and EKS/Helm deployments.

---

## 1. Prerequisites

| Requirement | Details |
|---|---|
| SeaTunnel client installed | The SeaTunnel home directory is available locally with `bin/seatunnel.sh` |
| Cluster reachable | REST API port (default **8080**) accessible from the submitting machine |
| Job configuration file | A HOCON `.conf` or JSON payload ready |

---

## 2. Submitting from the Local Machine to a Remote Cluster

### 2.1 `--master` flag (Zeta)

All SeaTunnel Zeta client commands accept a `--master` argument that tells the client which cluster
to connect to:

```bash
bin/seatunnel.sh \
  --config job.conf \
  --master seatunnel://192.168.1.100:5801
```

The default Zeta cluster-internal port is **5801**. This is *different* from the REST API port
(8080). The `--master` flag is used when running the job via the SeaTunnel client binary, which
communicates with the cluster over the Hazelcast member protocol.

### 2.2 Using the REST API (recommended for automation)

For CI/CD pipelines and scripts, prefer REST API submission over the binary client:

```bash
curl -X POST http://192.168.1.100:8080/submit-job \
  -H "Content-Type: application/json" \
  -d @job.json
```

See [REST API v2](../engines/zeta/rest-api-v2.md) for the complete request and response reference.

---

## 3. Docker: Single-Node Submission

### 3.1 Start a Zeta container with REST enabled

```bash
docker run -d --name seatunnel \
  -p 8080:8080 \
  -e ST_DOCKER_MEMBER_COUNT=1 \
  apache/seatunnel:<version>
```

### 3.2 Submit a job from outside the container

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

### 3.3 Run a quick local smoke test inside the container

```bash
docker run -d --name seatunnel \
  -p 8080:8080 \
  -v /path/to/your/jobs:/jobs \
  apache/seatunnel:<version>

# Execute inside the container
docker exec seatunnel \
  /opt/seatunnel/bin/seatunnel.sh --config /jobs/my-job.conf --master local
```

This command is useful only for a container-local smoke test. It does **not**
submit the job to a remote Zeta cluster because `--master local` starts the job
inside that container process.

---

## 4. Docker: Multi-Node Cluster

### 4.1 Docker Compose example

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

Start the cluster:

```bash
docker-compose up -d
```

Submit a job to the master:

```bash
curl -X POST http://localhost:8080/submit-job \
  -H "Content-Type: application/json" \
  -d @job.json
```

### 4.2 Network requirements (Docker)

| Port | Protocol | Purpose |
|---|---|---|
| **5801** | TCP | Hazelcast cluster-internal member communication |
| **8080** | TCP | REST API (job submission / monitoring) |

Ensure the Docker network allows all cluster members to reach each other on port 5801.
The REST API only needs to be exposed on the master node(s).

---

## 5. Kubernetes: Job Submission

### 5.1 Using `kubectl port-forward` (development / ad-hoc)

After deploying SeaTunnel to Kubernetes (see [Kubernetes Deployment](kubernetes/kubernetes.mdx)), forward the
master pod's REST port to your local machine:

```bash
# Find the master pod
kubectl get pods -n seatunnel

# Forward REST port
kubectl port-forward -n seatunnel \
  pod/seatunnel-master-0 8080:8080
```

In a second terminal, submit a job:

```bash
curl -X POST http://localhost:8080/submit-job \
  -H "Content-Type: application/json" \
  -d @job.json
```

### 5.2 Using NodePort service (staging / production)

If your cluster exposes the SeaTunnel master via a `NodePort` service:

```bash
# Get the NodePort
kubectl get svc -n seatunnel seatunnel-master-rest

# Submit via node IP and node port
curl -X POST http://<node-ip>:<node-port>/submit-job \
  -H "Content-Type: application/json" \
  -d @job.json
```

### 5.3 Using LoadBalancer service

```bash
LB_IP=$(kubectl get svc -n seatunnel seatunnel-master-rest \
  -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

curl -X POST http://${LB_IP}:8080/submit-job \
  -H "Content-Type: application/json" \
  -d @job.json
```

---

## 6. Kubernetes: Job Configuration via ConfigMap

When running jobs inside the cluster, mount job configuration files as ConfigMaps rather than
baking them into the image:

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

Mount in the Pod spec:

```yaml
volumeMounts:
  - name: job-config
    mountPath: /opt/seatunnel/jobs
volumes:
  - name: job-config
    configMap:
      name: seatunnel-job-config
```

Then submit via `kubectl exec`:

```bash
kubectl exec -n seatunnel seatunnel-master-0 -- \
  /opt/seatunnel/bin/seatunnel.sh \
  --config /opt/seatunnel/jobs/cdc-job.conf
```

---

## 7. Amazon EKS / Helm Deployment

### 7.1 Helm install

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

### 7.2 EKS-specific: retrieve the load balancer hostname

```bash
kubectl get svc -n seatunnel seatunnel-master \
  -o jsonpath='{.status.loadBalancer.ingress[0].hostname}'
```

Use that hostname as the API endpoint:

```bash
export ST_HOST=$(kubectl get svc -n seatunnel seatunnel-master \
  -o jsonpath='{.status.loadBalancer.ingress[0].hostname}')

curl -X POST http://${ST_HOST}:8080/submit-job \
  -H "Content-Type: application/json" \
  -d @job.json
```

### 7.3 Customizing resources via Helm values

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

Apply with:

```bash
helm upgrade seatunnel seatunnel/seatunnel \
  --namespace seatunnel \
  -f values-prod.yaml
```

---

## 8. Network and Port Requirements

| Port | Protocol | Required By | Notes |
|---|---|---|---|
| **5801** | TCP | Hazelcast cluster | Member-to-member communication; do NOT expose publicly |
| **8080** | TCP | REST API | Expose only via authenticated gateway in production |
| **8090** | TCP | Web UI | Optional; only needed for the dashboard |

In Kubernetes, set up a `NetworkPolicy` to restrict port 5801 to the SeaTunnel namespace only:

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

## 9. Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| `Connection refused` on port 8080 | REST API not enabled or wrong port | Set `enable-http: true`; check `port` config |
| Workers not joining the cluster | Firewall blocks port 5801 | Open TCP 5801 between all cluster nodes |
| `kubectl port-forward` disconnects | Idle timeout or pod restart | Restart port-forward; consider NodePort instead |
| Job submitted but status always `WAITING` | No available worker slots | Scale up worker replicas or check resource quotas |
| EKS LoadBalancer hostname not resolving | DNS propagation delay | Wait 1–2 minutes; verify with `nslookup` |
| Helm install fails with `pending-install` | Previous failed release | Run `helm rollback` or `helm uninstall` then retry |

---

## See Also

- [Kubernetes Deployment Guide](kubernetes/kubernetes.mdx)
- [Helm Chart Reference](kubernetes/helm.md)
- [Docker Quick Start](docker/docker.md)
- [REST API v2](../engines/zeta/rest-api-v2.md)
- [Zeta Engine Configuration](../engines/zeta/deployment.md)
