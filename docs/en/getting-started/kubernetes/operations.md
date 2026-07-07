---
sidebar_position: 6
---

# Kubernetes Operations

This page describes common operations for SeaTunnel Zeta Engine on Kubernetes.

## Check Cluster Status

```bash
kubectl get pods -l app=seatunnel
kubectl get statefulset
kubectl get svc
```

View Master logs:

```bash
kubectl logs -f seatunnel-master-0
```

View Worker logs:

```bash
kubectl logs -f seatunnel-worker-0
```

## Access REST API

Inside the cluster, access REST API through the `seatunnel-master` Service. For debugging, use port forwarding:

```bash
kubectl port-forward svc/seatunnel-master 8080:8080
curl http://127.0.0.1:8080/system-monitoring-information
curl http://127.0.0.1:8080/running-jobs
```

In production, expose REST API through Ingress or LoadBalancer as needed, and add authentication, network policy, and access control.

## Scale Worker Up

Scaling Workers increases available slots:

```bash
kubectl scale statefulset seatunnel-worker --replicas=4
```

Confirm Workers are Ready:

```bash
kubectl get pods -l app=seatunnel,component=worker
```

Before submitting large jobs, scale Workers first to avoid long waits caused by insufficient slots.

## Scale Worker Down

Before scaling down, confirm:

- Running jobs do not depend on the Worker being removed.
- Remaining Workers have enough slots for current and future jobs.
- Checkpoints have completed successfully.

Do not scale down multiple Workers at once. Decrease replicas one by one and observe job status.

## Rolling Updates

When image or configuration changes, StatefulSet updates Pods in order. Recommended practices:

- Configure `preStop` to call `stop-seatunnel-cluster.sh`.
- Set a long enough `terminationGracePeriodSeconds`.
- Check Master replicas and Worker slot capacity before updates.
- Avoid updating Master and Worker at the same time during peak hours.
- If configuration files are mounted through `subPath`, ConfigMap updates are not propagated into the running container automatically. Run a rolling restart or use a tool such as Reloader to trigger restarts.

If tools such as Reloader automatically restart Pods, ensure they do not restart too many Workers in a short time.

## PodDisruptionBudget

Configure PodDisruptionBudget for Master and Worker in production to limit unavailable Pods during voluntary disruption:

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: seatunnel-master-pdb
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: seatunnel
      component: master
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: seatunnel-worker-pdb
spec:
  maxUnavailable: 1
  selector:
    matchLabels:
      app: seatunnel
      component: worker
```

## Troubleshooting

### Pod Cannot Join the Cluster

Check:

- Whether the `seatunnel-cluster` Headless Service exists.
- Whether `namespace`, `service-name`, and `service-port` in `hazelcast.yaml`, `hazelcast-master.yaml`, or `hazelcast-worker.yaml` match the Service.
- Whether port 5801 is exposed by the Service.
- Whether Pod labels match Service selectors.

### Worker Readiness Fails

Check:

- Whether the container starts normally.
- Whether `/opt/seatunnel/config/hazelcast-worker.yaml` or `/opt/seatunnel/config/hazelcast.yaml` is mounted correctly.
- Whether the connector or custom plugin jars required by the job exist.
- Whether port 5801 is listening.

### Available Slots Are Insufficient

Check:

- Whether Worker replica count is enough.
- Whether `slot-service.dynamic-slot` and `slot-num` match expectation.
- Whether Workers are rolling, evicted, or restarting.
- Whether too many Worker Pods were terminated at once.

### Checkpoint or MapStore Write Fails

Check:

- Whether the storage path is shared storage or object storage.
- Whether Pods have network access.
- Whether Secret or mounted credentials are correct.
- Whether the storage path has read/write permissions.

### REST API Cannot Be Accessed

Check:

- Whether the `seatunnel-master` Service exists.
- Whether `seatunnel.engine.http.enable-http` in `seatunnel.yaml` is `true`.
- Whether the REST API port matches Service targetPort.
- Whether Ingress or LoadBalancer forwarding rules are correct.
