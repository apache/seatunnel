# SeaTunnel Kubernetes Session Cluster (MVP)

This directory contains a minimal Kubernetes Session Cluster. The two masters are a StatefulSet;
workers are bare Pods created by the active master. There is no worker Deployment or StatefulSet.

## Start

```bash
kubectl apply -k deploy/kubernetes/session
kubectl -n seatunnel get pods -w
```

The active master creates `min-workers` (default `2`) workers. Submit jobs through the REST
service:

```bash
kubectl -n seatunnel port-forward svc/seatunnel-session-cluster-rest 8080:8080
curl -X POST http://127.0.0.1:8080/submit-job \
  -H 'Content-Type: application/json' \
  -d @job.json
```

For the native Hazelcast client, create a client configuration that points at
`seatunnel-session-cluster-client.seatunnel.svc.cluster.local:5801`, then use:

```bash
HAZELCAST_CLIENT_CONFIG=/path/to/hazelcast-client.yaml \
  bin/seatunnel.sh --config job.conf --master cluster
```

## Stop

Stop jobs first, then delete the master StatefulSet before deleting the root ConfigMap. This
prevents the still-running master from replacing workers while their Pods are being removed:

```bash
kubectl -n seatunnel delete statefulset seatunnel-session-cluster-master
kubectl delete -k deploy/kubernetes/session
```

Checkpoint storage and user-owned PVCs are not deleted by the session manifests.

## Notes

- The image must contain `seatunnel-session-cluster.sh` and the Kubernetes resource-manager provider.
- Worker templates are immutable for the lifetime of a session. Create a new session to change
  slots, CPU, memory or the worker image.
- Only fixed slots are supported in this MVP. The worker `seatunnel.yaml` must use
  `slot-service.dynamic-slot: false` and a `slot-num` equal to `session.worker.slots`.
- The sample uses `file:///tmp/` for checkpoints and IMap persistence so it can run on a local
  single-node cluster. Replace it with HDFS, S3 or a PVC for production.
