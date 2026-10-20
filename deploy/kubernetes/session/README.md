# SeaTunnel Kubernetes Session Cluster (Experimental)

This directory contains a minimal Kubernetes Session Cluster. The two masters are a StatefulSet;
workers are bare Pods created by the active master. There is no worker Deployment or StatefulSet.

## Multiple Sessions in one namespace

`kubectl apply -k` applies the sample Session named `seatunnel-session-cluster`. To run another
Session in the same namespace, use the deployment helper with a unique session name:

```bash
bin/seatunnel-session.sh --session-name payments
```

The script reads `session.yaml` from `configmap-master.yaml`, renders its name into every resource
name, label, ConfigMap name, and Hazelcast/client service DNS entry, then calls `kubectl apply`.
It is a deployment helper, not a Session lifecycle CLI. Every Session in a namespace must use a
different `session.name`; `--session-name` overrides the value from the YAML. Use
`--manifests deploy/kubernetes/session` when running from the source tree, and `RENDER_ONLY=1` to
print the rendered manifests without applying them.

## Start

```bash
# The sample Session.
kubectl apply -k deploy/kubernetes/session

# Another Session in the same namespace.
bin/seatunnel-session.sh --session-name payments

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

For another Session, replace `seatunnel-session-cluster` with the `session.name` used by
`seatunnel-session.sh`.

For the native Hazelcast client, create a client configuration that points at
`<session-name>-client.<namespace>.svc.cluster.local:5801`, then use:

```bash
HAZELCAST_CLIENT_CONFIG=/path/to/hazelcast-client.yaml \
  bin/seatunnel.sh --config job.conf --master cluster
```

## Stop

Stop jobs first, then delete the master StatefulSet before deleting the root ConfigMap. This
prevents the still-running master from replacing workers while their Pods are being removed.

Delete one Session by its name. Do not use `kubectl delete -k deploy/kubernetes/session` in a
namespace that hosts multiple Sessions: the kustomization contains the shared namespace resource.

```bash
SESSION_NAME=seatunnel-session-cluster  # or the name passed to --session-name

kubectl -n seatunnel delete statefulset "${SESSION_NAME}-master"
kubectl -n seatunnel delete configmap \
  "${SESSION_NAME}-meta" "${SESSION_NAME}-master-config" "${SESSION_NAME}-worker"
kubectl -n seatunnel delete service \
  "${SESSION_NAME}-headless" "${SESSION_NAME}-rest" "${SESSION_NAME}-client"
kubectl -n seatunnel delete pdb "${SESSION_NAME}-master"
```

Checkpoint storage and user-owned PVCs are not deleted by the session manifests.

## Notes

- The image must contain `seatunnel-session-cluster.sh` and the Kubernetes resource-manager provider.
- Worker templates are immutable for the lifetime of a session. Create a new session to change
  slots, CPU, memory or the worker image.
- Only fixed slots are supported in this experimental release. The worker `seatunnel.yaml` must use
  `slot-service.dynamic-slot: false` and a `slot-num` equal to `session.worker.slots`.
- The sample uses `file:///tmp/` for checkpoints and IMap persistence so it can run on a local
  single-node cluster. Replace it with HDFS, S3 or a PVC for production.
