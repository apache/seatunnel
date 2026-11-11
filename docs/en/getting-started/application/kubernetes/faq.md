---
sidebar_position: 7
title: FAQ
---

# Kubernetes FAQ

High-frequency questions about operations, lifecycle, failures and security for Application Mode.
Commands live in [Commands](commands.md), full option tables in [Configuration](configuration.md).

## Operations

### How do I inspect or cancel an application?

```bash
bin/seatunnel-application.sh status --target kubernetes \
  --id ACTUAL_APPLICATION_ID --application-config kubernetes.conf

bin/seatunnel-application.sh cancel --target kubernetes \
  --id ACTUAL_APPLICATION_ID --application-config kubernetes.conf
```

Cancellation deletes the Job and labeled Pods, Service, and any Secret or ConfigMap created for the
application. It does not create a savepoint, and referenced Secrets or ConfigMaps are never modified.
Status returns `UNKNOWN` after deletion or Job TTL expiry.

### How do I inspect resources and logs?

```bash
kubectl -n seatunnel-apps get jobs,pods,services,secrets \
  -l seatunnel.apache.org/application-id=ACTUAL_APPLICATION_ID

kubectl -n seatunnel-apps logs job/ACTUAL_APPLICATION_ID
kubectl -n seatunnel-apps describe job ACTUAL_APPLICATION_ID
```

Inspect every worker Pod when diagnosing worker failures. Image pull, ResourceQuota, LimitRange, admission, and node-capacity problems normally appear in Pod events.

## Lifecycle

### How long are completed resources retained?

The master exit code produces a Job `Complete` or `Failed` condition. The completed master Pod,
Service, and any created Secret or ConfigMap remain until
`kubernetes.finished-job-retention-seconds` expires.

If the master is forcibly terminated before cleanup, workers exit after losing the master, but stopped Pod objects may remain until Job TTL cleanup. Retry `cancel` after an API outage is resolved.

### What happens when the master Pod never starts?

The Job stays `Active` and `status --wait` keeps polling; `--wait-timeout` bounds that wait.
For unattended batch runs, set `kubernetes.active-deadline-seconds` so Kubernetes fails the Job
automatically and `--wait` returns `FAILED`. Fix the root cause from the Pod events: image pull,
admission policy, ResourceQuota, or node capacity.

## Common failures

| Symptom | Check |
| --- | --- |
| Master Pod never runs | Image pull, admission policy, ResourceQuota, node capacity, and submitter kubeconfig permissions |
| Workers do not register | Service, NetworkPolicy, master port, ServiceAccount permissions, and worker events |
| A file is missing in a Pod | The referenced ConfigMap/Secret name and key, and the full `mount` path, must match; referenced keys are validated before submission. |
| Connector not found | Matching plugins and drivers under the image `connectors/` directory |
| Job failure has no obvious cause | Master logs, every worker log, and `kubectl describe job/pod` |
| Checkpoint recovery fails | Historical Zeta job ID, PVC or remote storage access, retention policy, and configuration consistency |

## Security recommendations

- Use a dedicated namespace and least-privilege ServiceAccount.
- Mount the API token only in the master; workers do not require it.
- Manage private image credentials through the ServiceAccount or cluster.
- Restrict application Secret access and enable Kubernetes encryption at rest for Secrets when required by the cluster's security policy.
- Keep connector credentials out of logs and prefer credential mechanisms supported by the connector, filesystem, or cluster.
- Put `kubernetes.kubeconfig` in a permission-restricted deployment configuration file, or use the Kubernetes SDK default. Do not pass it with `-i` because command-line arguments may be visible to other users on the submission host.
- Allow only required Hazelcast and external-system traffic in NetworkPolicy.
