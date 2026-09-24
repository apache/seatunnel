---
sidebar_position: 6
title: FAQ
---

# Kubernetes FAQ

## How do I inspect or cancel an application?

```bash
bin/seatunnel-application.sh status --target kubernetes \
  --id ACTUAL_APPLICATION_ID --deployment-config kubernetes.conf

bin/seatunnel-application.sh cancel --target kubernetes \
  --id ACTUAL_APPLICATION_ID --deployment-config kubernetes.conf
```

Cancellation deletes the Job and labeled Pods, Service, and Secret. It does not create a savepoint. Status returns `UNKNOWN` after deletion or Job TTL expiry.

## How do I inspect resources and logs?

```bash
kubectl -n seatunnel-apps get jobs,pods,services,secrets \
  -l seatunnel.apache.org/application-id=ACTUAL_APPLICATION_ID

kubectl -n seatunnel-apps logs job/ACTUAL_APPLICATION_ID
kubectl -n seatunnel-apps describe job ACTUAL_APPLICATION_ID
```

Inspect every worker Pod when diagnosing worker failures. Image pull, ResourceQuota, LimitRange, admission, and node-capacity problems normally appear in Pod events.

## How long are completed resources retained?

The master exit code produces a Job `Complete` or `Failed` condition. The completed master Pod, Secret, and Service remain until `kubernetes.finished-job-retention-seconds` expires.

If the master is forcibly terminated before cleanup, workers exit after losing the master, but stopped Pod objects may remain until Job TTL cleanup. Retry `cancel` after an API outage is resolved.

## Common failures

| Symptom | Check |
| --- | --- |
| Master Pod never runs | Image pull, admission policy, ResourceQuota, node capacity, and submitter kubeconfig permissions |
| Workers do not register | Service, NetworkPolicy, master port, ServiceAccount permissions, and worker events |
| Connector not found | Matching plugins and drivers under the image `connectors/` directory |
| Job failure has no obvious cause | Master logs, every worker log, and `kubectl describe job/pod` |
| Checkpoint recovery fails | Historical Zeta job ID, PVC or remote storage access, retention policy, and configuration consistency |

## Security recommendations

- Use a dedicated namespace and least-privilege ServiceAccount.
- Mount the API token only in the master; workers do not require it.
- Manage private image credentials through the ServiceAccount or cluster.
- Restrict application Secret access and enable Kubernetes encryption at rest for Secrets when required by the cluster's security policy.
- Keep connector credentials out of logs and prefer credential mechanisms supported by the connector, filesystem, or cluster.
- Put `kubernetes.kubeconfig` in a permission-restricted deployment configuration file, or use the Kubernetes SDK default. Do not pass it with `-D` because command-line arguments may be visible to other users on the submission host.
- Allow only required Hazelcast and external-system traffic in NetworkPolicy.
