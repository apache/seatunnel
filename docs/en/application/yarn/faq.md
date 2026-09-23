---
sidebar_position: 6
title: FAQ
---

# YARN FAQ

## Status and cancellation

Use the YARN application ID returned by submission:

```bash
bin/seatunnel-application.sh status --target yarn \
  --id application_0000000000000_0001 \
  --deployment-config yarn-deployment.conf

bin/seatunnel-application.sh cancel --target yarn \
  --id application_0000000000000_0001 \
  --deployment-config yarn-deployment.conf
```

Closing the client does not cancel the application. `cancel` terminates the YARN application and removes this submission's staging files. It does not create a savepoint.

## Logs

With YARN log aggregation enabled:

```bash
yarn logs -applicationId application_0000000000000_0001
```

Otherwise, inspect AM and worker container logs on the corresponding NodeManagers. Start with AM `stderr`, then inspect every worker.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| AM startup timeout | Queue resources, maximum container size, distribution path, Hadoop configuration, and NodeManager Java |
| Worker registration timeout | Queue capacity, NodeManager networking, actual master port, and firewalls |
| Connector not found | Matching plugins and drivers under the distribution `connectors/` directory |
| Staging remains after failure | Run `status` with the original configuration to retry cleanup, then inspect the private staging path by application ID |
| Checkpoint cannot be restored | Historical Zeta job ID, retention policy, storage permissions, and configuration consistency |

## Cleanup boundary

On normal termination, the ApplicationMaster releases containers, unregisters the final result, and deletes staging. A status client also retries staging cleanup after observing a terminal state.

If the AM is killed before its shutdown hook and no client later retrieves status, HDFS staging may remain. Hadoop independently manages NodeManager local caches and log retention. Checkpoints use a separate retention policy and are not removed with staging.

## Security recommendations

- Keep staging private to the submitting user.
- Do not log database passwords, object-store keys, or tokens.
- Use credential providers or protected runtime configuration supported by the underlying Hadoop filesystem.
- Submission is rejected when Kerberos configuration is detected in this release.
