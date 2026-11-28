---
sidebar_position: 7
title: FAQ
---

# YARN FAQ

## Operations

### How do I inspect or cancel an application?

Use the YARN application ID returned by submission:

```bash
bin/seatunnel-application.sh status --target yarn \
  --id application_0000000000000_0001 \
  --application-config yarn-deployment.conf

bin/seatunnel-application.sh cancel --target yarn \
  --id application_0000000000000_0001 \
  --application-config yarn-deployment.conf
```

Closing the client does not cancel the application. `cancel` terminates the YARN application and removes this submission's staging files. It does not create a savepoint.

### Can I open the SeaTunnel web UI from the ResourceManager?

Set `yarn.master.http-port` at submission and use the AM host and port while the application is running. The ResourceManager also publishes a proxied tracking link. If the HTML loads through that link but the page stays blank, check where the browser requests `/assets/` and `/overview`. Use the direct AM address or a dedicated gateway origin for the full UI. When the port is omitted the AM runs headless. See [Master HTTP endpoint](configuration.md#master-http-endpoint).

### Where are the container logs?

With YARN log aggregation enabled:

```bash
yarn logs -applicationId application_0000000000000_0001
```

Otherwise, inspect AM and worker container logs on the corresponding NodeManagers. Start with AM `stderr`, then inspect every worker.

## Common failures

| Symptom | Check |
| --- | --- |
| AM startup timeout | Queue resources, maximum container size, distribution path, Hadoop configuration, and NodeManager Java |
| Worker registration timeout | Queue capacity, NodeManager networking, actual master port, and firewalls |
| Connector not found | Matching plugins and drivers under the distribution `connectors/` directory |
| Staging remains after failure | Run `status` with the original configuration to retry cleanup, then inspect the private staging path by application ID |
| Job file missing | Use `-c` or `yarn.job-config-file`. Relative source paths are resolved from the submitter's working directory. |
| Container configuration file missing | Check the source and the destination in `yarn.local-resources`. Point the runtime option to the same `files/…` path. |
| Secure HDFS access denied | Check the submission identity, HDFS permissions, token lifetime and `yarn.resourcemanager.principal`. |
| Checkpoint cannot be restored | Historical Zeta job ID, retention policy, storage permissions, and configuration consistency |

## Lifecycle

### What happens to files when the application finishes?

On normal termination, the ApplicationMaster releases containers, unregisters the final result, and deletes staging. If a terminal status query encounters a staging cleanup error, it logs a warning and returns the ResourceManager's result. Run `status` again with the original staging path and an authorized user to retry cleanup.

If the AM is killed before its shutdown hook and no client later retrieves status, HDFS staging may remain. Hadoop independently manages NodeManager local caches and log retention. Checkpoints use a separate retention policy and are not removed with staging.

## Security recommendations

- Keep staging private to the submitting user.
- Do not log database passwords, object-store keys, or tokens.
- Use credential providers or protected runtime configuration supported by the underlying Hadoop filesystem.
- For a Kerberos cluster, use a valid ticket cache or a submitter keytab and check the ResourceManager principal. See [Kerberos configuration](configuration.md#kerberos).
