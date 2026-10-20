---
sidebar_position: 15
---

# Diagnose Runtime Logs with AI Tools

AI tools can help summarize SeaTunnel logs and identify evidence worth investigating. Their
output is not a confirmed root cause. Verify every suggested configuration option and remediation
against the SeaTunnel documentation, the connector documentation, and your deployment.

This guide focuses on SeaTunnel Engine (Zeta). When a job runs on Flink or Spark, collect the
driver and worker logs from that engine as well as the SeaTunnel starter logs. The Zeta REST log
endpoints described below do not collect Flink or Spark runtime logs.

:::caution Protect Production Data
Logs and job configurations can contain credentials, connection URLs, SQL statements, record
values, internal hostnames, and other sensitive data. Use only an AI service approved by your
organization. Redact the input before uploading it, and never upload private keys, access tokens,
heap dumps, or complete production configuration files.
:::

## Diagnostic Workflow

Use the following workflow instead of sending an entire log file directly to an AI tool:

1. Record the runtime context.
2. Collect logs for the affected job from every relevant node.
3. Keep the first failure and its complete exception chain.
4. Remove unrelated messages and redact sensitive values.
5. Ask for evidence, hypotheses, and verification steps separately.
6. Verify the result with SeaTunnel documentation, metrics, and the external system.

### Record the Runtime Context

Include the following information when it is available:

- SeaTunnel version
- execution engine and deployment mode
- batch or streaming mode
- job ID
- failure timestamp and time zone
- source, transform, and sink connector names
- changes made shortly before the failure
- whether the failure is repeatable or occurred during recovery

Do not include passwords, tokens, or unredacted connection details.

### Collect the Relevant Logs

SeaTunnel writes process logs to `$SEATUNNEL_HOME/logs` by default. Cluster scripts use separate
file names for master, worker, and combined server processes. See [Logging](logging.md) for Log4j2
configuration and per-job log routing.

When logs are mixed, use the `ST-JID` value to select one job. For example:

```shell
JOB_ID=<job-id>
grep -F "[${JOB_ID}]" "$SEATUNNEL_HOME/logs/seatunnel-engine-server.log" > job.log
```

If per-job routing is enabled, inspect `job-<job-id>.log`. In a multi-node Zeta cluster, collect
logs from the master and the workers that executed the job. The active master also provides these
REST endpoints:

```text
GET http://<master-host>:8080/logs/<job-id>
GET http://<master-host>:8080/logs?format=json
GET http://<node-host>:5801/log
```

The first endpoint retrieves matching logs across Zeta nodes. The last endpoint reads logs from
one node. A configured context path or dynamic HTTP port changes these URLs. See
[RESTful API V2](rest-api-v2.md#get-logs-from-all-nodes) for the complete behavior.

For Kubernetes, preserve logs from all relevant master and worker pods. Start with the failure
window and include previous container logs when a pod restarted:

```shell
kubectl logs <pod-name> --since=30m
kubectl logs <pod-name> --previous
kubectl describe pod <pod-name>
```

`kubectl describe pod` is important when the process was terminated by Kubernetes rather than by
a Java exception.

### Keep the Causal Context

Do not filter down to `ERROR` lines only. Keep:

- warnings before the first failure
- the first exception, not only later retries
- every nested `Caused by` section
- timestamps, logger names, thread names, and `ST-JID`
- worker or connector messages from the same time window

The following command can create an initial excerpt. Review and expand the context when the cause
starts outside the selected range.

```shell
grep -n -B 30 -A 80 -E \
  'ERROR|WARN|Caused by|Exception|OutOfMemoryError|timeout checkpoint' job.log \
  > diagnostic-excerpt.log
```

Repeated retry messages are usually a consequence, not the first cause. Search backward from the
first retry to find the original exception.

## Redact Before Sharing

Replace sensitive values with stable placeholders so relationships remain visible.

| Sensitive value | Example replacement |
|-----------------|---------------------|
| Password, token, secret, private key | `<redacted-secret>` |
| Database or broker hostname | `<source-host>` |
| Username or account ID | `<service-user>` |
| Internal path or bucket name | `<data-path>` |
| SQL literal or record value | `<record-value>` |

Keep option names, exception classes, timestamps, ports, and the structure of connection URLs when
they are relevant. For example, replace
`jdbc:mysql://orders.internal:3306/sales?useSSL=true` with
`jdbc:mysql://<source-host>:3306/<database>?useSSL=true`.

After automated replacement, read the excerpt manually. A simple regular expression cannot find
every credential or business value.

## Prompt Template

The template below works with a general AI tool, the [SeaTunnel Skill](../../tools/seatunnel-skill.md),
or another approved assistant.

```text
I am diagnosing an Apache SeaTunnel job failure.

Runtime context:
- SeaTunnel version: <version>
- Engine and deployment mode: <engine-and-mode>
- Job mode: <batch-or-streaming>
- Connectors: <source-transform-sink>
- Failure time and time zone: <timestamp>
- Recent changes: <changes-or-none>

Analyze only the evidence below.

1. State the observed failure and identify the earliest actionable exception.
2. Quote the exact log lines that support each conclusion.
3. Separate confirmed facts from hypotheses.
4. Rank hypotheses and explain what evidence is missing.
5. Give verification steps before suggesting a remediation.
6. Do not invent SeaTunnel configuration options. Mark any option that must be checked
   against the documentation.

Sanitized log excerpt:
<paste-excerpt-here>
```

For a follow-up question, add the result of the verification step instead of repeatedly sending
the full log.

## Common Failure Patterns

The patterns below are starting points. Similar messages can have different causes.

### Connector or Factory Discovery

Typical evidence includes `FactoryException`, `Unable to create a source`, or
`Could not find any factory for identifier`.

Verify:

- the connector identifier in the job configuration
- whether the connector plugin is installed on every required node
- whether all nodes use the same SeaTunnel and connector versions
- the list of available factory identifiers printed in the nested exception

### Connection, Authentication, or TLS Failure

The outer SeaTunnel exception can wrap a database, broker, HTTP, or cloud SDK exception. Preserve
the complete `Caused by` chain and verify connectivity from the node that ran the task. Check DNS,
ports, TLS trust, permissions, and rate limits independently of the AI result.

### Checkpoint Expiration

`CHECKPOINT_EXPIRED` means that all required acknowledgements did not arrive before the configured
checkpoint timeout. Increasing the timeout can hide the symptom without fixing the cause.

Check:

- [busyness and backpressure](busyness-and-backpressure.md)
- sink latency and external-system health
- worker loss or long garbage-collection pauses
- checkpoint history and the tasks that did not acknowledge
- checkpoint timeout configuration only after checking the above evidence

### Out of Memory

Distinguish these cases before changing memory settings:

- `java.lang.OutOfMemoryError: Java heap space`
- direct or native memory exhaustion
- a Kubernetes container terminated with `OOMKilled`
- host-level memory pressure

Collect the JVM message, pod termination reason, memory limits, recent garbage-collection evidence,
and workload volume. Do not upload heap dumps to an external AI service.

### Task Retry or Worker Failure

Repeated task deployment, notification, or recovery messages describe the retry path. Locate the
first exception before the retries and correlate the same time window across the master and worker
logs. Confirm worker health and cluster membership before changing retry settings.

## Reproducible Walkthrough

The following example uses exception messages from SeaTunnel's current factory discovery path.
Start with a working local job and temporarily replace a source connector identifier with
`JdbcTypo`. The job fails before the connector is created.

A shortened, sanitized exception chain looks like this:

```text
org.apache.seatunnel.api.table.factory.FactoryException:
Unable to create a source for identifier 'JdbcTypo'.
Caused by: org.apache.seatunnel.api.table.factory.FactoryException:
Could not find any factory for identifier 'JdbcTypo' that implements
'org.apache.seatunnel.api.table.factory.TableSourceFactory' in the classpath.

Available factory identifiers are:

...
Jdbc
...
```

The outer exception says which phase failed. The nested exception provides the actionable evidence:
`JdbcTypo` is not available, while `Jdbc` is available. This supports an identifier mismatch. It
does not prove that the JDBC connector will work after the spelling is corrected.

Verify the diagnosis before changing the job:

1. Check the source block identifier in the submitted configuration.
2. Confirm that the expected connector is installed on every node.
3. Compare the identifier with the connector documentation and the available identifier list.
4. Correct the identifier and rerun the job.
5. Treat any new exception as a separate failure with its own evidence.

This distinction prevents a plausible first diagnosis from being presented as proof that the entire
job configuration is valid.

## When to Ask the Community

If the evidence is still inconclusive, search existing
[GitHub issues](https://github.com/apache/seatunnel/issues) and the
[developer mailing list](https://lists.apache.org/list.html?dev@seatunnel.apache.org). When opening
an issue, include the sanitized runtime context, the earliest exception, relevant surrounding
lines, and the verification steps already performed. Do not post the original unredacted logs.
