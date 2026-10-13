---
sidebar_position: 4
title: Commands
---

# Application commands

`bin/seatunnel-application.sh` exposes exactly three operations: `submit`, `status` and `cancel`.
All of them target one external resource platform (`--target kubernetes`) and read the same
deployment file, so `status` and `cancel` connect to the same namespace and kubeconfig as the
original submission.

## Synopsis

```bash
bin/seatunnel-application.sh submit --target kubernetes -c job.conf -a kubernetes.conf
bin/seatunnel-application.sh status --target kubernetes --id APPLICATION_ID -a kubernetes.conf [--wait]
bin/seatunnel-application.sh cancel --target kubernetes --id APPLICATION_ID -a kubernetes.conf
```

## Arguments

| Argument | Commands | Meaning |
| --- | --- | --- |
| `submit` / `status` / `cancel` | all | Operation, the first positional argument |
| `-t`, `--target` | all | Resource platform; currently `kubernetes` |
| `-c`, `--config` | submit | Job configuration file; optional when the platform carries the job, for example through `kubernetes.job-config-file` |
| `-a`, `--application-config` | all | Optional HOCON deployment file, for example `kubernetes.conf` |
| `-i key=value` | all | Override one non-sensitive deployment option, for example `-i application.worker-count=4`; repeatable |
| `--id` | status, cancel | Application ID printed by `submit` |
| `--wait` | submit, status | Poll until the application reaches a terminal status |
| `--job-id` | submit | Native Zeta job ID to use for this application instead of a generated one |
| `--restore-job-id` | submit | Job ID whose latest durable checkpoint restores the new application |

## Capability per command

| Operation | Platform work | Printed result | Waits |
| --- | --- | --- | --- |
| `submit` | Create the owner Job, its owned Secret/ConfigMap/Service and start the Job | `Application ID` (the Kubernetes Job name) and `Job ID` | No, unless `--wait` is added |
| `status` | Read the Job, Pod and runtime state and map it to an `ApplicationStatus` | `Application ID` and `Status` | No, unless `--wait` is added |
| `cancel` | Delete the owner Job; owned resources are removed with it, referenced objects are not | `Application ID` and `Cancellation requested` | No |

## submit

`submit` localizes the job, creates every resource in the deployment chain and starts the Job, then
prints both identifiers:

```text
Application ID: example-9f2c41a0b7d3
Job ID: 917381145928572929
```

The application ID is generated from `application.name` and becomes the workload name, so it is
also the Kubernetes Job name and the value of the ownership label on every owned resource. It is
DNS-compatible and unique per submission.

The command returns as soon as the platform accepted the started Job. Image pull, admission and
scheduling therefore never fail the submission; adding `--wait` first evaluates the application and
only then prints the terminal status. When resource creation itself fails, the resources created by
that attempt are rolled back before the error is reported.

## status

`status` reads the current state without touching the application:

```bash
bin/seatunnel-application.sh status --target kubernetes --id example-9f2c41a0b7d3 -a kubernetes.conf
```

```text
Application ID: example-9f2c41a0b7d3
Status: RUNNING
```

The native statuses are `CREATED`, `DEPLOYING`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELED` and
`UNKNOWN`. With `--wait` the command polls every 500 milliseconds until a terminal status
(`SUCCEEDED`, `FAILED` or `CANCELED`) is reported. A reportable failure makes the command exit
non-zero, which turns it into a completion gate for schedulers and CI:

```bash
if ! bin/seatunnel-application.sh status --target kubernetes --id "$APP_ID" -a kubernetes.conf --wait; then
  echo "application $APP_ID did not succeed"
fi
```

## cancel

`cancel` deletes the owner Job of the application:

```bash
bin/seatunnel-application.sh cancel --target kubernetes --id example-9f2c41a0b7d3 -a kubernetes.conf
```

Owned Pods, Services, Secrets and ConfigMaps are garbage-collected with the Job owner reference.
Referenced objects created outside SeaTunnel are never modified or deleted. After cancellation the
application reports `CANCELED`; a finished Job is retained until
`kubernetes.finished-job-retention-seconds` expires.

## Inspecting in Kubernetes

Every platform resource carries the application ID as its ownership label, so the application can be
examined directly:

```bash
kubectl -n seatunnel-apps get jobs,pods,services -l seatunnel.apache.org/application-id=example-9f2c41a0b7d3
kubectl -n seatunnel-apps describe job example-9f2c41a0b7d3
kubectl -n seatunnel-apps logs job/example-9f2c41a0b7d3
```

:::tip
Use `status --wait` for programmatic completion and `kubectl` for diagnosis; the command reference
does not replace Pod events and logs when a start-up problem has to be explained.
:::

See [Configuration](configuration.md) for the deployment options these commands accept.
