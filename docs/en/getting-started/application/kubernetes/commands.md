---
sidebar_position: 4
title: Commands
---

# Kubernetes application commands

`bin/seatunnel-application.sh` submits, inspects and cancels applications on Kubernetes. The shared
[command line](../cli.md) reference defines the operations, arguments, wait semantics and exit
codes; this page describes what they do against a cluster.

## Synopsis

```bash
bin/seatunnel-application.sh submit --target kubernetes -a kubernetes.conf [-c job.conf]
bin/seatunnel-application.sh status --target kubernetes --id APPLICATION_ID -a kubernetes.conf [--wait]
bin/seatunnel-application.sh cancel --target kubernetes --id APPLICATION_ID -a kubernetes.conf
```

## submit

`submit` creates the owner `batch/v1` Job, the Secret/ConfigMap/Service the application owns, and
starts the Job:

```text
Application ID: example-9f2c41a0b7d3
Job ID: 917381145928572929
```

The application ID is generated from `application.name`, so it is also the Kubernetes Job name and
the value of the ownership label on every owned resource. `-c` is optional when the deployment file
selects a mounted job file through `kubernetes.job-config-file`.

The command returns as soon as the API accepted the started Job, so image pull, admission and
scheduling never fail the submission. When resource creation itself fails, the resources created by
that attempt are rolled back before the error is reported.

## status

`status` maps the Job and Pod state to the platform-neutral status:

```text
Application ID: example-9f2c41a0b7d3
Status: RUNNING
```

`--wait` keeps polling until a terminal status is reported; `FAILED`, `CANCELED` and `UNKNOWN` make
the command exit non-zero, which turns it into a completion gate for schedulers and CI.
`--wait-timeout` bounds that polling and fails with the last observed status when the limit is
reached, while the default `0` keeps waiting indefinitely; `--wait-interval` sets the poll interval
(500 ms by default).

## cancel

`cancel` deletes the owner Job of the application. Owned Pods, Services, Secrets and ConfigMaps are
garbage-collected through the owner reference; referenced objects created outside SeaTunnel are
never modified or deleted. After cancellation the application reports `CANCELED`, and a finished
Job is retained until `kubernetes.finished-job-retention-seconds` expires. A canceled master exits
with code 1, because a Kubernetes Job cannot express `CANCELED`, so the Job object ends in the
`Failed` state while `status` reports `CANCELED`.

## Inspecting in Kubernetes

Every platform resource carries the application ID as its ownership label, so the application can be
examined directly:

```bash
kubectl -n seatunnel-apps get jobs,pods,services -l seatunnel.apache.org/application-id=example-9f2c41a0b7d3
kubectl -n seatunnel-apps describe job example-9f2c41a0b7d3
kubectl -n seatunnel-apps logs job/example-9f2c41a0b7d3
```

:::tip
Use `status --wait` for programmatic completion and `kubectl` for diagnosis; the command itself does
not replace Pod events and logs when a start-up problem has to be explained.
:::

See [Configuration](configuration.md) for the deployment options these commands accept.
