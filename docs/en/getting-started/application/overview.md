---
sidebar_position: 1
title: Overview
---

# About Application Mode

Application Mode deploys one self-contained SeaTunnel application per submission: the resource
platform starts a dedicated Zeta master and a fixed number of workers, the master runs exactly one
job, and the platform reclaims every process when the application finishes.

Zeta itself owns worker registration, slot scheduling, task execution and checkpoints; the resource
platform only provides and reclaims compute. That boundary is what lets the same job run on
different platforms without changing the job file.

## What problem it solves

### One-off batch jobs

A job runs once, its result is written, and every resource it held is released immediately: no
long-running cluster to keep warm and no leftover processes to clean up by hand. The application
state maps directly to platform objects, so `status` and `cancel` are enough to observe and stop it.

### Large-scale batch teams

Teams that run many batch jobs at the same time get per-job isolation: every job owns its master and
its fixed set of workers, with its own CPU, memory and slot footprint, namespace and labels. One job
cannot borrow slots from another or disturb its state, and quotas and policies apply per
application. Checkpoints survive the application, so a later application can restore an earlier job
by its native job ID.

:::note
Application Mode is not a replacement for standalone or shared-cluster deployment: many small jobs
that should share one long-running cluster still belong to cluster mode, and a job that needs
dynamic worker scaling should not use the fixed worker set of Application Mode.
:::

## Cluster shape

- One application runs one Zeta master and a fixed number of workers; no member is added or removed
  while the job runs.
- Workers register their slots with the master before the single job starts.
- The master maps the job result to the platform state and releases the workers before it exits.
- Checkpoints live in engine-owned persistent storage and survive the application; a new application
  can restore them by native job ID.

## Command line

```bash
bin/seatunnel-application.sh submit -t <target> -c job.conf -a application.config
bin/seatunnel-application.sh status -t <target> --id APPLICATION_ID -a application.config --wait
bin/seatunnel-application.sh cancel -t <target> --id APPLICATION_ID -a application.config
```

| Option | Meaning |
| --- | --- |
| `-t`, `--target` | Resource platform that runs the application. |
| `-c`, `--config` | SeaTunnel job configuration file, required for submit. |
| `-a`, `--application-config` | Optional deployment file with common and platform options. |
| `--wait` | Submit/status wait for a terminal result; without it the command returns immediately. |
| `-ikey=value` | Non-sensitive deployment override, for example worker resources or platform options. |

`submit` returns as soon as the platform accepted the application; it does not wait for the master,
so slow image pulls or provider scheduling never fail the command. `status` and `cancel` only talk
to the platform API, and can be used after the master has exited.

## Requirements

- A supported resource platform provider must be present in the distribution.
- The platform must be able to start and observe the dedicated processes and clean them up when the
  application ends.
- Credentials stay outside the job file: inject them through platform-managed secrets and resolve
  them with `${VAR}` placeholders inside the application.

Platform-specific preparation, permissions and configuration are described in the platform guide.

## Glossary

| Term | Definition |
| --- | --- |
| Application | One job plus its dedicated master and fixed workers, identified by the platform-native ID. |
| Master | The single Zeta member that runs the job, coordinates workers and publishes the result. |
| Worker | A fixed-size Zeta member that registers slots and executes tasks. |
| Resource manager | The Engine component that owns worker lifecycle through a platform driver. |
| Resource manager driver | The platform provider implementation that starts, observes and releases worker processes. |
| Application specification | The immutable, platform-independent application fields carried to the master. |
| Localized specification | The platform mechanism that delivers the specification to the master, such as mounted files or injected environment variables. |
| Checkpoint | Engine-owned durable state in persistent storage; independent of the application lifecycle. |

## Recommended reading order

| Phase | Document | Description |
| --- | --- | --- |
| Understand | This page | What Application Mode is, and when to use it. |
| Try it out | Platform guide | Environment preparation, first submission and status inspection. |
| Configure | Platform guide | Platform options, credentials and runtime configuration. |
| Operate | Platform guide | Cancellation, failure diagnosis and retention behavior. |
