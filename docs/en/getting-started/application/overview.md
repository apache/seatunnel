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

## When to use Application Mode

- You already operate a resource platform and do not want to maintain a long-running shared
  SeaTunnel cluster.
- Every job should own an isolated cluster with a fixed resource footprint.
- The job lifecycle should be visible and manageable through platform objects.

## When not to use Application Mode

- Many small jobs should share one long-running cluster (use standalone or cluster deployment).
- The job needs dynamic worker scaling or automatic worker replenishment while it runs; Application
  Mode uses a fixed worker set and fails instead of scaling out.
- The distribution has no provider for your resource platform.

## Cluster shape

- One application runs one Zeta master and a fixed number of workers; no member is added or removed
  while the job runs.
- Workers register their slots with the master before the job starts.
- The master maps the job result to the platform state and releases the workers before it exits.
- Checkpoints live in engine-owned persistent storage and survive the application; a new application
  can restore them by native job ID.

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

## Next steps

The [command line](cli.md) reference documents submitting, inspecting, waiting for and cancelling an
application; the platform guide covers the environment its provider needs.
