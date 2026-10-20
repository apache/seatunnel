---
sidebar_position: 2
title: Command Line
---

# Application command line

`bin/seatunnel-application.sh` is the only entry point of Application Mode. It exposes three
operations, all of which target one external resource platform and read the same deployment file,
so `status` and `cancel` see exactly the application `submit` created.

## Synopsis

```bash
bin/seatunnel-application.sh submit -t <target> -a application.config [-c job.conf]
bin/seatunnel-application.sh status -t <target> --id APPLICATION_ID -a application.config [--wait] \
  [--wait-timeout <ms>] [--wait-interval <ms>]
bin/seatunnel-application.sh cancel -t <target> --id APPLICATION_ID -a application.config
```

The built-in help lists every option:

```bash
bin/seatunnel-application.sh -h
```

```text
Usage: seatunnel-application.sh [options] submit | status | cancel
  Options:
    -a, --application-config Optional HOCON deployment file, e.g.
                             application.config (not the job file)
    -c, --config             SeaTunnel job configuration file (required for
                             submit unless the provider supplies it, for
                             example kubernetes.job-config-file)
    -h, --help               Show the usage message
    --id                     Platform application ID (required for
                             status/cancel)
    --job-id                 Optional native Zeta job ID for submit
    --restore-job-id         Historical Zeta job ID whose latest checkpoint
                             restores this new application
  * -t, --target             External resource platform: kubernetes
    --wait                   Wait for completion (submit/status only; default:
                             return immediately) (default: false)
    --wait-timeout           Maximum time in milliseconds that --wait polls for
                             a terminal status before failing with the last
                             observed status; 0 waits indefinitely (default: 0)
    --wait-interval          Status poll interval in milliseconds while --wait
                             blocks (default: 500)
    -D                       Override a non-sensitive deployment option:
                             -Dkey=value (syntax: -Dkey=value) (default: {})
```

## Arguments

| Argument | Operations | Meaning |
| --- | --- | --- |
| `submit` / `status` / `cancel` | all | Operation, the first positional argument |
| `-t`, `--target` | all | Resource platform that runs the application |
| `-c`, `--config` | submit | SeaTunnel job configuration file; optional when the platform carries the job, such as a mounted `job.conf` |
| `-a`, `--application-config` | all | Optional deployment file with common and platform options |
| `-Dkey=value` | all | Override one non-sensitive `application.*` or `kubernetes.*` deployment option, for example `-Dapplication.worker-count=4`; repeatable. Job `${...}` variables are resolved inside the Pod, not on the submitting host |
| `--id` | status, cancel | Platform application ID printed by `submit` |
| `--wait` | submit, status | Poll until a terminal status |
| `--wait-timeout <ms>` | submit, status | Maximum time in milliseconds that `--wait` polls before failing with the last observed status; `0` (the default) waits indefinitely. Requires `--wait` |
| `--wait-interval <ms>` | submit, status | Status poll interval in milliseconds while `--wait` blocks; default `500`. Requires `--wait` |
| `--job-id` | submit | Native Zeta job ID to use for this application instead of a generated one |
| `--restore-job-id` | submit | Job ID whose latest durable checkpoint restores the new application |

## Operations

| Operation | What it does | Result |
| --- | --- | --- |
| `submit` | Localizes the job, creates the application on the platform and starts it | Prints the application ID and the native job ID |
| `status` | Reads the platform state and maps it to an `ApplicationStatus` | Prints the application ID and status; exits non-zero when the application failed |
| `cancel` | Removes the application and the resources it owns | Prints the application ID and the cancellation result |

## Submit returns immediately

`submit` returns as soon as the platform accepted the started application. Image pulls, admission
and scheduling therefore never fail the command; add `--wait` to wait for the terminal status
inside the same command:

```bash
bin/seatunnel-application.sh submit -t <target> -a application.config --wait
```

The job comes from `-c` or from a file the platform mounts inside the application, for example
`kubernetes.job-config-file`. A submission that provides neither is rejected before the first
resource is created. Values written as `${VAR}` are resolved inside the running application, never
on the submitting host, so platform-managed credentials can be injected through the environment.

## Status and waiting

`status` reports one snapshot by default and polls that snapshot until it becomes terminal when
`--wait` is given. `--wait-timeout` fails the wait with the last observed status when the limit is
reached; the default `0` waits indefinitely, like an attached client. `--wait-interval` sets how
often the status is polled. The platform-independent states are:

| Status | Meaning |
| --- | --- |
| `CREATED` | The platform accepted the submission but has not started the control plane |
| `DEPLOYING` | The platform is provisioning the control plane |
| `RUNNING` | The application is running; workers or the job may still be starting |
| `SUCCEEDED` | The job completed successfully |
| `FAILED` | Startup, execution or reportable cleanup failed |
| `CANCELED` | The platform reported a cancellation |
| `UNKNOWN` | A known application has a platform state that cannot yet be mapped |

`SUCCEEDED`, `FAILED` and `CANCELED` are terminal; `--wait` stops there. `FAILED`, `CANCELED` and
`UNKNOWN` make `status` exit non-zero, which turns the command into a completion gate for
schedulers and CI:

```bash
if ! bin/seatunnel-application.sh status -t <target> --id "$APP_ID" -a application.config --wait; then
  echo "application $APP_ID did not succeed"
fi
```

## Cancel

`cancel` asks the platform to remove the application. Every resource the application owns is
deleted with it, while objects managed outside SeaTunnel are left untouched. How long a canceled or
finished application stays queryable depends on the platform retention policy.

Each platform guide documents the platform-specific behavior behind these operations.
